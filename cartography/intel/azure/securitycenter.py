import logging
import uuid
from collections import deque
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Tuple

import neo4j
from azure.core.exceptions import ClientAuthenticationError
from azure.core.exceptions import HttpResponseError
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.security import SecurityCenter

from .util.credentials import Credentials
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)

@timeit
def get_client(credentials: Credentials, subscription_id: str) -> SecurityCenter:
    """
    Getting the SecurityCenter client
    """
    client = SecurityCenter(credentials, subscription_id)
    return client

@timeit
def get_sub_assessments_list(credentials: Credentials, subscription_id: str) -> List[Dict]:
    """
    Get a list of all assessments.
    """
    try:
        # We need to change the subscription_id to the long form for the list_all endpoint
        # Ex. /subscriptions/{subscription_id}
        client = get_client(credentials, subscription_id)
        sub_assessment_list = list(map(lambda x: x.as_dict(), client.sub_assessments.list_all(f"/subscriptions/{subscription_id}")))
        

    # ClientAuthenticationError and ResourceNotFoundError are subclasses under HttpResponseError
    except ClientAuthenticationError:
        logger.warning('Client Authentication Error while retrieving assessments', exc_info=True)
        return []
    except ResourceNotFoundError:
        logger.warning('SubAssessments not found error', exc_info=True)
        return []
    except HttpResponseError:
        logger.warning('Error while retrieving list of assessments', exc_info=True)
        return []
    
    for sub_assessment in sub_assessment_list:
        x = sub_assessment['id'].split('/')

        # TODO: Find a better way to search for case insensitive resourceGroups
        if "resourceGroups" not in x:
            if "resourcegroups" in x:
                sub_assessment['resourceGroup'] = x[x.index('resourcegroups') + 1]
                continue
            else:
                # There are subassessments as part of the id containing identities
                # In this case, we'll add an empty resourceGroup
                sub_assessment['resourceGroup'] = ""
                continue
        sub_assessment['resourceGroup'] = x[x.index('resourceGroups') + 1]

    return sub_assessment_list

@timeit
def transform_sub_assessment_data(sub_assessment_list: List[Dict]) -> List[Dict]:
    """
    Transforming the sub assessment response for neo4j ingestion.
    """
    for sub_assessment in sub_assessment_list:
        # Move out the status dict, resource_details and check for empty additional_data
        if 'code' in sub_assessment['status']:
            sub_assessment['code'] = sub_assessment['status']['code']
        if 'severity' in sub_assessment['status']:
            sub_assessment['severity'] = sub_assessment['status']['severity']
        
    return sub_assessment_list


@timeit
def load_sub_assessment_data(
        neo4j_session: neo4j.Session, subscription_id: str, sub_assessment_list: List[Dict], azure_update_tag: int,
) -> None:
    """
    Ingest data of all sub assessments into neo4j.
    """
    ingest_sub_assessment = """
    UNWIND $sub_assessment_list AS sa
    MERGE (s:AzureSubAssessment{id: sa.id})
    ON CREATE SET s.firstseen = timestamp() / 1000,
    s.type = sa.type, s.resourcegroup = sa.resourceGroup
    SET s.lastupdated = $azure_update_tag,
    s.name = sa.name, s.id_properties_id = sa.id_properties_id,
    s.display_name = sa.display_name, s.description = sa.description,
    s.category = sa.category, s.impact = sa.impact, s.remediation = sa.remediation,
    s.time_generated = sa.time_generated, s.code = sa.code, s.severity = sa.severity
    WITH s
    MATCH (owner:AzureSubscription{id: $AZURE_SUBSCRIPTION_ID})
    MERGE (owner)-[r:RESOURCE]->(s)
    ON CREATE SET r.firstseen = timestamp()
    SET r.lastupdated = $azure_update_tag

    """

    neo4j_session.run(
        ingest_sub_assessment,
        sub_assessment_list=sub_assessment_list,
        AZURE_SUBSCRIPTION_ID=subscription_id,
        azure_update_tag=azure_update_tag,
    )

@timeit
def load_sub_assessment_data_resources(
    neo4j_session: neo4j.Session, subscription_id: str, sub_assessment_list: List[Dict], azure_update_tag: int,
) -> None:
    """
    Ingest all relationships to other resources
    """
    ingest_sub_assessment_resources = """
    UNWIND $
    """

    neo4j_session.run(
        ingest_sub_assessment_resources,
        sub_assessment_list=sub_assessment_list,
        AZURE_SUBSCRIPTION_ID=subscription_id,
        azure_update_tag=azure_update_tag,
    )

@timeit
def create_chunked_sub_assessment_list(sub_assessment_list: List[Dict], chunk_size: int) -> List[Dict]:
    deque_obj = deque(sub_assessment_list)
    while deque_obj:
        chunk = []
        for _ in range(chunk_size):
            if deque_obj:
                chunk.append(deque_obj.popleft())
        yield chunk

@timeit
def cleanup_azure_sub_assessments(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
    run_cleanup_job('azure_sub_assessment_cleanup.json', neo4j_session, common_job_parameters)

@timeit
def sync(
        neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str,
        sync_tag: int, common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Azure SubAssessment for subscription '%s'.", subscription_id)
    sub_assessment_list = get_sub_assessments_list(credentials, subscription_id)
    sub_assessment_list = transform_sub_assessment_data(sub_assessment_list)
    # Split into chunks for processing
    # Note: This is specifically done for AuraDB which only has a memory total of 250MB 
    chunk_list = list(create_chunked_sub_assessment_list(sub_assessment_list, 5000))
    for chunk in chunk_list:
        load_sub_assessment_data(neo4j_session, subscription_id, chunk, sync_tag)

    #load_sub_assessment_data_resources(neo4j_session, subscription_id, sub_assessment_list, sync_tag)
    #sync_database_account_details(
    #    neo4j_session, credentials, subscription_id, database_account_list, sync_tag,
    #    common_job_parameters,
    #)
    #cleanup_azure_sub_assessments(neo4j_session, common_job_parameters)