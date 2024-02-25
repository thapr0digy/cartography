from cartography import cli
from cartography import sync
from cartography.intel import azure
from cartography.intel import create_indexes


import logging

logging.getLogger('neo4j').setLevel(logging.DEBUG)

def build_custom_sync():
    s = sync.Sync()
    s.add_stages([
        ('create-indexes', create_indexes.run),
        ('azure', azure.start_azure_ingestion),
    ])
    return s

def main(argv):
    return cli.CLI(build_custom_sync(), prog='cartography').main(argv)

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))