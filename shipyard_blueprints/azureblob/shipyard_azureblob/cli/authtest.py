import argparse
import os
from shipyard_blueprints import AzureBlobClient


def get_args():
    args = {}
    args['connection_string'] = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    return args


def main():
    args = get_args()
    conn_str = args['connection_string']
    azure = AzureBlobClient(connection_string=conn_str)
    try:
        conn = azure.connect()
        return 0
    except Exception as e:
        azure.logger.error(
            "Could not connect to the Azure with the provided connection string")
        return 1


if __name__ == "__main__":
    main()
