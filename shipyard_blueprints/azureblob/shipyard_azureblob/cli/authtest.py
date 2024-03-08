import os
import sys
from shipyard_azureblob import AzureBlobClient


def main():
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    azure = AzureBlobClient(connection_string=conn_str)
    try:
        conn = azure.connect()
        sys.exit(0)
    except Exception as e:
        azure.logger.error(
            "Could not connect to the Azure with the provided connection string"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
