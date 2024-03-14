import os
import sys

from shipyard_templates import ShipyardLogger

from shipyard_azureblob import AzureBlobClient

logger = ShipyardLogger().get_logger()


def main():
    try:
        sys.exit(
            AzureBlobClient(
                connection_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
            ).connect()
        )
    except Exception as e:
        logger.authtest(
            f"Could not connect to the Azure with the provided connection string due to {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
