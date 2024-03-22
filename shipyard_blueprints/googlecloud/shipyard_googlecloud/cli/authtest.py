import os
import sys
from shipyard_googlecloud import GoogleCloudClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger().get_logger()


def main():
    client = GoogleCloudClient(
        service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    try:
        client.connect()
        sys.exit(0)
    except Exception as e:
        logger.error(
            f"Could not connect to Google Cloud with the service account provided. {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
