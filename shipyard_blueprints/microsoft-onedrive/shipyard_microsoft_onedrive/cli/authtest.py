import os
import sys
from shipyard_templates import ShipyardLogger
from shipyard_microsoft_onedrive import OneDriveClient

logger = ShipyardLogger.get_logger()


def main():
    try:
        client_id = os.getenv("ONEDRIVE_CLIENT_ID")
        client_secret = os.getenv("ONEDRIVE_CLIENT_SECRET")
        tenant = os.getenv("ONEDRIVE_TENANT")
        client = OneDriveClient(client_id, client_secret, tenant)
        client.connect()
        logger.authtest(
            "Successfully authenticated with OneDrive using basic authentication"
        )
        sys.exit(0)
    except Exception as e:
        logger.error(
            f"Failed to authenticate with OneDrive using basic authentication: {str(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
