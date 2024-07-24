import os
import sys
from shipyard_templates import ShipyardLogger
from shipyard_microsoft_sharepoint import SharePointClient

logger = ShipyardLogger.get_logger()


def main():
    try:
        client_id = os.getenv("SHAREPOINT_CLIENT_ID")
        client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
        tenant = os.getenv("SHAREPOINT_TENANT")
        client = SharePointClient(
            client_id=client_id, client_secret=client_secret, tenant=tenant
        )
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
