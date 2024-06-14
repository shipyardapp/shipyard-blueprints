import os
import sys
from shipyard_templates import ShipyardLogger
from shipyard_microsoft_onedrive import OneDriveClient

logger = ShipyardLogger.get_logger()


def main():
    try:
        auth_type = os.getenv("MS_ONEDRIVE_AUTH_TYPE")
        if auth_type == "basic":
            client_id = os.getenv("MS_ONEDRIVE_CLIENT_ID")
            client_secret = os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE")
            tenant = os.getenv("MS_ONEDRIVE_TENANT")
            client = OneDriveClient(auth_type)
            client.connect(client_id, client_secret, tenant, None)
            print(f"Access token is {client.access_token}")
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
