import os
import sys

from shipyard_templates import ShipyardLogger

from shipyard_microsoft_onedrive import OneDriveClient

logger = ShipyardLogger.get_logger()


def main():
    logger.setLevel("AUTHTEST")

    sys.exit(
        OneDriveClient(
            client_id=os.getenv("ONEDRIVE_CLIENT_ID"),
            client_secret=os.getenv("ONEDRIVE_CLIENT_SECRET"),
            tenant=os.getenv("ONEDRIVE_TENANT"),
        ).connect()
    )


if __name__ == "__main__":
    main()
