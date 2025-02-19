# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "shipyard-microsoft-sharepoint",
#     "shipyard-templates"
# ]
# ///
import os
import sys
from shipyard_templates import ShipyardLogger
from shipyard_microsoft_sharepoint import SharePointClient

logger = ShipyardLogger.get_logger().setLevel("AUTHTEST")


def main():
    sys.exit(
        SharePointClient(
            client_id=os.getenv("SHAREPOINT_CLIENT_ID"),
            client_secret=os.getenv("SHAREPOINT_CLIENT_SECRET"),
            tenant=os.getenv("SHAREPOINT_TENANT"),
        ).connect()
    )


if __name__ == "__main__":
    main()
