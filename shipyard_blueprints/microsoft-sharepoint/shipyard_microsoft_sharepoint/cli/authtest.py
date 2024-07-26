import os
import sys
from shipyard_templates import ShipyardLogger
from shipyard_microsoft_sharepoint import SharePointClient

logger = ShipyardLogger.get_logger()


def main():
    client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
    tenant = os.getenv("SHAREPOINT_TENANT")
    client = SharePointClient(
        client_id=client_id, client_secret=client_secret, tenant=tenant
    )
    res = client.connect()
    sys.exit(res)


if __name__ == "__main__":
    main()
