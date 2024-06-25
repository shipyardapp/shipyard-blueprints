import os
import sys
from shipyard_templates import ShipyardLogger
from shipyard_excel import ExcelClient

logger = ShipyardLogger.get_logger()


def main():
    try:
        client_id = os.getenv("EXCEL_CLIENT_ID")
        client_secret = os.getenv("EXCEL_CLIENT_SECRET")
        tenant = os.getenv("EXCEL_TENANT")
        client = ExcelClient(client_id, client_secret, tenant)
        client.connect()
        logger.authtest(
            "Successfully authenticated with Excel using basic authentication"
        )
        sys.exit(0)
    except Exception as e:
        logger.error(
            f"Failed to authenticate with Excel using basic authentication: {str(e)}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
