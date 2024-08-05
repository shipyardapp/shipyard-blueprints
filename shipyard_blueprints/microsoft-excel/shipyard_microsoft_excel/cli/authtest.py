import os
import sys

from shipyard_templates import ShipyardLogger

from shipyard_microsoft_excel import ExcelClient

logger = ShipyardLogger.get_logger()


def main():
    logger.setLevel("AUTHTEST")

    sys.exit(
        ExcelClient(
            client_id=os.getenv("EXCEL_CLIENT_ID"),
            client_secret=os.getenv("EXCEL_CLIENT_SECRET"),
            tenant=os.getenv("EXCEL_TENANT"),
        ).connect()
    )


if __name__ == "__main__":
    main()
