import os
import sys

from shipyard_templates import ShipyardLogger

from shipyard_googlesheets import GoogleSheetsClient

logger = ShipyardLogger.get_logger()


def main():
    try:
        credentials = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        credentials = credentials.replace("\n", "\\n")

        GoogleSheetsClient(service_account=credentials).connect()
        logger.authtest("Successfully connected to google sheets")
        sys.exit(0)
    except Exception as e:
        logger.authtest(
            f"Could not connect to Google Sheets with the Service Account provided due to {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
