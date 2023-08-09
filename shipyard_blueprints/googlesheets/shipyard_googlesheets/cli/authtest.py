import os
import sys
from shipyard_blueprints import GoogleSheetsClient


def main():
    client = GoogleSheetsClient(
        service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    try:
        service, drive = client.connect()
        client.logger.info("Successfully connected to google sheets")
        sys.exit(0)
    except Exception as e:
        client.logger.error(
            "Could not connect to Google Sheets with the Service Account provided"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
