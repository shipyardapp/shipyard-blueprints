import os
import sys
from shipyard_blueprints import GoogleDriveClient


def main():
    drive = GoogleDriveClient(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    try:
        conn = drive.connect()
        drive.logger.info("Successfully connected to Google Drive")
        sys.exit(0)
    except Exception as e:
        drive.logger.error(
            "Could not connect to Google Drive with the service account provided"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
