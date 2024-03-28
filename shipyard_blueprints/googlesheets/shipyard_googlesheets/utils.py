import json
import os
import tempfile
import socket

from google.oauth2 import service_account
from googleapiclient.discovery import build
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
socket.setdefaulttimeout(600)


def check_workbook_exists(service, spreadsheet_id, tab_name):
    """
    Checks if the workbook exists within the spreadsheet.
    """
    logger.debug(
        f"Checking if workbook {tab_name} exists in spreadsheet {spreadsheet_id}"
    )
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet["sheets"]
        exists = [True for sheet in sheets if sheet["properties"]["title"] == tab_name]
        logger.debug(f"exists: {exists}")
        return bool(exists)
    except Exception as e:
        logger.error(
            f"Failed to check workbook {tab_name} for spreadsheet " f"{spreadsheet_id}"
        )
        raise e


def get_spreadsheet_id_by_name(drive_service, file_name, drive):
    """
    Attempts to get sheet id from the Google Drive Client using the
    sheet name
    """
    logger.debug(f"Fetching spreadsheetId for {file_name}")
    try:
        drive_id = get_shared_drive_id(drive_service, drive) if drive else None
        query = 'mimeType="application/vnd.google-apps.spreadsheet"'
        query += f' and name = "{file_name}"'
        if drive:
            logger.debug(f"Searching for {file_name} in shared drive {drive}")
            results = (
                drive_service.files()
                .list(
                    q=str(query),
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=drive_id,
                    fields="files(id, name)",
                )
                .execute()
            )
        else:
            logger.debug(f"Searching for {file_name} in personal drive")
            results = drive_service.files().list(q=str(query)).execute()
        files = results["files"]
        for _file in files:
            return _file["id"]
        return None
    except Exception as e:
        logger.error(f"Failed to fetch spreadsheetId for {file_name}")
        raise e


def set_environment_variables(args):
    """
    Set GCP credentials as environment variables if they're provided via keyword
    arguments rather than seeded as environment variables. This will override
    system defaults.
    """
    logger.debug("Setting environment variables")
    credentials = args.gcp_application_credentials
    try:
        json_credentials = json.loads(credentials)
        fd, path = tempfile.mkstemp()
        logger.info(f"Storing json credentials temporarily at {path}")
        with os.fdopen(fd, "w") as tmp:
            tmp.write(credentials)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        logger.debug("Environment variables set successfully")
        return path
    except Exception:
        logger.error("Using specified json credentials file")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials
        return


def get_shared_drive_id(service, drive):
    """
    Search for the drive under shared Google Drives.
    """
    logger.debug(f"Searching for shared drive {drive}")
    drives = service.drives().list().execute()
    drive_id = None
    for _drive in drives["drives"]:
        if _drive["name"] == drive:
            drive_id = _drive["id"]
    logger.debug(f"Drive ID: {drive_id}")
    return drive_id


def get_service(credentials):
    """
    Attempts to create the Google Drive Client with the associated
    environment variables
    """
    logger.debug("Creating Google Drive Client with service account")
    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials, scopes=SCOPES
        )
        service = build("sheets", "v4", credentials=creds)
        drive_service = build("drive", "v3", credentials=creds)
        logger.debug("Google Drive Client created successfully")
        return service, drive_service
    except Exception as e:
        logger.error(
            f"Error accessing Google Drive with service account " f"{credentials}"
        )
        raise e


def add_workbook(service, spreadsheet_id, tab_name):
    """
    Adds a workbook to the spreadsheet.
    """
    logger.debug(f"Adding workbook {tab_name} to spreadsheet {spreadsheet_id}")
    try:
        request_body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": tab_name,
                        }
                    }
                }
            ]
        }

        return (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute()
        )
    except Exception as e:
        logger.error(e)
