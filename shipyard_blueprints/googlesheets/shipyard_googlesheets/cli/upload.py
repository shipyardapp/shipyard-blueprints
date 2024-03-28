import argparse
import csv
import json
import os
import socket

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger

from shipyard_googlesheets import utils

logger = ShipyardLogger.get_logger()

socket.setdefaulttimeout(600)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument(
        "--destination-file-name", dest="file_name", default="", required=False
    )
    parser.add_argument(
        "--starting-cell", dest="starting_cell", default="A1", required=False
    )
    parser.add_argument("--tab-name", dest="tab_name", default=None, required=False)
    parser.add_argument(
        "--service-account",
        dest="gcp_application_credentials",
        default=None,
        required=True,
    )
    parser.add_argument("--drive", dest="drive", default=None, required=False)
    return parser.parse_args()


def upload_google_sheets_file(
    service, file_name, source_full_path, starting_cell, spreadsheet_id, tab_name
):
    """
    Uploads a single file to Google Sheets.
    """
    try:
        if not spreadsheet_id:
            file_metadata = {
                "properties": {"title": file_name},
                "namedRanges": {"range": starting_cell},
            }
            spreadsheet = (
                service.spreadsheets()
                .create(body=file_metadata, fields="spreadsheetId")
                .execute()
            )
            spreadsheet_id = spreadsheet["spreadsheetId"]

        # check if the workbook exists and create it if it doesn't
        workbook_exists = utils.check_workbook_exists(
            service=service, spreadsheet_id=spreadsheet_id, tab_name=tab_name
        )
        if not workbook_exists:
            utils.add_workbook(
                service=service, spreadsheet_id=spreadsheet_id, tab_name=tab_name
            )

        data = []
        with open(
            source_full_path, encoding="utf-8", newline=""
        ) as f:  # adding unicode encoding
            reader = csv.reader((line.replace("\0", "") for line in f), delimiter=",")
            data.extend(row for row in reader if set(row) != {""})
        _range = f"{starting_cell}:ZZZ5000000" if starting_cell else "A1:ZZZ5000000"
        if tab_name:
            _range = f"{tab_name}!{_range}"

        body = {
            "value_input_option": "RAW",
            "data": [{"values": data, "range": _range, "majorDimension": "ROWS"}],
        }
        response = (
            service.spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
    except Exception as e:
        if isinstance(e, FileNotFoundError):
            logger.error(f"File {source_full_path} does not exist.")
        elif hasattr(e, "content"):
            err_msg = json.loads(e.content)
            if "workbook above the limit" in err_msg["error"]["message"]:
                logger.error(
                    f"Failed to upload due to input csv size {source_full_path}"
                    " being to large (Limit is 5,000,000 cells)"
                )
        else:
            logger.error(
                f"Failed to upload spreadsheet {source_full_path} to " f"{file_name}"
            )
        raise e

    logger.info(f"{source_full_path} successfully uploaded to {file_name}")


def main():
    args = get_args()
    tmp_file = utils.set_environment_variables(args)
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = shipyard.combine_folder_and_file_name(
        folder_name=f"{os.getcwd()}/{source_folder_name}", file_name=source_file_name
    )
    file_name = shipyard.clean_folder_name(args.file_name)
    tab_name = args.tab_name
    starting_cell = args.starting_cell or "A1"
    drive = args.drive

    if not os.path.isfile(source_full_path):
        logger.error(f"{source_full_path} does not exist")
        raise SystemExit(1)

    if tmp_file:
        service, drive_service = utils.get_service(credentials=tmp_file)
    else:
        service, drive_service = utils.get_service(
            credentials=args.gcp_application_credentials
        )

    spreadsheet_id = utils.get_spreadsheet_id_by_name(
        drive_service=drive_service, file_name=file_name, drive=drive
    )
    if not spreadsheet_id:
        if len(file_name) >= 44:
            spreadsheet_id = file_name
        else:
            logger.error(f"The spreadsheet {file_name} does not exist")
            raise SystemExit(1)

    # check if workbook exists in the spreadsheet
    upload_google_sheets_file(
        service=service,
        file_name=file_name,
        source_full_path=source_full_path,
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        starting_cell=starting_cell,
    )

    if tmp_file:
        logger.info(f"Removing temporary credentials file {tmp_file}")
        os.remove(tmp_file)


if __name__ == "__main__":
    main()
