import argparse
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
    parser.add_argument(
        "--destination-file-name", dest="file_name", default="", required=False
    )
    parser.add_argument(
        "--cell-range", dest="cell_range", default="A1:ZZZ5000000", required=False
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


def clear_google_sheet(service, file_name, cell_range, spreadsheet_id, tab_name):
    """
    Clears data from a single Google Sheet.
    """
    try:
        if not spreadsheet_id:
            file_metadata = {
                "properties": {"title": file_name},
                "namedRanges": {"range": cell_range},
            }
            spreadsheet = (
                service.spreadsheets()
                .create(body=file_metadata, fields="spreadsheetId")
                .execute()
            )
            spreadsheet_id = spreadsheet["spreadsheetId"]

        if tab_name:
            cell_range = f"{tab_name}!{cell_range}"

        response = (
            service.spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=cell_range)
            .execute()
        )
    except Exception as e:
        if hasattr(e, "content"):
            err_msg = json.loads(e.content)
            logger.error(
                f"Failed to clear spreadsheet. Response from Google {file_name}: {err_msg}"
            )
        else:
            logger.errpr(f"Failed to clear spreadsheet {file_name}")
        raise e

    logger.info(f"{file_name} successfully cleared between range {cell_range}.")


def main():
    args = get_args()
    tmp_file = utils.set_environment_variables(args)
    file_name = shipyard.clean_folder_name(args.file_name)
    tab_name = args.tab_name
    cell_range = args.cell_range or "A1:ZZZ5000000"
    drive = args.drive

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
    clear_google_sheet(
        service=service,
        file_name=file_name,
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        cell_range=cell_range,
    )

    if tmp_file:
        logger.info(f"Removing temporary credentials file {tmp_file}")
        os.remove(tmp_file)


if __name__ == "__main__":
    main()
