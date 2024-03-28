import argparse
import csv
import os

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger

from shipyard_googlesheets import utils

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-file-name", dest="file_name", default="", required=True
    )
    parser.add_argument("--tab-name", dest="tab_name", default=None, required=False)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default=None,
        required=True,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--cell-range", dest="cell_range", default="A1:ZZZ5000000", required=False
    )
    parser.add_argument(
        "--service-account",
        dest="gcp_application_credentials",
        default=None,
        required=True,
    )
    parser.add_argument("--drive", dest="drive", default=None, required=False)
    return parser.parse_args()


def download_google_sheet_file(
    service, spreadsheet_id, file_name, tab_name, cell_range, destination_file_name=None
):
    """
    Download th contents of a spreadsheet from Google Sheets to local storage in
    the current working directory.
    """
    local_path = os.path.normpath(f"{os.getcwd()}/{destination_file_name}")
    try:
        if tab_name:
            if utils.check_workbook_exists(
                service=service, spreadsheet_id=spreadsheet_id, tab_name=tab_name
            ):
                cell_range = f"{tab_name}!{cell_range}"
            else:
                print(f"The tab {tab_name} could not be found")
                raise SystemExit(1)
        sheet = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=cell_range)
            .execute()
        )

        if not sheet.get("values"):
            logger.warning(f"No values for {file_name}.. Not downloading")
            return

        values = sheet["values"]
        with open(local_path, "+w") as f:
            writer = csv.writer(f)
            writer.writerows(values)
        logger.info(f"Successfully downloaded {file_name} - {tab_name} to {local_path}")
    except Exception as e:
        logger.error(f"Failed to download {file_name} from Google Sheets")
        raise e


def main():
    args = get_args()
    tmp_file = utils.set_environment_variables(args)
    file_name = shipyard.clean_folder_name(args.file_name)
    tab_name = args.tab_name
    cell_range = args.cell_range or "A1:ZZZ5000000"
    drive = args.drive

    destination_folder_name = shipyard.clean_folder_name(args.destination_folder_name)
    if not os.path.exists(destination_folder_name) and (destination_folder_name != ""):
        os.makedirs(destination_folder_name)

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
            logger.error(f"Sheet {file_name} does not exist")
            raise SystemExit(1)

    if not args.destination_file_name:
        args.destination_file_name = f"{file_name} - {tab_name}.csv"

    destination_name = shipyard.combine_folder_and_file_name(
        destination_folder_name, args.destination_file_name
    )

    if len(destination_name.rsplit("/", 1)) > 1:
        path = destination_name.rsplit("/", 1)[0]
        if not os.path.exists(path):
            os.makedirs(path)

    download_google_sheet_file(
        service=service,
        tab_name=tab_name,
        spreadsheet_id=spreadsheet_id,
        file_name=file_name,
        cell_range=cell_range,
        destination_file_name=destination_name,
    )

    if tmp_file:
        logger.info(f"Removing temporary credentials file {tmp_file}")
        os.remove(tmp_file)


if __name__ == "__main__":
    main()
