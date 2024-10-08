import argparse
import sys

import shipyard_bp_utils as shipyard
from shipyard_microsoft_onedrive import utils
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

from shipyard_microsoft_excel import ExcelClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--access-token", required=False, help="Access token to use for authentication"
    )
    parser.add_argument(
        "--client-id", required=False, help="Client ID for OAuth authentication"
    )
    parser.add_argument(
        "--client-secret", required=False, help="Client secret for OAuth authentication"
    )
    parser.add_argument(
        "--tenant", required=False, help="Tenant for OAuth authentication"
    )
    parser.add_argument(
        "--file-name", required=False, help="The name of the file once it is downloaded"
    )
    parser.add_argument("--directory", default="", required=False)
    parser.add_argument(
        "--user-email", required=True, help="Email of the user to upload the file to"
    )
    parser.add_argument(
        "--onedrive-file-name",
        required=True,
        default="",
        help="Name of the file in OneDrive to download",
    )
    parser.add_argument(
        "--onedrive-directory",
        required=False,
        default="",
        help="Directory in OneDrive to upload the file to",
    )
    parser.add_argument(
        "--sheet-name", required=False, default="Sheet1", dest="sheet_name"
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        credentials = utils.get_credential_group(args)

        src_file = args.onedrive_file_name
        src_dir = args.onedrive_directory

        target_file = args.file_name or src_file
        target_dir = args.directory
        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, target_file
        )
        sheet_name = args.sheet_name

        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        excel = ExcelClient(**credentials)

        user_id = excel.get_user_id(args.user_email)
        drive_id = excel.get_drive_id(user_id)
        file_id = excel.get_file_id(src_file, drive_id, src_dir)

        sheet_id = excel.get_sheet_id(sheet_name, file_id, drive_id)

        sheet_data = excel.get_sheet_data_as_df(file_id, sheet_id, drive_id)
        logger.info("Successfully fetched sheet data")

        sheet_data.to_csv(target_path, index=False)
        logger.info(f"Successfully saved sheet data to {target_path} ")

    except ExitCodeException as ec:
        logger.error(ec)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
