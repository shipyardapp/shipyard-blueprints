import pandas as pd
import argparse
import os
import sys
import shipyard_bp_utils as shipyard
from shipyard_microsoft_onedrive import OneDriveClient
from shipyard_excel import ExcelClient
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

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
    parser.add_argument("--file-name", required=True, help="Name of the file to upload")
    parser.add_argument("--directory", default="", required=False)
    parser.add_argument(
        "--user-email", required=True, help="Email of the user to upload the file to"
    )
    parser.add_argument(
        "--onedrive-file-name",
        required=False,
        default="",
        help="Name of the file in OneDrive",
    )
    parser.add_argument(
        "--onedrive-directory",
        required=False,
        default="",
        help="Directory in OneDrive to upload the file to",
    )
    return parser.parse_args()


def is_valid_file(file_name: str):
    ext = os.path.splitext(file_name)[1]
    return ext in [".csv", ".xlsx"]


def is_csv(file_name: str):
    return os.path.splitext(file_name)[1] == ".csv"


def convert_to_excel(file_path: str):
    pd.read_csv(file_path).to_excel(file_path.replace(".csv", ".xlsx"), index=False)


def main():
    try:
        args = get_args()
        access_token = args.access_token
        client_id = args.client_id
        client_secret = args.client_secret
        tenant = args.tenant
        src_file = args.file_name
        src_dir = args.directory
        src_path = shipyard.files.combine_folder_and_file_name(src_dir, src_file)
        user_email = args.user_email
        target_file = args.onedrive_file_name
        target_dir = args.onedrive_directory

        target_file = target_file if target_file else src_file
        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, target_file
        )

        excel = ExcelClient(client_id, client_secret, tenant, user_email)
        excel.connect()

        user_id = excel.get_user_id()
        drive_id = excel.get_drive_id(user_id)
        if target_dir:
            folder_id = excel.get_folder_id(target_dir, drive_id)
            if not folder_id:
                excel.create_folder(target_dir, drive_id)

        if not is_valid_file(src_file):
            raise ValueError(
                f"Invalid file type: {src_file}. Only .csv and .xlsx files are supported"
            )

        if is_csv(src_file):
            convert_to_excel(src_path)
            src_file = src_file.replace(".csv", ".xlsx")
            src_path = src_path.replace(".csv", ".xlsx")
            target_path = target_path.replace(".csv", ".xlsx")
            logger.debug(f"New file path: {src_path}")

        excel.upload(src_path, drive_id, target_path)

    except ValueError as ve:
        logger.error(ve)
        sys.exit(CloudStorage.EXIT_CODE_INVALID_INPUT)
    except FileNotFoundError as e:
        logger.error(e)
        sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
