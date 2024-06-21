import pandas as pd
import argparse
import os
import sys
import shipyard_bp_utils as shipyard
from shipyard_microsoft_onedrive import OneDriveClient
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

        onedrive = None
        if client_id and client_secret and tenant:
            onedrive = OneDriveClient(auth_type="basic")
            onedrive.connect(client_id, client_secret, tenant)
        else:
            onedrive = OneDriveClient(auth_type="oauth", access_token=access_token)

        user_id = onedrive.get_user_id(user_email)
        drive_id = onedrive.get_drive_id(user_id)
        if target_dir:
            folder_id = onedrive.get_folder_id(target_dir, drive_id)
            if not folder_id:
                onedrive.create_folder(target_dir, drive_id)

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

        onedrive.upload(src_path, drive_id, target_path)
        # elif args.match_type == "regex_match":
        #     file_names = shipyard.files.find_all_local_file_names(src_dir)
        #     file_matches = shipyard.files.find_all_file_matches(file_names, src_file)
        #     if (n_matches := len(file_matches)) == 0:
        #         raise FileNotFoundError(f"No files found matching {src_file}")
        #     logger.info(f"{n_matches} files found. Preparing to upload...")
        #     for i, file in enumerate(file_matches, start=1):
        #         file_ext = os.path.splitext(file)[1]
        #         if not is_valid_file(file):
        #             raise ValueError(f"Invalid file type: {file}. Only .csv and .xlsx files are supported")
        #         if is_csv(file):
        #             convert_to_excel(file)
        #             file = src_file.replace('.csv','.xlsx')
        #         dest_path = shipyard.files.determine_destination_full_path(
        #             destination_folder_name=target_dir,
        #             destination_file_name=target_file,
        #             source_full_path=file,
        #             file_number=i if target_file else None,
        #         )
        #         dest_path += file_ext
        #
        #         onedrive.upload(file, drive_id, dest_path)

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
