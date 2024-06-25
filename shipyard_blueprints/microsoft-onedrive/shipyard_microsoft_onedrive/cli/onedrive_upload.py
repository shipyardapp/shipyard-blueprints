import os
import sys
import argparse
import shipyard_bp_utils as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage
from shipyard_microsoft_onedrive import OneDriveClient

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
    parser.add_argument(
        "--match-type",
        required=False,
        default="exact_match",
        choices=["exact_match", "regex_match"],
        help="Type of matching to use when finding the file in the directory",
    )
    return parser.parse_args()


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

        onedrive = OneDriveClient(
            client_id=client_id,
            client_secret=client_secret,
            tenant=tenant,
            user_email=user_email,
        )
        onedrive.connect()

        user_id = onedrive.get_user_id()
        drive_id = onedrive.get_drive_id(user_id)
        if target_dir:
            folder_id = onedrive.get_folder_id(target_dir, drive_id)
            if not folder_id:
                onedrive.create_folder(target_dir, drive_id)

        if args.match_type == "exact_match":
            onedrive.upload(src_path, drive_id, target_path)
        elif args.match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(src_dir)
            file_matches = shipyard.files.find_all_file_matches(file_names, src_file)
            logger.debug(f"File matches: {file_matches}")
            if (n_matches := len(file_matches)) == 0:
                raise FileNotFoundError(f"No files found matching {src_file}")
            logger.info(f"{n_matches} files found. Preparing to upload...")
            for i, file in enumerate(file_matches, start=1):
                file_ext = os.path.splitext(file)[1]
                dest_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=target_dir,
                    destination_file_name=target_file,
                    source_full_path=file,
                    file_number=i if target_file else None,
                )
                dest_path += file_ext

                onedrive.upload(file, drive_id, dest_path)

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
