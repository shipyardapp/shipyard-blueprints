import argparse
import sys

import shipyard_bp_utils as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

from shipyard_microsoft_onedrive import OneDriveClient, utils

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
        "--user-email", required=True, help="Email of the user to upload the file to"
    )
    parser.add_argument(
        "--onedrive-file-name", required=True, help="Name of the file in OneDrive"
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
        help="Type of match to use when downloading the files",
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        credentials = utils.get_credential_group(args)
        src_file = args.onedrive_file_name
        src_dir = args.onedrive_directory
        target_path = shipyard.files.combine_folder_and_file_name(src_dir, src_file)

        onedrive = OneDriveClient(**credentials)
        user_id = onedrive.get_user_id(args.user_email)
        drive_id = onedrive.get_drive_id(user_id)
        if args.match_type == "exact_match":
            onedrive.remove(target_path, drive_id)
        elif args.match_type == "regex_match":
            matches = onedrive.get_file_matches(src_dir, src_file, drive_id)
            if (n_matches := len(matches)) == 0:
                logger.error(f"No files found matching '{src_file}'")
                sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)
            logger.info(f"{n_matches} files found, preparing to download...")
            file_names = [file["name"] for file in matches]
            for file in file_names:
                if src_dir:
                    target = shipyard.files.combine_folder_and_file_name(src_dir, file)
                else:
                    target = file
                onedrive.remove(target, drive_id)
            logger.info("Successfully removed all files")

    except ExitCodeException as ec:
        logger.error(ec)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
