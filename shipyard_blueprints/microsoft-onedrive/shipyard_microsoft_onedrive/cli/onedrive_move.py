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
    parser.add_argument("--src-file", required=True, help="Name of the file to upload")
    parser.add_argument("--src-dir", default="", required=False)
    parser.add_argument(
        "--dest-file",
        required=False,
        default="",
        help="Name of the moved file in Onedrive",
    )
    parser.add_argument(
        "--dest-dir",
        required=False,
        default="",
        help="Name of the directory to move the file into",
    )
    parser.add_argument(
        "--user-email", required=True, help="Email of the user to load the drive into"
    )
    parser.add_argument(
        "--match-type",
        required=False,
        default="exact_match",
        choices=["exact_match", "regex_match"],
        help="Type of match to use when moving the files",
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        access_token = args.access_token
        client_id = args.client_id
        client_secret = args.client_secret
        tenant = args.tenant
        src_file = args.src_file
        src_dir = args.src_dir
        dest_file = args.dest_file
        dest_dir = args.dest_dir
        user_email = args.user_email

        if not dest_file:
            dest_file = src_file

        onedrive = OneDriveClient(
            client_id=client_id,
            client_secret=client_secret,
            tenant=tenant,
            user_email=user_email,
        )
        onedrive.connect()

        user_id = onedrive.get_user_id()
        drive_id = onedrive.get_drive_id(user_id)
        if args.match_type == "exact_match":
            onedrive.move(
                src_name=src_file,
                src_dir=src_dir,
                target_name=dest_file,
                target_dir=dest_dir,
                drive_id=drive_id,
            )
        elif args.match_type == "regex_match":
            matches = onedrive.get_file_matches(src_dir, src_file, drive_id)
            if (n_matches := len(matches)) == 0:
                logger.error(f"No files found matching '{src_file}'")
                sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)
            logger.info(f"{n_matches} files found, preparing to download...")
            file_names = [file["name"] for file in matches]
            for index, file in enumerate(file_names, start=1):
                src_path = shipyard.files.combine_folder_and_file_name(src_dir, file)
                file_name = shipyard.files.determine_destination_file_name(
                    source_full_path=src_path,
                    destination_file_name=dest_file,
                    file_number=index if args.dest_file else None,
                )
                onedrive.move(
                    src_name=file,
                    src_dir=src_dir,
                    target_name=file_name,
                    target_dir=dest_dir,
                    drive_id=drive_id,
                )
            logger.info("Successfully moved all files")

    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
