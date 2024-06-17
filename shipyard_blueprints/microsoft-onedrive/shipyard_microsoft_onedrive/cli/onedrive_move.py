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

        # src_path = shipyard.files.combine_folder_and_file_name(src_dir, src_file)
        if not dest_file:
            dest_file = src_file
        # dest_path = shipyard.files.combine_folder_and_file_name(dest_dir, dest_file)

        onedrive = None
        if client_id and client_secret and tenant:
            onedrive = OneDriveClient(auth_type="basic")
            onedrive.connect(client_id, client_secret, tenant)
        else:
            onedrive = OneDriveClient(auth_type="oauth", access_token=access_token)

        user_id = onedrive.get_user_id(user_email)
        drive_id = onedrive.get_drive_id(user_id)
        onedrive.move(
            src_name=src_file,
            src_dir=src_dir,
            target_name=dest_file,
            target_dir=dest_dir,
            drive_id=drive_id,
        )

    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
