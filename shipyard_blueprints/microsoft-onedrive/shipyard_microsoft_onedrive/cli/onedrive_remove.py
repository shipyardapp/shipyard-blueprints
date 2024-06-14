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
    return parser.parse_args()


def main():
    try:
        args = get_args()
        access_token = args.access_token
        client_id = args.client_id
        client_secret = args.client_secret
        tenant = args.tenant
        user_email = args.user_email
        target_file = args.onedrive_file_name
        target_dir = args.onedrive_directory
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
        onedrive.remove(target_path, drive_id)

    except ExitCodeException as ec:
        logger.error(ec)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
