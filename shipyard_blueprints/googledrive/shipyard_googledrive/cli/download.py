import os
import sys
import argparse

from shipyard_templates import ExitCodeException
from shipyard_googledrive import GoogleDriveClient, drive_utils
from shipyard_bp_utils import files


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        choices={"exact_match", "regex_match"},
        required=True,
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--service-account",
        dest="service_account",
        required=True,
    )
    parser.add_argument("--drive", dest="drive", default="", required=False)
    return parser.parse_args()


def main():
    args = get_args()
    dest_file_name = (
        args.destination_file_name if args.destination_file_name != "" else None
    )
    dest_folder_name = (
        args.destination_folder_name if args.destination_folder_name != "" else None
    )
    source_folder = args.source_folder_name if args.source_folder_name != "" else None

    drive = args.drive if args.drive != "" else None

    client = GoogleDriveClient(service_account=args.service_account)

    try:
        drive_id = (
            drive_utils.get_drive_id(drive_id=drive, service=client.service)
            if drive
            else None
        )
        folder_id = drive_utils.get_folder_id(
            folder_identifier=source_folder, service=client.service, drive_id=drive_id
        )

        # for downloading multiple file names
        if args.source_file_name_match_type == "regex_match":
            drive_files = drive_utils.get_file_matches(
                service=client.service,
                pattern=args.source_file_name,
                folder_id=folder_id,
                drive_id=drive_id,
            )

            client.logger.info(
                f"Found {len(drive_files)} files, preparing to download..."
            )
            for index, file in enumerate(drive_files):
                file_id = file["id"]
                file_name = file["name"]
                # rename the file appropriately
                dest_name = files.determine_destination_file_name(
                    source_full_path=file_name,
                    destination_file_name=dest_file_name,
                    file_number=index,
                )
                client.download(
                    file_id=file_id,
                    drive_file_name=file_name,
                    destination_file_name=dest_name,
                    destination_path=dest_folder_name,
                    drive=drive_id,
                    drive_folder=folder_id,
                )
                client.logger.info(f"Processed {dest_name}")
        # for single file downloads
        else:  # handles the case for exact_match, any other option will receive an argument error
            file_id = drive_utils.get_file_id(
                file_name=args.source_file_name,
                drive_id=drive_id,
                folder_id=folder_id,
                service=client.service,
            )
            if not file_id:
                client.logger.error(
                    f"File {args.source_file_name} not found or is not accessible to {client.email}. Ensure that the file exists in Google Drive and is shared with the service account"
                )
                sys.exit(client.EXIT_CODE_FILE_ACCESS_ERROR)

            client.download(
                file_id=file_id,
                drive_file_name=args.source_file_name,
                destination_file_name=dest_file_name,
                destination_path=dest_folder_name,
                drive=drive_id,
                drive_folder=folder_id,
            )

    except ExitCodeException as ec:
        client.logger.error(ec.message, sys.exit(ec.exit_code))

    except Exception as e:
        client.logger.error(
            f"Error in downloading the file from Google Drive due to {str(e)}"
        )
        sys.exit(client.EXIT_CODE_UNKNOWN_ERROR)

    else:
        client.logger.info("Successfully downloaded file(s) from Google Drive")


if __name__ == "__main__":
    main()
