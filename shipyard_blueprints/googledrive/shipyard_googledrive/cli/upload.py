import os
import sys
import argparse
import re

from shipyard_templates import ExitCodeException
from shipyard_googledrive import GoogleDriveClient, drive_utils
from shipyard_bp_utils import files


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-account", dest="service_account", required=True)
    parser.add_argument("--drive", required=False, default="")
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", required=False, default=""
    )
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        required=True,
        default="exact_match",
        choices={"exact_match", "regex_match"},
    )
    parser.add_argument(
        "--destination-file-name", dest="destination_file_name", required=False
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        required=False,
        default="",
    )
    return parser.parse_args()


def main():
    args = get_args()
    client = GoogleDriveClient(
        service_account=args.service_account, shared_drive_name=args.drive
    )
    if args.source_folder_name != "":
        source_path = os.path.join(args.source_folder_name, args.source_file_name)
    else:
        source_path = args.source_file_name

    drive_folder = (
        args.destination_folder_name if args.destination_folder_name != "" else None
    )
    drive_file_name = (
        args.destination_file_name if args.destination_file_name != "" else None
    )
    drive_name = args.drive if args.drive != "" else None

    # for multiple file uploads

    if args.source_file_name_match_type == "regex_match":
        files_listed = drive_utils.list_local_files(
            args.source_folder_name if args.source_folder_name != "" else None
        )
        file_matches = files.find_all_file_matches(
            file_names=files_listed,
            file_name_re=re.compile(args.source_file_name),
        )
        if len(file_matches) == 0:
            client.logger.error(
                f"No files found matching regex {args.source_file_name}"
            )
            sys.exit(client.EXIT_CODE_FILE_NOT_FOUND)
        for index, file in enumerate(file_matches, start=1):
            if args.source_folder_name:
                file_path = os.path.join(args.source_folder_name, file)
            else:
                file_path = os.path.join(os.getcwd(), file)
            new_file_name = files.determine_destination_file_name(
                source_full_path=file,
                destination_file_name=drive_file_name,
                file_number=index,
            )
            client.upload(
                file_path=file_path,
                drive_folder=drive_folder,
                drive=drive_name,
                drive_file_name=new_file_name,
            )
            client.logger.info(f"Processed {file_path}")

    # for single file uploads
    else:  # handles the case for exact_match, any other option will receive an argument error
        try:
            client.upload(
                file_path=source_path,
                drive_folder=drive_folder,
                drive=drive_name,
                drive_file_name=drive_file_name,
            )
        except ExitCodeException as ec:
            client.logger.error(ec.message)
            sys.exit(ec.exit_code)
        except Exception as e:
            client.logger.error("Error in uploading file to drive")
            client.logger.exception(str(e))
            sys.exit(client.EXIT_CODE_UPLOAD_ERROR)
        else:
            client.logger.info("Successfully loaded file(s) to Google Drive!")


if __name__ == "__main__":
    main()
