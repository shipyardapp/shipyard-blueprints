import argparse
import os
import re
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

from shipyard_ftp.ftp import FtpClient

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        choices={"exact_match", "regex_match"},
        required=True,
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default=None,
        required=False,
    )
    parser.add_argument("--host", dest="host", default=None, required=True)
    parser.add_argument("--port", dest="port", default=21, required=True)
    parser.add_argument("--username", dest="username", default=None, required=False)
    parser.add_argument("--password", dest="password", default=None, required=False)
    return parser.parse_args()


def main():
    try:
        args = get_args()
        host = args.host
        port = args.port
        username = args.username
        password = args.password
        source_file_name = args.source_file_name
        source_folder_name = args.source_folder_name
        destination_filename = args.destination_file_name or source_file_name
        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        source_file_name_match_type = args.source_file_name_match_type
        errors = []
        ftp_client = FtpClient(host=host, port=port, user=username, pwd=password)
        if source_file_name_match_type == "exact_match":
            source_full_path = shipyard.combine_folder_and_file_name(
                folder_name=f"{os.getcwd()}/{source_folder_name}",
                file_name=source_file_name,
            )
            destination_full_path = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=destination_filename,
                source_full_path=source_full_path,
            )
            if len(destination_full_path.split("/")) > 1:
                path, file_name = destination_full_path.rsplit("/", 1)
                ftp_client.create_new_folders(path)

            if os.path.isfile(source_full_path):
                ftp_client.upload(source_full_path, destination_full_path)

            else:
                raise ExitCodeException(
                    message=f"{source_full_path} does not exist",
                    exit_code=ftp_client.EXIT_CODE_FILE_MATCH_ERROR,
                )

        elif source_file_name_match_type == "regex_match":
            file_names = shipyard.find_all_local_file_names(source_folder_name)
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )

            if number_of_matches:=len(matching_file_names) == 0:
                logger.error(f'No matches were found for regex "{source_file_name}".')
                sys.exit(ftp_client.EXIT_CODE_FILE_MATCH_ERROR)

            logger.info(
                f"{number_of_matches} files found. Preparing to upload..."
            )

            for index, key_name in enumerate(matching_file_names, start=1):
                destination_full_path = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=destination_filename,
                    source_full_path=key_name,
                    file_number=index if number_of_matches > 1 else None,
                )
                if len(destination_full_path.split("/")) > 1:
                    path, file_name = destination_full_path.rsplit("/", 1)
                    ftp_client.create_new_folders(destination_path=path)
                logger.info(f"Uploading file {index} of {number_of_matches}")
                if os.path.isfile(key_name):
                    ftp_client.upload(key_name, destination_full_path)
                else:
                    logger.warning(f"{key_name} does not exist")
                    errors.append(key_name)
        if errors:
            logger.error("Failed to upload the following files:\n" + "\n".join(errors))
            sys.exit(ftp_client.EXIT_CODE_FILE_MATCH_ERROR)

    except ExitCodeException as e:
        logger.error(e.message)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
