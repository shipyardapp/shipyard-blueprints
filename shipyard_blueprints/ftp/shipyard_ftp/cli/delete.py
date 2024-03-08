import argparse
import re
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

from shipyard_ftp.exceptions import (
    EXIT_CODE_NO_MATCHES_FOUND,
    EXIT_CODE_FTP_DELETE_ERROR,
)
from shipyard_ftp.ftp import FtpClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--host", dest="host", default=None, required=True)
    parser.add_argument("--port", dest="port", default=21, required=True)
    parser.add_argument("--username", dest="username", default=None, required=False)
    parser.add_argument("--password", dest="password", default=None, required=False)
    parser.add_argument(
        "--file-name-match-type",
        dest="file_name_match_type",
        choices={"exact_match", "regex_match"},
        required=False,
        default="exact_match",
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=True
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        host = args.host
        port = args.port
        username = args.username
        password = args.password
        file_name_match_type = args.file_name_match_type
        file_name = args.source_file_name
        folder_name = shipyard.clean_folder_name(args.source_folder_name)
        errors = []
        client = FtpClient(host=host, port=port, user=username, pwd=password)

        if file_name_match_type == "regex_match":
            folders = [folder_name]
            files = []
            while folders:
                folder_filter = folders[0]

                files, folders = client.find_files_in_directory(
                    folder_filter=folder_filter, files=files, folders=folders
                )

            matching_file_names = shipyard.find_all_file_matches(
                files, re.compile(file_name)
            )

            if number_of_matches := len(matching_file_names) == 0:
                logger.info(f'No matches were found for regex "{file_name}".')
                sys.exit(EXIT_CODE_NO_MATCHES_FOUND)

            for index, file_name in enumerate(matching_file_names, start=1):
                logger.info(f"Deleting file {index} out of {number_of_matches}")

                try:
                    client.remove(file_name)
                except Exception:
                    logger.error(f"Failed to delete {file_name}... Skipping")
                    errors.append(file_name)

        elif file_name_match_type == "exact_match":
            file_path = shipyard.combine_folder_and_file_name(folder_name, file_name)
            try:
                client.remove(file_path)
            except Exception as e:
                logger.error(
                    "Check the file/folder name for misspellings and ensure that entire folder name was "
                    "provided"
                )
                raise e
        if errors:
            logger.error("Failed to delete the following files:\n" + "\n".join(errors))
            sys.exit(EXIT_CODE_FTP_DELETE_ERROR)

    except ExitCodeException as e:
        logger.error(e.message)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
