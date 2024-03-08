import argparse
import re
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ExitCodeException, CloudStorage
from shipyard_templates.shipyard_logger import ShipyardLogger

from shipyard_ftp.exceptions import EXIT_CODE_NO_MATCHES_FOUND, EXIT_CODE_DOWNLOAD_ERROR
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
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument("--host", dest="host", default=None, required=True)
    parser.add_argument("--port", dest="port", default=21, required=True)
    parser.add_argument("--username", dest="username", default=None, required=False)
    parser.add_argument("--password", dest="password", default=None, required=False)
    return parser.parse_args()


def find_matching_files(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for file_name in file_names:
        fname = file_name.rsplit("/", 1)[-1]
        if re.search(file_name_re, fname):
            matching_file_names.append(file_name)

    return matching_file_names


def main():
    try:
        args = get_args()
        host = args.host
        port = args.port
        username = args.username
        password = args.password
        source_file_name = args.source_file_name
        source_folder_name = shipyard.clean_folder_name(args.source_folder_name)
        source_full_path = shipyard.combine_folder_and_file_name(
            folder_name=source_folder_name, file_name=source_file_name
        )
        source_file_name_match_type = args.source_file_name_match_type
        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        errors = []
        if destination_folder_name:
            shipyard.create_folder_if_dne(destination_folder_name)
        client = FtpClient(host=host, port=port, user=username, pwd=password)

        logger.info("Beginning the download process...")
        if source_file_name_match_type == "exact_match":
            try:
                destination_name = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=source_full_path,
                )
                client.download(source_full_path, destination_name)

            except Exception as e:
                logger.error(
                    f"Failed to download {source_full_path} due to {e}\n"
                    f"Most likely, the file name/folder name you specified has typos or the full folder name was "
                    f"not provided. Check these and try again."
                )
                raise e
        elif source_file_name_match_type == "regex_match":
            folders = [source_folder_name]
            files = []
            while folders:
                folder_filter = folders[0]
                files, folders = client.find_files_in_directory(
                    folder_filter=folder_filter, files=files, folders=folders
                )

            matching_file_names = shipyard.find_all_file_matches(
                files, re.compile(source_file_name)
            )

            if number_of_matches := len(matching_file_names) == 0:
                logger.info(f'No matches were found for regex "{source_file_name}".')
                sys.exit(EXIT_CODE_NO_MATCHES_FOUND)

            logger.info(f"{number_of_matches} files found. Preparing to download...")
            for index, file_name in enumerate(matching_file_names, start=1):
                destination_name = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=file_name,
                    file_number=index if number_of_matches > 1 else None,
                )

                logger.info(
                    f"Attempting to download file {index} of {number_of_matches}: {file_name} to {destination_name}..."
                )
                try:
                    client.download(file_name, destination_name)
                    logger.info(
                        f"Successfully downloaded {file_name} to {destination_name}."
                    )
                except Exception as e:
                    logger.error(f"Failed to download {file_name} due to {e}")
                    errors.append(file_name)

        logger.info("Download process complete.")

        if errors:
            logger.error(
                "Failed to download the following files:\n" + "\n".join(errors)
            )
            sys.exit(EXIT_CODE_DOWNLOAD_ERROR)
    except ExitCodeException as e:
        logger.error(e.message)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An error occurred during the download process: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
