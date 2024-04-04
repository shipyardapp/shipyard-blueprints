import argparse
import re
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import CloudStorage, ExitCodeException
from shipyard_templates.shipyard_logger import ShipyardLogger

from shipyard_sftp.sftp import SftpClient
from shipyard_sftp.utils import setup_connection, tear_down
from shipyard_sftp.exceptions import InvalidCredentialsError

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        choices={"exact_match", "regex_match"},
        default="exact_match",
        required=False,
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
    parser.add_argument("--key", dest="key", default=None, required=False)
    return parser.parse_args()


def main():
    key_path = None
    sftp = None
    exit_code = 0
    try:
        args = get_args()
        try:
            connection_args, key_path = setup_connection(args)
            sftp = SftpClient(**connection_args)
        except Exception as e:
            raise InvalidCredentialsError(e) from e

        source_file_name = args.source_file_name
        source_folder_name = args.source_folder_name
        source_full_path = shipyard.combine_folder_and_file_name(
            source_folder_name, source_file_name
        )
        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name.strip("/")
        )
        source_file_name_match_type = args.source_file_name_match_type or "exact_match"

        if source_file_name_match_type == "exact_match":
            destination_full_path = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )
            sftp.move(source_full_path, destination_full_path)

        elif source_file_name_match_type == "regex_match":
            file_names = sftp.list_files_recursive(source_folder_name or ".")
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )
            logger.info(f"{len(matching_file_names)} files found. Preparing to move...")

            for index, key_name in enumerate(matching_file_names, 1):
                destination_full_path = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=key_name,
                    file_number=None if len(matching_file_names) == 1 else index,
                )

                logger.info(f"Moving file {index} of {len(matching_file_names)}")
                sftp.move(key_name, destination_full_path)
    except ExitCodeException as e:
        logger.error(e)
        exit_code = e.exit_code
    except Exception as e:
        logger.error(e)
        exit_code = CloudStorage.EXIT_CODE_UNKNOWN_ERROR
    finally:
        tear_down(key_path, sftp)
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
