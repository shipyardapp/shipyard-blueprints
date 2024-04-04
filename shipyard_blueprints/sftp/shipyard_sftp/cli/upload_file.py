import argparse
import os
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
    parser.add_argument("--key", dest="key", default=None, required=False)
    return parser.parse_args()


def main():
    sftp = None
    key_path = None
    exit_code = 0
    try:
        args = get_args()

        try:
            connection_args, key_path = setup_connection(args)
            sftp = SftpClient(**connection_args)
        except Exception as e:
            raise InvalidCredentialsError(e) from e

        source_folder_name = args.source_folder_name

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name.strip("/")
        )

        source_file_name_match_type = args.source_file_name_match_type or "exact_match"
        files = shipyard.file_match(
            search_term=args.source_file_name,
            files=shipyard.fetch_file_paths_from_directory(source_folder_name),
            source_directory=source_folder_name,
            destination_directory=destination_folder_name,
            destination_filename=args.destination_file_name,
            match_type=source_file_name_match_type,
        )

        for file in files:
            source_file = file["source_path"]
            destination_full_path = file["destination_filename"]
            if not os.path.isfile(source_file):
                raise ExitCodeException(
                    f"{source_file} is not a file", sftp.EXIT_CODE_FILE_MATCH_ERROR
                )
            sftp.upload(source_file, destination_full_path)

    except ExitCodeException as e:
        logger.error(e)
        exit_code = e.exit_code
    except FileNotFoundError as e:
        logger.error(e)
        exit_code = CloudStorage.EXIT_CODE_FILE_NOT_FOUND
    except Exception as e:
        logger.error(e)
        exit_code = CloudStorage.EXIT_CODE_UNKNOWN_ERROR
    finally:
        tear_down(key_path, sftp)
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
