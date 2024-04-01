import argparse
import os
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import CloudStorage, ExitCodeException
from shipyard_templates.shipyard_logger import ShipyardLogger

from shipyard_sftp.sftp import SftpClient
from shipyard_sftp.utils import setup_connection, tear_down

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
        connection_args, key_path = setup_connection(args)
        sftp = SftpClient(**connection_args)

        source_folder_name = args.source_folder_name or "."

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        logger.debug(f"Source folder name: {source_folder_name}")

        sftp_files_full_paths = sftp.list_files_recursive(source_folder_name or ".")
        if not sftp_files_full_paths:
            raise ExitCodeException(
                f"No files found in the folder {source_folder_name}",
                CloudStorage.EXIT_CODE_FILE_NOT_FOUND,
            )
        # Get the relative path of the files which is the format that shipyard.file_match expects
        sftp_files = [
            os.path.relpath(path, start=source_folder_name)
            for path in sftp_files_full_paths
        ]
        files = shipyard.file_match(
            search_term=args.source_file_name,
            files=sftp_files,
            source_directory=source_folder_name,
            destination_directory=destination_folder_name,
            destination_filename=args.destination_file_name,
            match_type=args.source_file_name_match_type or "exact_match",
        )

        for file in files:
            source_file = file["source_path"]
            destination_full_path = file["destination_filename"]
            logger.info(f"Attempting to move {source_file} to {destination_full_path}...")
            sftp.move(source_file, destination_full_path)
            logger.info(f"Successfully moved {source_file} to {destination_full_path}")

    except ExitCodeException as e:
        logger.error(e)
        exit_code = e.exit_code
    except FileNotFoundError as e:
        logger.error(e)
        exit_code = CloudStorage.EXIT_CODE_FILE_MATCH_ERROR
    except Exception as e:
        logger.error(e)
        exit_code = CloudStorage.EXIT_CODE_UNKNOWN_ERROR
    finally:
        tear_down(key_path, sftp)
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
