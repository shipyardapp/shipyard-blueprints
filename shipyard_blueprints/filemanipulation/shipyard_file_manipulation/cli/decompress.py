import argparse
import os
import sys

import shipyard_bp_utils as shipyard
from shipyard_file_manipulation import decompress
from shipyard_file_manipulation.errors import (
    EXIT_CODE_UNKNOWN_ERROR,
    EXIT_CODE_FILE_NOT_FOUND,
    EXIT_CODE_NO_FILE_MATCHES,
)
from shipyard_templates import ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--compression",
        dest="compression",
        choices={"zip", "tar", "tar.bz2", "tar.gz"},
        required=True,
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--destination-file-name", dest="destination_file_name", required=False
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    return parser.parse_args()


def create_fallback_destination_file_name(source_file_name, compression):
    """
    If a destination_file_name is not provided, uses the source_file_name with a removal of the compression extension.
    """
    file_name = os.path.basename(source_file_name)
    file_name = file_name.replace(f".{compression}", "")
    return file_name


def main():
    try:
        args = get_args()
        compression = args.compression
        src_file = args.source_file_name
        src_dir = shipyard.files.clean_folder_name(args.source_folder_name)
        src_path = shipyard.files.combine_folder_and_file_name(
            folder_name=src_dir, file_name=src_file
        )
        target_dir = shipyard.files.clean_folder_name(args.destination_folder_name)
        target_file = args.destination_file_name

        if not target_file:
            target_file = create_fallback_destination_file_name(src_file, compression)

        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, target_file
        )

        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        logger.info("Decompressing file")
        decompress(src_path, target_path, compression)

    except FileNotFoundError as e:
        logger.error(f"File {src_path} not found")
        sys.exit(EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to convert files: {e}"
        )
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    else:
        logger.info(f"Successfully decompressed {src_path} to {target_path}")


if __name__ == "__main__":
    main()
