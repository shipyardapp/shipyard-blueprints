import argparse
import os
from zipfile import ZipFile
import tarfile
import glob
import re
import sys

import shipyard_bp_utils as shipyard
from shipyard_file_manipulation import compress
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
        default="Archive",
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        compression = args.compression
        src_file = args.source_file_name
        src_dir = shipyard.files.clean_folder_name(args.source_folder_name)
        src_path = shipyard.files.combine_folder_and_file_name(
            folder_name=src_dir, file_name=src_file
        )
        match_type = args.source_file_name_match_type
        target_dir = shipyard.files.clean_folder_name(args.destination_folder_name)
        target_file = args.destination_file_name
        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, target_file
        )

        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        if match_type == "regex_match":
            file_paths = shipyard.files.find_all_local_file_names(src_dir)
            matching_file_paths = shipyard.files.find_all_file_matches(
                file_paths, re.compile(src_file)
            )
            logger.debug(f"Matching files: {matching_file_paths}")
            if (n_matches := len(matching_file_paths)) == 0:
                raise ExitCodeException(
                    f"No files found matching the regex: {src_file}",
                    EXIT_CODE_NO_FILE_MATCHES,
                )
            logger.info(
                f"{n_matches} files found. Preparing to compress with {compression}..."
            )
            compress(matching_file_paths, target_path, compression)
            logger.info(f"All files were compressed into {target_path}")
        else:
            compress([src_path], target_path, compression)
            logger.info(f"Successfully compressed {src_path} into {target_path}")

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


if __name__ == "__main__":
    main()
