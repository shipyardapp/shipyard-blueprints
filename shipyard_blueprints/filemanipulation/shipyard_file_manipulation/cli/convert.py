import os
import re
import argparse
import sys
import shipyard_bp_utils as shipyard
from shipyard_file_manipulation import convert
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
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match"},
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
    parser.add_argument(
        "--destination-file-format",
        dest="destination_file_format",
        choices={"tsv", "psv", "xlsx", "parquet", "stata", "hdf5"},
        required=True,
    )
    return parser.parse_args()


def create_fallback_destination_file_name(source_file_name, destination_file_format):
    """
    If a destination_file_name is not provided, uses the source_file_name with a removal of the compression extension.
    """

    format_extensions = {
        "tsv": ".tsv",
        "psv": ".psv",
        "xlsx": ".xlsx",
        "parquet": ".parquet",
        "stata": ".dta",
        "hdf5": ".h5",
    }

    file_name = os.path.basename(source_file_name)
    file_name = (
        f"{os.path.splitext(file_name)[0]}{format_extensions[destination_file_format]}"
    )
    return file_name


def main():
    try:
        args = get_args()

        src_file = args.source_file_name
        src_dir = shipyard.files.clean_folder_name(args.source_folder_name)
        src_path = shipyard.files.combine_folder_and_file_name(
            folder_name=src_dir, file_name=src_file
        )
        match_type = args.source_file_name_match_type
        target_dir = shipyard.files.clean_folder_name(args.destination_folder_name)
        target_file = args.destination_file_name
        target_format = args.destination_file_format
        if not target_file:
            target_file = create_fallback_destination_file_name(src_file, target_format)
        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, target_file
        )

        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        if match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(src_dir)
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(src_file)
            )
            if (n_matches := len(matching_file_names)) == 0:
                raise ExitCodeException(
                    message="No files found matching the provided regex.",
                    exit_code=EXIT_CODE_NO_FILE_MATCHES,
                )

            logger.info(f"{n_matches} files found. Preparing to convert...")
            print(f"{len(matching_file_names)} files found. Preparing to convert...")

            for index, key_name in enumerate(matching_file_names, start=1):
                target_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=target_dir,
                    destination_file_name=args.destination_file_name,
                    source_full_path=key_name,
                    file_number=index + 1,
                )
                logger.info(f"Target path is {target_path}")
                logger.info(
                    f"Converting file {index} of {len(matching_file_names)} to {target_format}"
                )
                convert(
                    src_path=key_name,
                    target_file_type=target_format,
                    target_path=target_path,
                )

        else:
            target_path = shipyard.files.determine_destination_full_path(
                destination_folder_name=target_dir,
                destination_file_name=args.destination_file_name,
                source_full_path=src_path,
            )
            convert(
                src_path=src_path,
                target_file_type=target_format,
                target_path=target_path,
            )

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
