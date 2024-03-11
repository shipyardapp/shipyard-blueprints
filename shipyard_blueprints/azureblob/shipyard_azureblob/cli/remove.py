import argparse
import os
import re
import sys

import shipyard_utils as shipyard
from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger

from shipyard_azureblob import AzureBlobClient
from shipyard_azureblob.cli import exit_codes as ec

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container-name", dest="container_name", required=True)
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match"},
        required=False,
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--connection-string", dest="connection_string", default=None, required=True
    )
    return parser.parse_args()


def main():
    args = get_args()
    source_folder_name = shipyard.files.clean_folder_name(args.source_folder_name)

    client = AzureBlobClient(
        connection_string=args.connection_string, container_name=args.container_name
    )
    if args.source_file_name_match_type == "exact_match":
        source_full_path = shipyard.files.combine_folder_and_file_name(
            folder_name=source_folder_name, file_name=args.source_file_name
        )
        client.remove(file_name=source_full_path)

    elif args.source_file_name_match_type == "regex_match":
        file_names = client.find_blob_file_names(prefix=source_folder_name)
        matching_file_names = shipyard.find_all_file_matches(
            file_names, re.compile(args.source_file_name)
        )
        if number_of_matches := len(matching_file_names) == 0:
            logger.error("No file matches found")
            sys.exit(ec.EXIT_CODE_NO_MATCHES_FOUND)

        logger.info(f"{number_of_matches} files found. Preparing to delete...")
        for index, file_name in enumerate(matching_file_names, start=1):
            logger.info(f"Deleting file {index} of {number_of_matches}")
            client.remove(file_name=file_name)


if __name__ == "__main__":
    main()
