import argparse
import os
import re
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException

from shipyard_azureblob import AzureBlobClient, exceptions

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
        "--connection-string", dest="connection_string", default=None, required=True
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        source_folder_name = args.source_folder_name

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        client = AzureBlobClient(args.connection_string, args.container_name)

        if args.source_file_name_match_type == "exact_match":
            source_full_path = shipyard.combine_folder_and_file_name(
                folder_name=f"{os.getcwd()}/{source_folder_name}",
                file_name=args.source_file_name,
            )
            destination_full_path = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )
            client.upload(
                source_full_path=source_full_path,
                destination_full_path=destination_full_path,
            )
        elif args.source_file_name_match_type == "regex_match":
            file_names = shipyard.find_all_local_file_names(source_folder_name)
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(args.source_file_name)
            )

            if number_of_matches := len(matching_file_names) == 0:
                raise exceptions.NoFilesFoundError(
                    f"No file matches found for "
                    f"{args.source_file_name} in {source_folder_name}"
                )

            logger.info(f"{number_of_matches} files found. Preparing to upload...")

            for index, key_name in enumerate(matching_file_names, start=1):
                destination_full_path = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=key_name,
                    file_number=index if number_of_matches > 1 else None,
                )
                logger.info(f"Uploading file {index} of {number_of_matches}")
                client.upload(
                    source_full_path=key_name,
                    destination_full_path=destination_full_path,
                )

    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(exceptions.UnknownException(e).message)
        sys.exit(exceptions.UnknownException(e).exit_code)


if __name__ == "__main__":
    main()
