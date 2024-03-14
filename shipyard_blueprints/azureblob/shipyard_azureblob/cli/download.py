import argparse
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
    parser.add_argument(
        "--connection-string", dest="connection_string", default=None, required=True
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        container_name = args.container_name
        source_folder_name = shipyard.clean_folder_name(args.source_folder_name)

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        if destination_folder_name:
            shipyard.create_folder_if_dne(destination_folder_name)
        client = AzureBlobClient(
            connection_string=args.connection_string, container_name=container_name
        )

        if args.source_file_name_match_type == "exact_match":
            source_full_path = shipyard.combine_folder_and_file_name(
                source_folder_name, args.source_file_name
            )

            destination_name = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )

            client.download(
                file_name=source_full_path,
                destination_file_name=destination_name,
            )

        elif args.source_file_name_match_type == "regex_match":
            file_names = client.find_blob_file_names(prefix=source_folder_name)
            matching_file_names = shipyard.find_all_file_matches(
                file_names, args.source_file_name
            )

            if number_of_matches := len(matching_file_names) == 0:
                raise exceptions.NoFilesFoundError(
                    f"No files found matching the regex {args.source_file_name}"
                )

            logger.info(f"{number_of_matches} files found. Preparing to download...")

            for index, file_name in enumerate(matching_file_names, start=1):
                destination_name = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=file_name,
                    file_number=index if number_of_matches > 1 else None,
                )

                logger.info(f"Downloading file {index} of {number_of_matches}")
                client.download(
                    file_name=file_name,
                    destination_file_name=destination_name,
                )
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(exceptions.UnknownException(e).message)
        sys.exit(exceptions.UnknownException(e).exit_code)


if __name__ == "__main__":
    main()
