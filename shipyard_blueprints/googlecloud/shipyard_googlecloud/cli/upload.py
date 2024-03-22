import argparse
import os
import re
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, CloudStorage, ExitCodeException

from shipyard_googlecloud import utils

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket-name", dest="bucket_name", required=True)
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
        "--service-account",
        dest="gcp_application_credentials",
        default=None,
        required=True,
    )
    return parser.parse_args()


def main():
    tmp_file = None
    try:
        args = get_args()
        tmp_file = utils.set_environment_variables(args.gcp_application_credentials)
        bucket_name = args.bucket_name
        source_file_name = args.source_file_name
        source_full_path = shipyard.combine_folder_and_file_name(
            folder_name=os.path.join(os.getcwd(), args.source_folder_name),
            file_name=source_file_name,
        )

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )

        gclient = utils.get_gclient(args.gcp_application_credentials)
        bucket = utils.get_bucket(gclient=gclient, bucket_name=bucket_name)
        if args.source_file_name_match_type == "exact_match":
            if not os.path.exists(source_full_path):
                raise FileNotFoundError(f"File {source_full_path} does not exist")

            destination_full_path = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )
            utils.upload_file(
                source_full_path=source_full_path,
                destination_full_path=destination_full_path,
                bucket=bucket,
            )
        elif args.source_file_name_match_type == "regex_match":
            file_names = shipyard.find_all_local_file_names(args.source_folder_name)
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )

            if number_of_matches := len(matching_file_names) == 0:
                raise FileNotFoundError(f"No files found matching {source_file_name}")

            logger.info(f"{number_of_matches} files found. Preparing to upload...")

            for index, key_name in enumerate(matching_file_names, start=1):
                destination_full_path = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=key_name,
                    file_number=index,
                )
                logger.info(f"Uploading file {index} of {number_of_matches}")
                utils.upload_file(
                    source_full_path=key_name,
                    destination_full_path=destination_full_path,
                    bucket=bucket,
                )
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except FileNotFoundError as e:
        logger.error(e)
        sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        logger.error(f"An unknown error occurred\n{e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)
    finally:
        if tmp_file:
            logger.info(f"Removing temporary credentials file {tmp_file}")
            os.remove(tmp_file)


if __name__ == "__main__":
    main()
