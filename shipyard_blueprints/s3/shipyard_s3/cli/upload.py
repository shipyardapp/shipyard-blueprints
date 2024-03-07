import os
import re
import argparse
import ast
import sys

import shipyard_bp_utils as shipyard
import shipyard_templates

from shipyard_s3.s3 import S3Client
from shipyard_templates import ShipyardLogger, ExitCodeException

from shipyard_s3.utils.exceptions import NoMatchesFound

logger = ShipyardLogger.get_logger()


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
    parser.add_argument("--s3-config", dest="s3_config", default=None, required=False)
    parser.add_argument("--aws-access-key-id", dest="aws_access_key_id", required=False)
    parser.add_argument(
        "--aws-secret-access-key", dest="aws_secret_access_key", required=False
    )
    parser.add_argument(
        "--aws-default-region", dest="aws_default_region", required=False
    )
    parser.add_argument("--extra-args", dest="extra_args", required=False)
    return parser.parse_args()


def main():
    try:
        args = get_args()
        bucket_name = args.bucket_name
        source_file = args.source_file_name
        source_dir = args.source_folder_name if args.source_folder_name else os.getcwd()
        dest_file = (
            args.destination_file_name if args.destination_file_name else source_file
        )
        source_path = os.path.join(source_dir, source_file)
        match_type = args.source_file_name_match_type
        s3_config = args.s3_config
        extra_args = ast.literal_eval(args.extra_args if args.extra_args else "{}")

        client = S3Client(
            aws_access_key=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region=args.aws_default_region,
        )
        client.connect()

        logger.info("Successfully connected to S3")

        if match_type == "regex_match":
            logger.info("Beginning to scan for file matches...")
            file_names = shipyard.files.find_all_local_file_names(source_dir)
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(source_file)
            )

            if (n_matches := len(matching_file_names)) == 0:
                raise NoMatchesFound(source_file)
            else:
                logger.info(f"{n_matches} files found. Preparing to upload...")

            for index, key_name in enumerate(matching_file_names, start=1):
                if args.destination_folder_name:
                    s3_folder = shipyard.files.clean_folder_name(
                        args.destination_folder_name
                    )
                    s3_path = shipyard.files.determine_destination_full_path(
                        destination_folder_name=s3_folder,
                        destination_file_name=args.destination_file_name,
                        source_full_path=key_name,
                        file_number=index,
                    )
                else:
                    s3_path = shipyard.files.determine_destination_file_name(
                        source_full_path=key_name,
                        destination_file_name=dest_file,
                        file_number=index,
                    )
                logger.info(f"Uploading file {index} of {len(matching_file_names)}")
                client.upload(
                    bucket_name=bucket_name,
                    source_file=key_name,
                    destination_path=s3_path,
                    extra_args=extra_args,
                )
                logger.info(
                    f"Successfully uploaded {key_name} to s3://{bucket_name}/{s3_path}"
                )
        else:
            if args.destination_folder_name:
                s3_folder = shipyard.files.clean_folder_name(
                    args.destination_folder_name
                )
                s3_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=s3_folder,
                    destination_file_name=dest_file,
                    source_full_path=source_path,
                )
            else:
                logger.debug("S3 folder not provided")
                s3_path = shipyard.files.determine_destination_file_name(
                    source_full_path=source_path, destination_file_name=dest_file
                )
                logger.debug(f"S3 path is {s3_path}")
            client.upload(
                bucket_name=bucket_name,
                source_file=source_path,
                destination_path=s3_path,
                extra_args=extra_args,
            )
            logger.info(
                f"Successfully loaded {source_path} to s3://{bucket_name}/{s3_path}"
            )
    except ExitCodeException as ue:
        logger.error(ue.message)
        sys.exit(ue.exit_code)
    except Exception as e:
        logger.error(
            f"An unhandled exception occurred when attempting to upload to S3: {str(e)}"
        )
        sys.exit(shipyard_templates.CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
