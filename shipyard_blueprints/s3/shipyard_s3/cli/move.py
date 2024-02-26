import re
import argparse
import sys
import shipyard_bp_utils as shipyard
from shipyard_s3 import S3Client
from shipyard_templates import CloudStorage, ShipyardLogger, ExitCodeException

from shipyard_s3.utils.exceptions import NoMatchesFound

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
        "--source-bucket-name", dest="source_bucket_name", default="", required=True
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
        "--destination-bucket-name",
        dest="destination_bucket_name",
        default="",
        required=True,
    )
    parser.add_argument("--aws-access-key-id", dest="aws_access_key_id", required=False)
    parser.add_argument(
        "--aws-secret-access-key", dest="aws_secret_access_key", required=False
    )
    parser.add_argument(
        "--aws-default-region", dest="aws_default_region", required=False
    )

    return parser.parse_args()


def main():
    try:
        args = get_args()
        src_file = args.source_file_name
        src_folder = (
            shipyard.files.clean_folder_name(args.source_folder_name)
            if args.source_folder_name
            else None
        )
        dest_folder = shipyard.files.clean_folder_name(args.destination_folder_name)
        match_type = args.source_file_name_match_type
        src_bucket = args.source_bucket_name
        dest_bucket = args.destination_bucket_name

        client = S3Client(
            aws_access_key=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region=args.aws_default_region,
        )

        logger.info("Successfully connected to S3")

        if match_type == "regex_match":
            logger.info("Beginning to scan for file matches...")
            file_names = client.list_files(bucket_name=src_bucket, s3_folder=src_folder)
            logger.debug(f"All file names: {file_names}")
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(src_file)
            )
            logger.debug(f"matching file names: {matching_file_names}")
            if (n_matches := len(matching_file_names)) == 0:
                raise NoMatchesFound(src_file)

            logger.info(f" {n_matches} files found. Preparing to move...")

            for index, key_name in enumerate(matching_file_names, start=1):
                dest_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=dest_folder,
                    destination_file_name=args.destination_file_name,
                    source_full_path=key_name,
                    file_number=index if args.destination_file_name else None,
                )
                logger.info(f"Moving file {index} of {n_matches}")
                client.move(
                    src_bucket=src_bucket,
                    src_path=key_name,
                    dest_bucket=dest_bucket,
                    dest_path=dest_path,
                )

        else:
            if src_folder:
                src_path = shipyard.files.combine_folder_and_file_name(
                    src_folder, src_file
                )
            else:
                src_path = src_file

            dest_file = shipyard.files.determine_destination_file_name(
                source_full_path=src_path,
                destination_file_name=args.destination_file_name,
            )
            if args.destination_folder_name:
                dest_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=dest_folder,
                    destination_file_name=dest_file,
                    source_full_path=src_path,
                )
            else:
                dest_path = dest_file

            client.move(
                src_bucket=src_bucket,
                src_path=src_path,
                dest_bucket=dest_bucket,
                dest_path=dest_path,
            )
            logger.info(
                f"Successfully moved s3://{src_bucket}/{src_path} to s3://{dest_bucket}/{dest_path} "
            )
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"An unhandled error occurred when attempting to move file: {str(e)}"
        )
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
