import sys
import re
import argparse
import shipyard_bp_utils as shipyard
from shipyard_s3 import S3Client
from shipyard_templates import ExitCodeException, ShipyardLogger, CloudStorage

from shipyard_s3.utils.exceptions import NoMatchesFound

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket-name", dest="bucket_name", required=True)
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
        default="",
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
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
    return parser.parse_args()


def main():
    try:
        args = get_args()
        bucket_name = args.bucket_name
        src_file = args.source_file_name
        src_folder = (
            shipyard.files.clean_folder_name(args.source_folder_name)
            if args.source_folder_name
            else None
        )
        match_type = args.source_file_name_match_type
        target_dir = shipyard.files.clean_folder_name(args.destination_folder_name)

        shipyard.files.create_folder_if_dne(target_dir)

        client = S3Client(
            aws_access_key=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region=args.aws_default_region,
        )
        client.connect()

        logger.info("Successfully connected to S3")

        if match_type == "regex_match":
            logger.info("Beginning to scan for file matches...")
            file_names = client.list_files(
                bucket_name=bucket_name,
                s3_folder=src_folder,
            )
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(src_file)
            )
            if (n_matches := len(matching_file_names)) == 0:
                raise NoMatchesFound(src_file)

            logger.info(f" {n_matches} files found. Preparing to download...")

            for index, key_name in enumerate(matching_file_names, start=1):
                dest_name = shipyard.files.determine_destination_file_name(
                    source_full_path=key_name,
                    destination_file_name=args.destination_file_name,
                )

                dest_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=target_dir,
                    destination_file_name=dest_name,
                    source_full_path=key_name,
                    file_number=index if args.destination_file_name else None,
                )
                logger.info(f"Downloading file {index} of {len(matching_file_names)}")
                client.download(
                    bucket_name=bucket_name,
                    s3_path=key_name,
                    dest_path=dest_path,
                )
                logger.info(
                    f"Successfully downloaded s3://{bucket_name}/{key_name} to {dest_path}"
                )
        else:
            if src_folder:
                src_path = shipyard.files.combine_folder_and_file_name(
                    folder_name=src_folder, file_name=src_file
                )
            else:
                src_path = src_file

            dest_path = shipyard.files.determine_destination_full_path(
                destination_folder_name=target_dir,
                destination_file_name=args.destination_file_name,
                source_full_path=src_path,
            )
            client.download(
                bucket_name=bucket_name,
                s3_path=src_path,
                dest_path=dest_path,
            )
            logger.info(f"Successfully downloaded s3://{src_path} to {dest_path}")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"Error in downloading from S3: {str(e)}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
