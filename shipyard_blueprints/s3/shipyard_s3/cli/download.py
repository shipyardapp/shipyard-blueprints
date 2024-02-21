import os
import sys
import re
import argparse
import shipyard_bp_utils as shipyard
from shipyard_s3 import S3Client
from shipyard_templates import ExitCodeException, ShipyardLogger, CloudStorage
from typing import Optional

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


def get_dest_file(
    src_path: str, target_file: str, file_number: Optional[int] = None
) -> str:
    """
    Determines the destination file name based on provided parameters.

    Args:
    source_full_path (str): The full path of the source file.
    destination_file_name (str): The initial name of the destination file.
    file_number (int, optional): The number to append to the file name if necessary.

    Returns:
    str: The determined destination file name.
    """
    if not target_file:
        target_file = os.path.basename(src_path)
    if file_number:
        name, ext = os.path.splitext(target_file)
        target_file = f"{name}_{file_number}{ext}"
    return target_file


def main():
    try:
        args = get_args()
        bucket_name = args.bucket_name
        source_file = args.source_file_name
        source_folder = shipyard.files.clean_folder_name(args.source_folder_name)
        source_path = shipyard.files.combine_folder_and_file_name(
            folder_name=source_folder, file_name=source_file
        )
        match_type = args.source_file_name_match_type
        target_dir = shipyard.files.clean_folder_name(args.destination_folder_name)

        if not os.path.exists(target_dir) and (target_dir != ""):
            os.makedirs(target_dir)

        client = S3Client(
            aws_access_key=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region=args.aws_default_region,
        )

        s3_conn = client.connect()

        if match_type == "regex_match":
            file_names = client.list_files(
                s3_connection=s3_conn,
                bucket_name=bucket_name,
                s3_folder=source_folder,
            )
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(source_file)
            )
            if n_matches := len(matching_file_names) == 0:
                logger.error(
                    f"No files were found with the regex {source_file}, exitting now"
                )
                sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)

            logger.info(f" {n_matches} files found. Preparing to download...")

            for index, key_name in enumerate(matching_file_names, start=1):
                # NOTE: the `determine_destination_file_name` function in shipyard_bp_utils does not work as expected

                dest_name = get_dest_file(
                    src_path=key_name, target_file=args.destination_file_name
                )

                dest_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=target_dir,
                    destination_file_name=dest_name,
                    source_full_path=key_name,
                    file_number=index,
                )
                logger.info(f"Downloading file {index} of {len(matching_file_names)}")
                client.download(
                    s3_conn=s3_conn,
                    bucket_name=bucket_name,
                    s3_path=key_name,
                    dest_path=dest_path,
                )
                logger.info(f"Successfully downlaoded {key_name} to {dest_path}")
        else:
            dest_path = shipyard.files.determine_destination_full_path(
                destination_folder_name=target_dir,
                destination_file_name=args.destination_file_name,
                source_full_path=source_path,
            )
            client.download(
                s3_conn=s3_conn,
                bucket_name=bucket_name,
                s3_path=source_path,
                dest_path=dest_path,
            )
            logger.info(f"Successfully downloaded file to {dest_path}")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"Error in downloading from S3: {str(e)}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
