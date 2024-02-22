import os
import boto3
import botocore
from botocore.client import Config
import re
import argparse
import glob
from ast import literal_eval
import sys
import shipyard_bp_utils as shipyard
from shipyard_s3.cli import exit_codes as ec
from shipyard_s3 import S3Client
from shipyard_templates import CloudStorage, ExitCodeException, ShipyardLogger

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
    args = get_args()
    bucket_name = args.bucket_name
    src_file = args.source_file_name
    src_folder = args.source_folder_name if args.source_folder_name else None
    source_file_name_match_type = args.source_file_name_match_type
    client = S3Client(
        aws_access_key=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        region=args.aws_default_region,
    )

    s3_conn = client.connect()
    logger.info("Successfully connected to S3")

    if source_file_name_match_type == "regex_match":
        logger.info("Beginning to scan for file matches...")
        file_names = client.list_files(
            s3_connection=s3_conn, bucket_name=bucket_name, s3_folder=src_folder
        )
        matching_file_names = shipyard.files.find_all_file_matches(
            file_names, re.compile(src_file)
        )
        if n_matches := len(matching_file_names) == 0:
            raise NoMatchesFound(src_file)

        logger.info(f"{n_matches} files found. Preparing to remove...")

        for index, key_name in enumerate(matching_file_names, start=1):
            client.remove(s3_conn=s3_conn, bucket_name=bucket_name, src_path=key_name)
            logger.info(f"Removing file {index} of {n_matches}")

    else:
        if src_folder:
            src_path = shipyard.files.combine_folder_and_file_name(src_folder, src_file)
        else:
            src_path = src_file
        client.remove(s3_conn=s3_conn, bucket_name=bucket_name, src_path=src_path)


if __name__ == "__main__":
    main()
