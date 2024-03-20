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
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--service-account",
        dest="gcp_application_credentials",
        default=None,
        required=True,
    )
    return parser.parse_args()


def delete_google_cloud_storage_file(blob):
    """
    Deletes a selected file from Google cloud storage
    """
    blob_bucket, blob_name = blob.bucket.name, blob.name
    blob.delete()
    logger.info(f"Blob {blob_bucket}/{blob_name} delete ran successfully")


def main():
    tmp_file = None
    try:
        args = get_args()
        tmp_file = utils.set_environment_variables(args.gcp_application_credentials)
        bucket_name = args.bucket_name
        source_file_name = args.source_file_name
        if args.source_folder_name:
            source_folder_name = args.source_folder_name
        else:
            source_folder_name = shipyard.clean_folder_name(args.source_folder_name)

        gclient = utils.get_gclient(args.gcp_application_credentials)
        bucket = utils.get_bucket(gclient=gclient, bucket_name=bucket_name)

        if args.source_file_name_match_type == "exact_match":
            blob = utils.get_storage_blob(
                bucket=bucket,
                source_folder_name=source_folder_name,
                source_file_name=source_file_name,
            )
            delete_google_cloud_storage_file(blob=blob)

        elif args.source_file_name_match_type == "regex_match":
            blobs = list(bucket.list_blobs(prefix=source_folder_name))
            file_names = [blob.name for blob in blobs]
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )
            if not matching_file_names:
                raise FileNotFoundError(f"No files found matching {source_file_name}")
            number_of_matches = len(matching_file_names)
            logger.info(f"{number_of_matches} files found. Preparing to delete...")

            for index, blob in enumerate(matching_file_names, start=1):
                logger.info(f"deleting file {index} of {number_of_matches}")
                delete_google_cloud_storage_file(blob=bucket.blob(blob))
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except FileNotFoundError as e:
        logger.error(e)
        sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        logger.error(e)
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)
    finally:
        if tmp_file:
            logger.info(f"Removing temporary credentials file {tmp_file}")
            os.remove(tmp_file)


if __name__ == "__main__":
    main()
