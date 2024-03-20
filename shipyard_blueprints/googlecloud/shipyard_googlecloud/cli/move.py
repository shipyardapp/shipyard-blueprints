import argparse
import os
import re
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

from shipyard_googlecloud import utils

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-bucket-name", dest="source_bucket_name", required=True
    )
    parser.add_argument(
        "--destination-bucket-name", dest="destination_bucket_name", required=True
    )
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
        "--service-account",
        dest="gcp_application_credentials",
        default=None,
        required=True,
    )
    return parser.parse_args()


def download_google_cloud_storage_file(blob, destination_file_name=None):
    """
    Download a selected file from Google Cloud Storage to local storage in
    the current working directory.
    """
    local_path = os.path.join(os.getcwd(), destination_file_name)

    blob.download_to_filename(local_path)

    logger.info(
        f"{blob.bucket.name}/{blob.name} successfully downloaded to {local_path}"
    )


def move_google_cloud_storage_file(
    source_bucket, source_blob_path, destination_bucket, destination_blob_path
):
    """
    Moves blobs between directories or buckets. First copies the
    blob to destination then deletes the source blob from the old location.
    """
    # get source blob
    source_blob = source_bucket.blob(source_blob_path)

    # copy to destination
    dest_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_path
    )
    # delete in old destination
    source_blob.delete()

    logger.info(f"File moved from {source_blob} to {dest_blob}")


def main():
    tmp_file = None
    try:
        args = get_args()
        source_bucket_name = args.source_bucket_name
        destination_bucket_name = args.destination_bucket_name
        source_file_name = args.source_file_name
        source_folder_name = shipyard.clean_folder_name(args.source_folder_name)
        source_file_name_match_type = args.source_file_name_match_type

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        destination_file_name = args.destination_file_name

        tmp_file = utils.set_environment_variables(args.gcp_application_credentials)
        gclient = utils.get_gclient(args.gcp_application_credentials)

        source_bucket = utils.get_bucket(
            gclient=gclient, bucket_name=source_bucket_name
        )
        destination_bucket = utils.get_bucket(
            gclient=gclient, bucket_name=destination_bucket_name
        )

        if args.source_file_name_match_type == "exact_match":
            blob = utils.get_storage_blob(
                bucket=source_bucket,
                source_folder_name=source_folder_name,
                source_file_name=source_file_name,
            )
            dest_file = shipyard.determine_destination_file_name(
                source_full_path=blob.name, destination_file_name=destination_file_name
            )
            destination_full_path = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=dest_file,
                source_full_path=blob,
            )
            move_google_cloud_storage_file(
                source_bucket=source_bucket,
                source_blob_path=blob.name,
                destination_bucket=destination_bucket,
                destination_blob_path=destination_full_path,
            )
        elif source_file_name_match_type == "regex_match":
            blobs = list(source_bucket.list_blobs(prefix=source_folder_name))
            file_names = list(map(lambda x: x.name, blobs))
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )

            if not matching_file_names:
                raise FileNotFoundError(f"No files found matching {source_file_name}")
            number_of_matches = len(matching_file_names)
            logger.info(f"{number_of_matches} files found. Preparing to move...")

            for index, blob in enumerate(matching_file_names, 1):
                destination_full_path = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=destination_file_name,
                    source_full_path=blob,
                    file_number=None if number_of_matches == 1 else index,
                )
                logger.info(f"moving file {index} of {number_of_matches}")
                move_google_cloud_storage_file(
                    source_bucket=source_bucket,
                    source_blob_path=blob,
                    destination_bucket=destination_bucket,
                    destination_blob_path=destination_full_path,
                )
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
