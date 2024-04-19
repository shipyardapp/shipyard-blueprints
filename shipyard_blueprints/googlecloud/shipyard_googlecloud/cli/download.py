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
    logger.info(
        f"Downloading {blob.bucket.name}/{blob.name} to {destination_file_name}"
    )
    local_path = os.path.join(os.getcwd(), destination_file_name)

    blob.download_to_filename(local_path)

    logger.info(
        f"{blob.bucket.name}/{blob.name} successfully downloaded to {local_path}"
    )


def main():
    tmp_file = None
    try:
        args = get_args()
        tmp_file = utils.set_environment_variables(args.gcp_application_credentials)
        gclient = utils.get_gclient(args.gcp_application_credentials)

        bucket_name = args.bucket_name
        source_file_name = args.source_file_name

        if args.source_folder_name:
            source_folder_name = shipyard.clean_folder_name(args.source_folder_name)
        else:
            source_folder_name = ""

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        if destination_folder_name:
            shipyard.create_folder_if_dne(destination_folder_name)

        bucket = utils.get_bucket(gclient=gclient, bucket_name=bucket_name)

        if args.source_file_name_match_type == "exact_match":
            source_full_path = shipyard.combine_folder_and_file_name(
                folder_name=source_folder_name, file_name=source_file_name
            )

            blob = utils.get_storage_blob(
                bucket=bucket,
                source_folder_name=source_folder_name,
                source_file_name=source_file_name,
            )
            destination_name = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )

            download_google_cloud_storage_file(
                blob=blob, destination_file_name=destination_name
            )
        elif args.source_file_name_match_type == "regex_match":
            blobs = list(bucket.list_blobs(prefix=source_folder_name))
            file_names = [blob.name for blob in blobs]
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )
            if not matching_file_names:
                raise FileNotFoundError(f"No files found matching {source_file_name}")

            logger.info(
                f"{len(matching_file_names)} files found. Preparing to download..."
            )

            for index, blob_name in enumerate(matching_file_names, start=1):
                destination_name = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=blob_name,
                    file_number=index,
                )

                logger.info(f"Downloading file {index} of {len(matching_file_names)}")
                download_google_cloud_storage_file(
                    blob=bucket.blob(blob_name), destination_file_name=destination_name
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
