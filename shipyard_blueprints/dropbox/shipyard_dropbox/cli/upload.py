import argparse
import os
import re
import sys

from dropbox import Dropbox
from dropbox.exceptions import *
from dropbox.files import UploadSessionCursor, CommitInfo
from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

logger = ShipyardLogger.get_logger()

CHUNK_SIZE = 10 * 1024 * 1024


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
    parser.add_argument("--access-key", dest="access_key", default=None, required=True)
    return parser.parse_args()


def upload_dropbox_file(client, source_full_path, destination_full_path):
    """
    Uploads a single file to Dropbox.
    """
    if os.path.getsize(source_full_path) <= CHUNK_SIZE:
        upload_small_dropbox_file(
            client=client,
            source_full_path=source_full_path,
            destination_full_path=destination_full_path,
        )
    else:
        upload_large_dropbox_file(
            client=client,
            source_full_path=source_full_path,
            destination_full_path=destination_full_path,
        )


def upload_small_dropbox_file(client, source_full_path, destination_full_path):
    """
    Uploads a small (<=CHUNK_SIZE) single file to Dropbox.
    """
    with open(source_full_path, "rb") as f:
        try:
            client.files_upload(f.read(CHUNK_SIZE), destination_full_path)
            logger.info(
                f"{source_full_path} successfully uploaded to "
                f"{destination_full_path}"
            )

        except ApiError as e:
            logger.error(f"Failed to upload file {source_full_path} due to {e}")


def upload_large_dropbox_file(client, source_full_path, destination_full_path):
    """
    Uploads a large (>CHUNK_SIZE) single file to Dropbox.
    """
    file_size = os.path.getsize(source_full_path)
    with open(source_full_path, "rb") as f:
        try:
            upload_session_start_result = client.files_upload_session_start(
                f.read(CHUNK_SIZE)
            )
            session_id = upload_session_start_result.session_id
            cursor = UploadSessionCursor(session_id=session_id, offset=f.tell())
            commit = CommitInfo(path=destination_full_path)

            while f.tell() < file_size:
                if (file_size - f.tell()) <= CHUNK_SIZE:
                    logger.info(
                        client.files_upload_session_finish(
                            f.read(CHUNK_SIZE), cursor, commit
                        )
                    )
                else:
                    client.files_upload_session_append(
                        f.read(CHUNK_SIZE), cursor.session_id, cursor.offset
                    )
                    cursor.offset = f.tell()
        except ApiError as e:
            logger.error(f"Failed to upload file {source_full_path} due to {e}")

    logger.info(f"{source_full_path} successfully uploaded to {destination_full_path}")


def get_dropbox_client(access_key):
    """
    Attempts to create the Dropbox Client with the associated
    """
    try:
        client = Dropbox(access_key)
        client.users_get_current_account()
        return client
    except AuthError as e:
        raise ExitCodeException(
            f"Failed to authenticate using key {access_key}",
            CloudStorage.EXIT_CODE_INVALID_CREDENTIALS,
        ) from e


def main():
    try:
        args = get_args()
        access_key = args.access_key
        source_file_name = args.source_file_name
        source_folder_name = args.source_folder_name
        source_full_path = shipyard.combine_folder_and_file_name(
            folder_name=os.path.join(os.getcwd(), source_folder_name),
            file_name=source_file_name,
        )
        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )

        client = get_dropbox_client(access_key=access_key)
        if args.source_file_name_match_type == "exact_match":
            destination_full_path = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )
            upload_dropbox_file(
                source_full_path=source_full_path,
                destination_full_path=destination_full_path,
                client=client,
            )

        elif args.source_file_name_match_type == "regex_match":
            file_names = shipyard.find_all_local_file_names(source_folder_name)
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )
            if not matching_file_names:
                raise ExitCodeException(
                    f"No files found matching {source_file_name}",
                    CloudStorage.EXIT_CODE_FILE_NOT_FOUND,
                )
            logger.info(
                f"{len(matching_file_names)} files found. Preparing to upload..."
            )

            for index, key_name in enumerate(matching_file_names, start=1):
                destination_full_path = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=key_name,
                    file_number=index,
                )
                if not os.path.exists(key_name):
                    raise ExitCodeException(
                        f"File {key_name} does not exist",
                        CloudStorage.EXIT_CODE_FILE_NOT_FOUND,
                    )
                logger.info(f"Uploading file {index} of {len(matching_file_names)}")
                upload_dropbox_file(
                    source_full_path=key_name,
                    destination_full_path=destination_full_path,
                    client=client,
                )
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"Failed to upload to Dropbox due to {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)
    else:
        logger.info("All files uploaded successfully")


if __name__ == "__main__":
    main()
