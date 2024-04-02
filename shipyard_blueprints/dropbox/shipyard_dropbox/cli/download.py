import argparse
import os
import re
import sys

from dropbox import Dropbox
from dropbox.files import FileMetadata, FolderMetadata
from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger

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
    parser.add_argument("--access-key", dest="access_key", default=None, required=True)
    return parser.parse_args()


def find_dropbox_file_names(client, prefix=None):
    """
    Fetched all the files in the bucket which are returned in a list as
    file names
    """
    result = []
    folders = []
    if prefix and not prefix.startswith("/"):
        prefix = f"/{prefix}"
    try:
        files = client.files_list_folder(prefix)
    except Exception:
        logger.error(f"Failed to search folder {prefix}")
        return []

    for f in files.entries:
        if isinstance(f, FileMetadata):
            result.append(f.path_lower)
        elif isinstance(f, FolderMetadata):
            folders.append(f.path_lower)
    for folder in folders:
        result.extend(find_dropbox_file_names(client, prefix=folder))
    return result


def download_dropbox_file(file_name, client, destination_file_name=None):
    """
    Download a selected file from Dropbox to local storage in
    the current working directory.
    """
    local_path = os.path.normpath(f"{os.getcwd()}/{destination_file_name}")

    try:
        with open(local_path, "wb") as f:
            metadata, _file = client.files_download(path=file_name)
            f.write(_file.content)
    except Exception as e:
        if "not_found" in str(e):
            print(f"Download failed. Could not find {file_name}")
        elif "not_file" in str(e):
            print(f"Download failed. {file_name} is not a file")
        else:
            print(f"Failed to download {file_name} to {local_path}")
        os.remove(local_path)
        raise (e)

    logger.info(f"{file_name} successfully downloaded to {local_path}")


def main():
    args = get_args()
    try:
        access_key = args.access_key
        source_file_name = args.source_file_name
        source_folder_name = shipyard.clean_folder_name(args.source_folder_name)
        source_full_path = shipyard.combine_folder_and_file_name(
            folder_name=source_folder_name, file_name=source_file_name
        )

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        if destination_folder_name:
            shipyard.create_folder_if_dne(destination_folder_name)

        client = Dropbox(access_key)
        if args.source_file_name_match_type == "exact_match":
            destination_name = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )

            download_dropbox_file(
                file_name=(
                    source_full_path
                    if source_full_path.startswith("/")
                    else f"/{source_full_path}"
                ),
                client=client,
                destination_file_name=destination_name,
            )
        elif args.source_file_name_match_type == "regex_match":
            file_names = find_dropbox_file_names(
                client=client, prefix=source_folder_name
            )
            matching_file_names = shipyard.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )

            if number_of_matching_files := len(matching_file_names) == 0:
                raise ValueError(f"No files found matching {source_file_name}")
            logger.info(
                f"{number_of_matching_files} files found. Preparing to download..."
            )

            for index, file_name in enumerate(matching_file_names, start=1):
                destination_name = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=file_name,
                    file_number=index,
                )

                logger.info(f"Downloading file {index} of {number_of_matching_files}")
                download_dropbox_file(
                    file_name=(
                        file_name if file_name.startswith("/") else f"/{file_name}"
                    ),
                    client=client,
                    destination_file_name=destination_name,
                )
    except Exception as e:
        logger.error(
            f"Failed to download {args.source_file_name} from Dropbox due to {e}"
        )
        sys.exit(1)
    else:
        logger.info("Download completed successfully")


if __name__ == "__main__":
    main()
