import argparse
import requests
import os
import re
import sys
import shipyard_utils as shipyard
from shipyard_databricks.cli import databricks_client, errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--instance-url", dest="instance_url", required=True)
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name",
        dest="source_folder_name",
        default="/FileStore/",
        required=False,
    )
    parser.add_argument("--dest-file-name", dest="dest_file_name", required=False)
    parser.add_argument(
        "--dest-folder-name",
        dest="dest_folder_name",
        default="/FileStore/",
        required=False,
    )
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        choices={"exact_match", "regex_match"},
        required=True,
    )
    args = parser.parse_args()
    return args


def dbfs_mkdirs(client, directory_path):
    """Create the given directory and necessary parent directories if they do not exist"""
    mkdir_endpoint = "/dbfs/mkdirs"
    payload = {"path": os.path.dirname(directory_path)}
    mkdir_response = client.post(mkdir_endpoint, data=payload)
    if mkdir_response.status_code == requests.codes.ok:
        print(f"Successfully made directories for path: {directory_path}")
    else:
        print(f"Failed to make directory path: {mkdir_response.text}")


def dbfs_move_file(client, source_file_path, destination_file_path):
    move_endpoint = "/dbfs/move"
    payload = {
        "source_path": source_file_path,
        "destination_path": destination_file_path,
    }

    try:
        move_response = client.post(move_endpoint, data=payload)
    except Exception as e:
        # Check if incorrect url provided first
        if "nodename nor servname provided" in str(e):
            print(
                "Invalid or wrong databricks instance url provided. Please check and try again"
            )
            sys.exit(errors.EXIT_CODE_INVALID_INSTANCE)
        else:
            print(f"Error occurred when trying move request: {e}")
            sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)
    if move_response.status_code == requests.codes.ok:
        print(f"File: {source_file_path} moved successfully to {destination_file_path}")
    elif move_response.status_code == 404:
        print(f"File: {source_file_path} does not exist")
        sys.exit(errors.EXIT_CODE_DBFS_INVALID_SOURCE)
    elif move_response.status_code == 500:
        error_message = move_response.json()["message"]
        print(f"Error trying to move to {destination_file_path} : {error_message}")
        sys.exit(errors.EXIT_CODE_DBFS_MOVE_ERROR)
    else:
        print(
            f"DBFS {source_file_path} to {destination_file_path} failed.",
            f"response: {move_response.text} status: {move_response.status_code}",
        )
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    access_token = args.access_token
    instance_url = args.instance_url
    source_file_name = args.source_file_name
    if args.source_folder_name == "":
        source_folder_name = "/FileStore/"
    else:
        source_folder_name = args.source_folder_name
    dest_file_name = args.dest_file_name
    if args.dest_folder_name == "":
        dest_folder_name = "/FileStore/"
    else:
        dest_folder_name = args.dest_folder_name
    source_file_name_match_type = args.source_file_name_match_type
    # create client
    client = databricks_client.DatabricksClient(access_token, instance_url)

    # clean folder name
    source_folder_name = (
        "/" + shipyard.files.clean_folder_name(source_folder_name) + "/"
    )

    # clean folder name
    dest_folder_name = "/" + shipyard.files.clean_folder_name(dest_folder_name) + "/"

    if not dest_file_name:
        dest_file_name = source_file_name

    if source_file_name_match_type == "regex_match":
        files = databricks_client.list_dbfs_files(client, source_folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(
            files, re.compile(source_file_name)
        )
        num_matches = len(matching_file_names)
        if num_matches == 0:
            print("No files matching regex found")
            sys.exit(errors.EXIT_CODE_DBFS_MOVE_ERROR)
        print(f"{num_matches} files found. Preparing to move...")
        # create folder directory path
        dbfs_mkdirs(client, dest_folder_name)
        # create move file path
        for index, file_name in enumerate(matching_file_names):
            file_number = None if num_matches == 1 else index + 1
            source_file_path = shipyard.files.combine_folder_and_file_name(
                folder_name="", file_name=file_name
            )
            dest_file_path = shipyard.files.determine_destination_full_path(
                destination_folder_name=dest_folder_name,
                destination_file_name=dest_file_name,
                source_full_path=source_file_path,
                file_number=file_number,
            )
            dbfs_move_file(client, source_file_path, dest_file_path)
    else:
        source_file_path = shipyard.files.combine_folder_and_file_name(
            source_folder_name, source_file_name
        )
        destination_file_path = shipyard.files.combine_folder_and_file_name(
            dest_folder_name, dest_file_name
        )
        dbfs_mkdirs(client, destination_file_path)
        dbfs_move_file(client, source_file_path, destination_file_path)


if __name__ == "__main__":
    main()
