import os
import re
import sys
import time
import argparse

from shipyard_hubspot import HubspotClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)

    parser.add_argument("--import-name", dest="import_name", required=True)
    parser.add_argument("--object-type", dest="object_type", required=True)
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_match_type",
        choices={"exact_match", "regex_match"},
        required=True,
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument("--import-operation", dest="import_operation", required=True)
    parser.add_argument("--file-format", dest="file_format", required=True)
    return parser.parse_args()


def find_files_matching_pattern(start_path=".", pattern=".*", exact_match=False):
    """Find files in the given directory and its subdirectories that match the given regex pattern.

    Args:
        start_path (str): The starting path for the directory search. Defaults to the current directory.
        pattern (str): The regex pattern to match filenames against.
        exact_match (bool): Whether to match the pattern exactly or just contain it.

    Returns:
        list: A list of filenames (with their full paths) that match the given regex pattern.
    """
    matching_files = []
    valid_extensions = {".csv", ".xlsx", ".xls", ".txt"}
    print(
        f"Looking for files matching {pattern} in {start_path} with a file format of {valid_extensions}..."
    )
    for dirpath, _, filenames in os.walk(start_path):
        for filename in filenames:
            if exact_match:
                is_match = re.fullmatch(pattern, filename)
            else:  # Check if the filename contains the pattern
                is_match = re.search(pattern, filename)

            # If there's a match, add the full path to the list
            if is_match and os.path.splitext(filename)[1] in valid_extensions:
                full_path = os.path.join(dirpath, filename)
                matching_files.append(full_path)

    return matching_files


def import_file(
    client, import_name, filename, import_operations, object_type, file_format
):
    import_job_id = client.import_contact_data(
        import_name=import_name,
        filename=filename,
        object_type=object_type,
        import_operations=import_operations,
        file_format=file_format,
    ).get("id")

    while client.get_import_status(import_job_id).get("state") not in {
        "FAILED",
        "CANCELED",
        "DONE",
        None,
    }:
        client.logger.info("Waiting for import to complete...")
        time.sleep(30)


def main():
    args = get_args()
    hubspot = HubspotClient(access_token=args.access_token)
    try:
        files = find_files_matching_pattern(
            start_path=args.source_folder_name,
            pattern=args.source_file_name,
            exact_match=args.source_match_type == "exact_match",
        )

        for file in files:
            hubspot.logger.info(
                f"Attempting to import {file} with import name {args.import_name}"
            )
            import_file(
                client=hubspot,
                import_name=args.import_name,
                filename=file,
                object_type=args.object_type,
                import_operations=args.import_operation,
                file_format=args.file_format,
            )

    except ExitCodeException as e:
        hubspot.logger.error(e)
        sys.exit(e.exit_code)


if __name__ == "__main__":
    main()
