from pyairtable.api.table import Table
import os
import argparse
import pandas as pd
from requests.exceptions import HTTPError
import sys

EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_BASE = 201
EXIT_CODE_INVALID_TABLE = 202
EXIT_CODE_INVALID_VIEW = 203


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-id", dest="base_id", default="", required=True)
    parser.add_argument("--table-name", dest="table_name", default=None, required=True)
    parser.add_argument("--view-name", dest="view_name", default=None, required=False)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default=None,
        required=True,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument("--api-key", dest="api_key", default=None, required=True)
    parser.add_argument(
        "--include-record-id",
        dest="include_record_id",
        choices={"TRUE", "FALSE"},
        default="TRUE",
        required=False,
    )
    return parser.parse_args()


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip("/")
    if folder_name != "":
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}'
    )

    return combined_name


def convert_to_boolean(string):
    """
    Shipyard can't support passing Booleans to code, so we have to convert
    string values to their boolean values.
    """
    if string in ["True", "true", "TRUE"]:
        value = True
    else:
        value = False
    return value


def main():
    args = get_args()
    api_key = args.api_key
    table_name = args.table_name
    base_id = args.base_id
    view_name = args.view_name
    include_record_id = convert_to_boolean(args.include_record_id)
    destination_file_name = clean_folder_name(args.destination_file_name)
    destination_folder_name = clean_folder_name(args.destination_folder_name)

    destination_full_path = combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name
    )
    if not os.path.exists(destination_folder_name) and (destination_folder_name != ""):
        os.makedirs(destination_folder_name)

    try:
        table = Table(api_key, base_id, table_name)
        records = table.all(view=view_name)
    except HTTPError as httpe:
        print(httpe)
        if "Unauthorized for url:" in str(httpe):
            print(
                "The API key provided is incorrect. Please double-check for extra spaces or invalid characters."
            )

            sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
        elif "Error: NOT_FOUND" in str(httpe):
            print(
                "The Base ID provided is incorrect. Please double-check for extra spaces or invalid characters."
            )
            sys.exit(EXIT_CODE_INVALID_BASE)
        elif "TABLE_NOT_FOUND" in str(httpe):
            print(
                "The Table Name or ID provided is incorrect. Please double-check for extra spaces, invalid characters, or typos."
            )
            sys.exit(EXIT_CODE_INVALID_TABLE)
        elif "VIEW_NAME_NOT_FOUND" in str(httpe):
            print(
                "The View Name or ID provided is incorrect. Please double-check for extra spaces, invalid characters, or typos."
            )
            sys.exit(EXIT_CODE_INVALID_VIEW)
        else:
            sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    except BaseException as e:
        print(e)
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    df = pd.DataFrame.from_records(row["fields"] for row in records)

    if include_record_id:
        for index, row in enumerate(records):
            df.at[index, "airtable_record_id"] = row["id"]

    df.to_csv(destination_full_path, index=False)
    print(
        f"Successfully stored the contents of Base:{base_id} Table:{table_name} View:{view_name} as {destination_full_path}"
    )


if __name__ == "__main__":
    main()
