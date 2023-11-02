import argparse
import os
import smartsheet
import sys
import logging
import requests
import pandas as pd
from shipyard_templates import ExitCodeException, Spreadsheets as ss
from typing import Dict, List, Any

# custom exit code
EXIT_CODE_INVALID_SHEET_ID = 220

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--sheet-id", dest="sheet_id", required=True)
    parser.add_argument("--destination-file-name", dest="file_name", required=True)
    parser.add_argument(
        "--destination-folder-name", dest="folder_name", required=False, default=""
    )
    parser.add_argument(
        "--file-type",
        dest="file_type",
        required=False,
        default="csv",
        choices={"xlsx", "csv"},
    )
    return parser.parse_args()


def connect(logger: logging.Logger, smartsheet: smartsheet.Smartsheet):
    try:
        test = smartsheet.Users.get_current_user()
    except Exception as e:
        logger.error("Error in connecting to Smartsheet")
        logger.error(str(e))
        return 1
    else:
        return 0


def get_logger():
    logger = logging.getLogger("Shipyard")
    logger.setLevel(logging.DEBUG)
    # Add handler for stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # add specific format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
    )
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger

def is_valid_sheet(smart:smartsheet.Smartsheet, sheet_id:str) -> bool:
    try:
        response = smart.Sheets.get_sheet(sheet_id, page_size = 1)
        if isinstance(response, smart.models.error.Error):
            return False
    except Exception as e:
        return False
    else:
        return True

def flatten_json(json_data: Dict[Any, Any]) -> Dict[str, List[Any]]:
    try:
        row_count = json_data["totalRowCount"]
        columns = json_data["columns"]  # this will be a list of all the columns
        # set up a quick lookup table for for column id and column name
        lookup = {c["id"]: c["title"] for c in columns}

        ret_dict = {}
        # setup the keys for return dictionary and initialize and empty list as the value
        for k in lookup.values():
            ret_dict[k] = []

        # get the row json
        rows = json_data["rows"]

        # iterate through each row and grab the cell
        for row in rows:
            cells = row["cells"]
            for cell in cells:
                col_id = cell["columnId"]
                value = cell["value"]
                col_name = lookup.get(col_id)
                # update the return dictionary
                ret_dict.get(col_name).append(value)
    except Exception as e:
        raise ExitCodeException(
            message=f"Error in parsing json data: {str(e)}",
            exit_code=ss.EXIT_CODE_DOWNLOAD_ERROR,
        )

    else:
        return ret_dict


def main():
    args = get_args()
    logger = get_logger()
    try:
        # establish client and check if access token is valid
        smart = smartsheet.Smartsheet(args.access_token)
        if connect(logger, smart) == 1:
            sys.exit(ss.EXIT_CODE_INVALID_TOKEN)

        token = args.access_token
        sheet_id = args.sheet_id

        if not is_valid_sheet(smart, sheet_id):
            logger.error("Error: sheet ID provided is not valid")
            sys.exit(EXIT_CODE_INVALID_SHEET_ID)


        if args.folder_name != "":
            file_path = os.path.join(args.folder_name, args.file_name)
        else:
            file_path = args.file_name

        url = url = f"https://api.smartsheet.com/2.0/sheets/{sheet_id}"
        headers = {"Authorization": f"Bearer {token}", "Accpet": "text/csv"}

        response = requests.get(url, headers=headers)

        # check the status code of the response
        if response.status_code == 200:
            logger.info("Fetched data from Smartsheet, parsing now...")
            flat = flatten_json(response.json())
            df = pd.DataFrame(flat)
            if args.file_type == "csv":
                df.to_csv(file_path, index=False)
            else:
                df.to_excel(file_path, index=False)
            logger.info(f"Successfully downloaded sheet to {file_path}")
        else:
            logger.error(
                f"Error in downloading sheet. Response from API is {response.text}"
            )
            sys.exit(ss.EXIT_CODE_BAD_REQUEST)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error("Error in downloading file")
        logger.exception(str(e))
        sys.exit(ss.EXIT_CODE_DOWNLOAD_ERROR)


if __name__ == "__main__":
    main()
