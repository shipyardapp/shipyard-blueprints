import argparse
import os
import smartsheet
import sys
import logging
import pandas as pd
from shipyard_templates import ExitCodeException, Spreadsheets as ss
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class DataRow:
    index:int
    values:List[smartsheet.Smartsheet.models.Row]



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


def connect(logger: logging.Logger, smartsheet: smartsheet.Smartsheet):
    try:
        test = smartsheet.Users.get_current_user()
    except Exception as e:
        logger.error("Error in connecting to Smartsheet")
        logger.error(str(e))
        return 1
    else:
        return 0


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--sheet-name", dest="sheet_name", required=False, default="")
    parser.add_argument("--source-file-name", dest="file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="folder_name", required=False, default=""
    )
    parser.add_argument(
        "--file-type",
        dest="file_type",
        required=False,
        default="csv",
        choices={"xlsx", "csv"},
    )
    parser.add_argument('--insert-method', dest = 'insert_method', required = False, default = 'append', choices = {'replace','append'})
    return parser.parse_args()

def form_rows_to_append(smart:smartsheet.Smartsheet,column_mapping:Dict[str,str], data:pd.DataFrame) -> List[Any]:
    all_rows = [] # this will be a list of DataRow objects
    columns = data.columns
    for index in range(len(data)):
        single_row = [] # this will be a list of smartsheet.Smartsheet.models.Row objects
        new_row = smart.models.Row()
        for column in columns:
            row_value = data[column].iloc[index]
            column_id = column_mapping.get(column)
            new_row.to_top = False # specify that the rows will be appended to an existing sheet
            new_row.cells.append({
                    'column_id': column_id,
                    'value': row_value})
            single_row.append(new_row)
        dr = DataRow(index, single_row)
        # all_rows.append(dr)
        all_rows.append(new_row)

    return all_rows

def upload_append(smart:smartsheet.Smartsheet, rows:List[Any], sheet_id:str):
    try:
        smart.Sheets.add_rows(sheet_id, rows)
    except Exception as e:
        raise(ExitCodeException(f'Error when trying to append rows to sheet: {str(e)}', exit_code= ss.EXIT_CODE_UPLOAD_ERROR))

def upload_replace(smart:smartsheet.Smartsheet, file_path:str,name:str,file_type:str,sheet_id:Optional[str] = None):
    # NOTE: if sheet exists, then update the rows, otherwise create a new one
    if file_type == 'csv':
        smart.Sheets.import_csv_sheet(file = file_path, sheet_name = name, header_row_index = 0, primary_column_index = 0)

    if file_type == 'xlsx':
        pass




def main():
    args = get_args()
    logger = get_logger()
    try:
        if args.folder_name != "":
            file_path = os.path.join(args.folder_name, args.file_name)
        else:
            file_path = args.file_name

        # establish client and check cif access token is valid
        smart = smartsheet.Smartsheet(args.access_token)
        sheet_name = args.sheet_name if args.sheet_name != "" else None
        if connect(logger, smart) == 1:
            sys.exit(ss.EXIT_CODE_INVALID_TOKEN)

        if args.insert_method == 'append':
            # handle replace
            pass

        # handle the file types
        if args.file_type == "csv":
            response = smart.Sheets.import_csv_sheet(
                file=file_path,
                sheet_name=sheet_name,
                header_row_index=0,
                primary_column_index=0,
            )
        else:
            response = smart.Sheets.import_xlsx_sheet(
                file=file_path, sheet_name=sheet_name
            )

        if response.message == "SUCCESS":
            logger.info("Successfully loaded sheet")
            logger.info(f"Sheet can be found at {response.result.permalink}")

        elif response.message == "PARTIAL_SUCCESS":
            logger.warning("Sheet has been partially loaded")
            sys.exit(ss.EXIT_CODE_UPLOAD_ERROR)

    except FileNotFoundError as fne:
        logger.error(
            f"File {file_path} not found. Ensure that the file name and folder name (if supplied) is correct"
        )
    except Exception as e:
        logger.error("Error in uploading sheet")
        logger.error(str(e))


if __name__ == "__main__":
    main()
