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

def map_columns(smart:smartsheet.Smartsheet, sheet_id:str) -> Dict[str,str]:
    """ Helper function to produce a quick lookup table for column ids. 

    Args:
        smart: The Smartsheet client
        sheet_id: The ID of the sheet to pull

    Returns: The column mapping in the form of a dictionary where the key is the column name and the value is the ID 
        
    """
    mapping = {}
    sheet_data = smart.get_sheet(sheet_id, page_size = 1)
    columns = sheet_data.columns
    for col in columns:
        title = col['title']
        col_id = col['id']
        mapping[title] = col_id

    return mapping
    

def form_rows(smart:smartsheet.Smartsheet,column_mapping:Dict[str,str], data:pd.DataFrame, to_top:bool) -> List[Any]:
    """ Helper function to generate a list of Rows to sent to Smartsheet. This is used to udpate an existing sheet (whether that is an overwrite, or an append)

    Args:
       n smart: The Smartsheet client
        column_mapping: A dictionary of the column_name as the key and the column_id as the value
        data: The dataframe of the new rows to load

    Returns: The list of rows to be sent
        
    """
    all_rows = [] # this will be a list of DataRow objects
    columns = data.columns
    for index in range(len(data)):
        single_row = [] # this will be a list of smartsheet.Smartsheet.models.Row objects
        new_row = smart.models.Row()
        for column in columns:
            row_value = data[column].iloc[index]
            column_id = column_mapping.get(column)
            new_row.to_top = to_top # specify whether the rows will be appended to an existing sheet or replaced at the top
            new_row.cells.append({
                    'column_id': column_id,
                    'value': row_value})
            single_row.append(new_row)
        dr = DataRow(index, single_row)
        # all_rows.append(dr)
        all_rows.append(new_row)

    return all_rows

def read_data(file_path:str, file_type:str = 'csv'):
    if file_type == 'xlsx':
        return pd.read_excel(file_path)
    return pd.read_csv(file_path)

def upload_append(smart:smartsheet.Smartsheet,file_path:str,sheet_id:str,file_type:str = 'csv'):
    df = read_data(file_path, file_type)
    column_mapping = map_columns(smart, sheet_id)
    rows = form_rows(smart,column_mapping, df, to_top = False)
    try:
        smart.Sheets.add_rows(sheet_id, rows)
    except FileNotFoundError:
        raise(ExitCodeException(f'Error when trying to read in data, file {file_path} was not found', exit_code = ss.EXIT_CODE_FILE_NOT_FOUND))
    except Exception as e:
        raise(ExitCodeException(f'Error when trying to append rows to sheet: {str(e)}', exit_code= ss.EXIT_CODE_UPLOAD_ERROR))

def upload_replace(smart:smartsheet.Smartsheet, file_path:str,name:str,file_type:str, sheet_name:str,sheet_id:Optional[str] = None):
    try:
        if file_type == 'csv':
            # NOTE: if sheet exists, then update the rows, otherwise create a new one
            if sheet_id:
                # get the sheet_data
                sheet = smart.get_sheet(sheet_id)
                # clear the exiting rows in the sheet 
                sheet.rows = []
                data = pd.read_csv(file_path)
                column_mapping = map_columns(smart,sheet_id)
                new_rows = form_rows(smart,column_mapping, data, True)
                # add the new rows
                sheet.rows = new_rows
                response = smart.Sheets.update_sheet(sheet)
            else:
                response = smart.Sheets.import_csv_sheet(file = file_path, sheet_name = name, header_row_index = 0, primary_column_index = 0)

        if file_type == 'xlsx':
            if sheet_id:
                # get the sheet_data
                sheet = smart.get_sheet(sheet_id)
                # clear the exiting rows in the sheet 
                sheet.rows = []
                data = pd.read_excel(file_path)
                column_mapping = map_columns(smart,sheet_id)
                new_rows = form_rows(smart,column_mapping, data, True)
                # add the new rows
                sheet.rows = new_rows
                response = smart.Sheets.update_sheet(sheet)
            else:
                response = smart.Sheets.import_xlsx_sheet(
                    file=file_path, sheet_name=sheet_name
                    )
    except Exception as e:
        raise(ExitCodeException(f'Error in running replace job. {str(e)}', exit_code = ss.EXIT_CODE_UPLOAD_ERROR))
    else:
        return response

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
        sheet_id = args.sheet_id if args.sheet_id != '' else None
        if connect(logger, smart) == 1:
            sys.exit(ss.EXIT_CODE_INVALID_TOKEN)

        if args.insert_method == 'append':
           # handle replace
            
            upload_append(smart,sheet_id = sheet_id, file_path = file_path)


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
