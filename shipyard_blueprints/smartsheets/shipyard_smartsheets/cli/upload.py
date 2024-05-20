import argparse
import os
import smartsheet
import sys
import logging
import pandas as pd
import numpy as np
import requests
from shipyard_templates import ExitCodeException, Spreadsheets as ss, ShipyardLogger
from typing import Dict, Any, List, Optional

# custom exit code
EXIT_CODE_INVALID_SHEET_ID = 220
EXIT_CODE_COLUMN_UPDATE_ERROR = 221

logger = ShipyardLogger.get_logger()


def connect(smartsheet: smartsheet.Smartsheet):
    try:
        conn = smartsheet.Users.get_current_user()
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
    parser.add_argument(
        "--insert-method",
        dest="insert_method",
        required=False,
        default="replace",
        choices={"replace", "append", "create"},
    )
    parser.add_argument("--sheet-id", dest="sheet_id", required=False, default="")
    return parser.parse_args()


def is_valid_sheet(smart: smartsheet.Smartsheet, sheet_id: str) -> bool:
    try:
        response = smart.Sheets.get_sheet(sheet_id, page_size=1)
        if isinstance(response, smart.models.error.Error):
            return False
    except Exception as e:
        return False
    else:
        return True


def map_columns(smart: smartsheet.Smartsheet, sheet_id: str) -> Dict[str, str]:
    """Helper function to produce a quick lookup table for column ids.

    Args:
        smart: The Smartsheet client
        sheet_id: The ID of the sheet to pull

    Returns: The column mapping in the form of a dictionary where the key is the column name and the value is the ID

    """
    try:
        mapping = {}
        sheet_data = smart.Sheets.get_sheet(sheet_id, page_size=1)
        columns = sheet_data.columns
        for col in columns:
            title = col.title
            col_id = col.id
            mapping[title] = col_id
    except Exception as e:
        raise ExitCodeException(f"Error in mapping columns: {str(e)}", 1)
    else:
        return mapping


def update_column(
    smart: smartsheet.Smartsheet, sheet_id: str, column_id: str, column_name: str
):
    """Helper function to update the column name to the name provided in the dataframe

    Args:
        smart: The Smartsheet client
        sheet_id: The ID of the sheet
        column_id: The ID of the column to update
        column_name: The new name for the column to have

    Raises:
        ExitCodeException:

    Returns: The resposne from the Smartsheet API

    """
    col_spec = smart.models.Column({"title": column_name})
    try:
        resp = smart.Sheets.update_column(sheet_id, column_id, col_spec)
    except Exception as e:
        raise ExitCodeException(
            f"Error in updating Smartsheet column: {str(e)}",
            EXIT_CODE_COLUMN_UPDATE_ERROR,
        )
    else:
        return resp


def form_rows(
    smart: smartsheet.Smartsheet,
    column_mapping: Dict[str, str],
    data: pd.DataFrame,
    sheet_id: str,
    insert_method="append",
) -> List[Any]:
    """Helper function to generate a list of Rows to sent to Smartsheet. This is used to udpate an existing sheet (whether that is an overwrite, or an append)


    Args:
        insert_method ():  replace or append
        smart: the smartsheet client
        column_mapping: The mapping of column name to column ID (can be obtained by the map_columns function)
        data: Pandas Dataframe to upload
        sheet_id: The ID of the related sheet

    Returns:

    """
    all_rows = []  # this will be a list of DataRow objects
    columns = data.columns
    try:
        for index in range(len(data)):
            single_row = (
                []
            )  # this will be a list of smartsheet.Smartsheet.models.Row objects
            new_row = smart.models.Row()
            for column, ss_col in zip(columns, column_mapping):
                row_value = data[column].iloc[index]
                column_id = column_mapping.get(column)
                if (
                    not column_id
                ):  # if for some reason there is a change in column names, then grab the id by index
                    column_id = column_mapping.get(ss_col)
                    # update the column name in smartsheet to match the column name from the file
                    temp = update_column(
                        smart,
                        sheet_id=sheet_id,
                        column_id=column_id,
                        column_name=column,
                    )
                    if temp.message != "SUCCESS":
                        raise ExitCodeException(
                            "Error in updating column names in Sheet",
                            EXIT_CODE_COLUMN_UPDATE_ERROR,
                        )

                # specify whether the rows will be appended to an existing sheet or replaced at the top
                if insert_method == "append":
                    new_row.to_bottom = True
                elif insert_method == "replace":
                    new_row.to_top = True
                cell = smart.models.Cell()
                cell.column_id = column_id
                if isinstance(row_value, np.bool_):
                    if row_value:
                        cell.value = bool(
                            row_value
                        )  # this is to properly set the cell value when it is false
                    else:
                        cell.value = not True
                elif isinstance(row_value, np.int64):
                    cell.value = int(row_value)
                elif isinstance(row_value, np.float64):
                    cell.value = float(row_value)
                else:
                    cell.value = row_value
                new_row.cells.append(cell)
            single_row.append(new_row)
            all_rows.append(new_row)
    except Exception as e:
        raise ExitCodeException(
            f"Error in forming rows to be loaded: {str(e)}", ss.EXIT_CODE_UPLOAD_ERROR
        )

    else:
        return all_rows


def read_data(file_path: str, file_type: str = "csv"):
    """Helper function to read in data from either csv or xlsx format

    Args:
        file_path: The file path to read in
        file_type: csv or xlsx

    Returns: A Pandas Datafram

    """
    if file_type == "xlsx":
        return pd.read_excel(file_path)

    return pd.read_csv(file_path)


def upload_append(
    smart: smartsheet.Smartsheet,
    file_path: str,
    sheet_id: Optional[str],
    file_type: str = "csv",
):
    """Function to upload append jobs to an existing Sheet in Smartsheet.

    Args:
        smart: The smartsheet client
        logger: The logger to STDOUT
        file_path: The file path of the data to load
        sheet_id: The ID of the existing sheet to append to
        file_type: csv or xlsx file


    Returns: The response from the Smartsheet API

    """
    if not sheet_id:
        raise ExitCodeException(
            "A Sheet ID is required in order to append", ss.EXIT_CODE_BAD_REQUEST
        )
    df = read_data(file_path, file_type)
    column_mapping = map_columns(smart, sheet_id)
    sheet = smart.Sheets.get_sheet(sheet_id)
    rows = form_rows(
        smart, column_mapping, df, insert_method="append", sheet_id=sheet.id
    )
    try:
        resp = smart.Sheets.add_rows(sheet.id, rows)
    except FileNotFoundError:
        raise (
            ExitCodeException(
                f"Error when trying to read in data, file {file_path} was not found",
                exit_code=ss.EXIT_CODE_FILE_NOT_FOUND,
            )
        )
    except Exception as e:
        raise (
            ExitCodeException(
                f"Error when trying to append rows to sheet: {str(e)}",
                exit_code=ss.EXIT_CODE_UPLOAD_ERROR,
            )
        )
    else:
        return resp


def delete_columns(column_id: str, sheet_id, token):
    url = f"https://api.smartsheet.com/2.0/sheets/{sheet_id}/columns/{column_id}"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.delete(url=url, headers=headers)


def validate_columns(df_colnames: List[Any], api_colnames: List[Any]):
    # check to see if the column names from the dataframe match the column names in the sheet already
    bad_map = {}
    if len(df_colnames) != len(api_colnames):
        msg = f"There is an error in the "
    for df_col, api_col in zip(df_colnames, api_colnames):
        if df_col != api_col:
            bad_map[api_col] = df_col


def delete_sheet_contents(
    smart: smartsheet.Smartsheet, logger: logging.Logger, sheet_id: str
):
    """Helper function to delete all the row content in an existing sheet

    Args:
        smart: The smartsheet client
        logger: The logger to STDOUT
        sheet_id: The ID for the sheet to modify
    """
    # NOTE: May need to delete columns as well as rows
    try:
        sheet = smart.Sheets.get_sheet(sheet_id)
        # cols = smart.Sheets.get_columns(sheet_id)
        row_ids = [row.id for row in sheet.rows]
        # col_ids = [col.id for col in sheet.columns]
        for i in range(0, len(row_ids), 100):
            smart.Sheets.delete_rows(sheet.id, row_ids[i : i + 100])

    except Exception as e:
        logger.error("Error encoutered when deleting rows")
        raise ExitCodeException(
            f"Error in deleting rows {str(e)}", ss.EXIT_CODE_BAD_REQUEST
        )

    else:
        logger.info("Successfully deleted sheet rows")


def upload_create(
    smart: smartsheet.Smartsheet,
    file_path: str,
    name: Optional[str],
    file_type: str,
):
    try:
        if file_type == "csv":
            response = smart.Sheets.import_csv_sheet(
                file=file_path,
                sheet_name=name,
                header_row_index=0,
                primary_column_index=0,
            )
        else:
            response = smart.Sheets.import_xlsx_sheet(
                file=file_path,
                sheet_name=name,
                header_row_index=0,
                primary_column_index=0,
            )

    except FileNotFoundError:
        raise (
            ExitCodeException(
                f"Error when trying to read in data, file {file_path} was not found",
                exit_code=ss.EXIT_CODE_FILE_NOT_FOUND,
            )
        )

    except Exception as e:
        raise (
            ExitCodeException(
                f"Error in running replace job. {str(e)}",
                exit_code=ss.EXIT_CODE_UPLOAD_ERROR,
            )
        )
    else:
        return response


def upload_replace(
    smart: smartsheet.Smartsheet,
    logger: logging.Logger,
    file_path: str,
    name: Optional[str],
    file_type: str,
    sheet_id: Optional[str],
):
    """This function uploads a file (read in as a pandas dataframe) to Smartsheet. If the sheet_id is provided, the existing sheet will be overwritten (if the datatypes) match, otherwise a new sheet will be created
    Args:
        smart: The smartsheet client
        logger: The logger to STDOUT
        file_path: The file path of target file to be uploaded
        name: The name of the sheet to be created
        file_type: The type of file being loaded (csv or xlsx)
        sheet_id: The ID of the sheet to be overwritten

    Returns: The response from the smartsheet API

    """
    try:
        if not sheet_id:
            raise ExitCodeException(
                "In order to replace an existing sheet, a sheet ID is required",
                ss.EXIT_CODE_UPLOAD_ERROR,
            )

        data = read_data(file_path, file_type)
        sheet = smart.Sheets.get_sheet(sheet_id)
        column_mapping = map_columns(smart, sheet_id)
        new_rows = form_rows(
            smart, column_mapping, data, insert_method="replace", sheet_id=sheet.id
        )
        # clear the existing sheet content
        delete_sheet_contents(smart, logger, sheet_id)
        response = smart.Sheets.add_rows(sheet.id, new_rows)

    except FileNotFoundError:
        raise (
            ExitCodeException(
                f"Error when trying to read in data, file {file_path} was not found",
                exit_code=ss.EXIT_CODE_FILE_NOT_FOUND,
            )
        )

    except Exception as e:
        raise (
            ExitCodeException(
                f"Error in running replace job. {str(e)}",
                exit_code=ss.EXIT_CODE_UPLOAD_ERROR,
            )
        )
    else:
        return response


def main():
    args = get_args()
    try:
        if args.folder_name != "":
            file_path = os.path.join(args.folder_name, args.file_name)
        else:
            file_path = args.file_name

        # establish client and check cif access token is valid
        smart = smartsheet.Smartsheet(args.access_token)
        sheet_name = args.sheet_name if args.sheet_name != "" else None
        sheet_id = args.sheet_id if args.sheet_id != "" else None
        if connect(smart) == 1:
            sys.exit(ss.EXIT_CODE_INVALID_TOKEN)

        # check to see if the sheet id provided is valid, fail if not
        if sheet_id and not is_valid_sheet(smart, sheet_id):
            logger.error("Error: sheet ID provided is not valid")
            sys.exit(EXIT_CODE_INVALID_SHEET_ID)

        if args.insert_method == "create":
            response = upload_create(
                smart,
                logger=logger,
                file_path=file_path,
                name=sheet_name,
                file_type=args.file_type,
            )

        elif args.insert_method == "append":
            response = upload_append(
                smart,
                file_path=file_path,
                sheet_id=sheet_id,
                file_type=args.file_type,
            )

        else:
            response = upload_replace(
                smart,
                logger=logger,
                file_path=file_path,
                name=sheet_name,
                file_type=args.file_type,
                sheet_id=sheet_id,
            )

        if response.message == "SUCCESS":
            logger.info("Successfully loaded sheet")

        elif (
            response.message == "PARTIAL_SUCCESS"
        ):  # this shouldn't ever be reached, but just for precautions
            logger.warning("Sheet has been partially loaded")
            sys.exit(ss.EXIT_CODE_UPLOAD_ERROR)
        elif response.message == "ERROR":
            error_json = response.to_dict()["response"]
            reason = error_json["reason"]
            message = error_json["content"]["message"]
            status_code = error_json["statusCode"]
            logger.error(
                f"Upload returned a {status_code} for signifying a {reason}. Error message reads: {message}"
            )
            sys.exit(ss.EXIT_CODE_UPLOAD_ERROR)

        else:
            logger.warning(
                f"Unknown response status from Smartsheet API: {response.message}"
            )
            sys.exit(ss.EXIT_CODE_UNKNOWN_ERROR)

    except FileNotFoundError as fne:
        logger.error(
            f"File {file_path} not found. Ensure that the file name and folder name (if supplied) is correct"
        )
        sys.exit(ss.EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as ec:
        logger.error(
            f"Error encountered when attemtping to upload sheet via {args.insert_method} method: {ec.message}"
        )
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error("Error in uploading sheet")
        logger.error(str(e))
        sys.exit(ss.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
