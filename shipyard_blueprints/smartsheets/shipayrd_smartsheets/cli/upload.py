import argparse
import os
import smartsheet
import sys
import logging
from shipyard_templates import Spreadsheets as ss

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

def connect(logger:logging.Logger,smartsheet:smartsheet.Smartsheet):
    response = smartsheet.Sheets.list_sheets()['response']
    content = response['content']
    if response['statusCode'] == 401 and content['errorCode'] in (1002,1003):
        logger.error("Error connecting to Smartsheet")
        logger.error(content['message'])
        return 1
    else:
        return 0


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest = 'access_token', required = True)
    parser.add_argument('--sheet-name', dest = 'sheet_name', required = False, default = '')
    parser.add_argument('--source-file-name', dest = 'file_name', required = True)
    parser.add_argument('--source-folder-name', dest = 'folder_name', required = False, default = '')
    parser.add_argument('--file-type', dest = 'file_type', required = False, default = 'csv', choices = {'xlsx','csv'})
    return parser.parse_args()

def main():
    args = get_args()
    logger = get_logger()
    try:
        if args.folder_name != '':
            file_path = os.path.join(args.folder_name, args.file_name)
        else:
            file_path = args.file_name

        # establish client and check cif access token is valid
        smart = smartsheet.Smartsheet(args.access_token)
        sheet_name = args.sheet_name if args.sheet_name != '' else None
        if connect(logger, smart) == 1:
            sys.exit(ss.EXIT_CODE_INVALID_TOKEN)
        # handle the file types 
        if args.file_type == 'csv':
            response = smart.Sheets.import_csv_sheet(file = file_path, sheet_name = sheet_name)
        else:
            response = smart.Sheets.import_xlsx_sheet(file = file_path, sheet_name = sheet_name)

        if response['message'] == 'SUCCESS':

            pass
        elif response['message'] == 'PARTIAL_SUCCESS':
            pass


    except Exception as e:
        pass

if __name__ == "__main__":
    main()



