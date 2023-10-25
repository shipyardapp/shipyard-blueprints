import argparse
import os
import smartsheet
import sys 
import logging
from shipyard_templates import Spreadsheets as ss

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest = 'access_token', required = True)
    parser.add_argument('--sheet-id', dest = 'sheet_id', required = True )
    parser.add_argument('--destination-file-name', dest = 'file_name', required = True)
    parser.add_argument('--destination-folder-name', dest = 'folder_name', required = False, default = '')
    parser.add_argument('--file-type', dest = 'file_type', required = False, default = 'csv', choices = {'xlsx','csv'})
    return parser.parse_args()

def connect(logger:logging.Logger,smartsheet:smartsheet.Smartsheet):
    try:
        test = smartsheet.Users.get_current_user()
    except Exception as e:
        logger.error('Error in connecting to Smartsheet')
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

def main():
    args = get_args()
    logger = get_logger()
    try:
        # establish client and check if access token is valid
        smart = smartsheet.Smartsheet(args.access_token)
        if connect(logger, smart) == 1:
            sys.exit(ss.EXIT_CODE_INVALID_TOKEN)

        folder_path = args.folder_name if args.folder_name != '' else os.getcwd()

        if args.file_type == 'csv':
            # handle csv cases
            # response = smart.Sheets.get_sheet_as_csv(sheet_id = args.sheet_id, download_path = folder_path)
            response = smart.Sheets.get_sheet(sheet_id = args.sheet_id)
            print(response)
        else:
            # handle excel sheets
            response = smart.Sheets.get_sheet_as_excel(sheet_id = args.sheet_id, download_path = folder_path, alternate_file_name = args.file_name)

    except Exception as e:
        logger.error('Error in downloading file')
        logger.exception(str(e))
        sys.exit(ss.EXIT_CODE_DOWNLOAD_ERROR)



if __name__ == "__main__":
    main()
