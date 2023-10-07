import os
import logging
import argparse
import sys
import subprocess
import requests
from sys import argv
from typing import Optional
from csv2notion.notion_db_client import NotionClient as csv_nc 


EXIT_CODE_FILE_NOT_FOUND = 201
EXIT_CODE_DATABASE_NOT_FOUND = 202
EXIT_CODE_UPLOAD_ERROR = 203
EXIT_CODE_INVALID_CREDENTIALS = 204

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token-v2", dest = 'token', required = True)
    parser.add_argument('--url', dest = 'url', required = False)
    # parser.add_argument('--table', dest = 'table', required = True)
    parser.add_argument('--file-name', dest = 'file_name', required = True)
    parser.add_argument('--file-name-match-type', dest = 'file_name_match_type', default= 'exact_match', choices = {'regex_match','exact_match'}, required = False)
    parser.add_argument('--folder-name', dest = 'folder_name', required = False)
    parser.add_argument('--insert-method', dest = 'insert_method', default = 'append', choices = {'replace','append'}, required = False)

    return parser.parse_args()


def get_shipyard_logger():
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


# def file_exists(file_name:str, dir_name:Optional[str] = None):
#     if dir_name:
#         full_path = os.path.normpath(os.path.join(dir_name, file_name))
#         return os.path.isfile(full_path)
#     return os.path.isfile(file_name)
#
# def upload(logger:logging.Logger, args):
#     file_name = args.file_name
#     insert_method = args.insert_method
#
#     try:
#         res = subprocess.run(['csv2notion','--token', args.token, file_name])
#     except Exception as e:
#         logger.error(f"Could not load the file into Notion")
#         logger.exception(e)
#     else:
#         return res
#
# def format_args(args:argparse.Namespace) ->str:
#     base = f"--token {args.token}"
#     return base

# def main():
#     args = get_args()
#     logger = get_logger()
#     # check to see if the token provided is valid
#     try:
#         connect(access_token= args.token, logger = logger)
#     except:
#         logger.error(f"Could not connect to Notion with provided token")
#         sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
#
#     if args.match_type == 'regex_match':
#         # handle regex match 
#         pass
#     else:
#         # handle exact match
#         try:
#             res = upload(logger, args)
#         except Exception as e:
#             sys.exit(EXIT_CODE_UPLOAD_ERROR)
#     
def delete_database(database_id:str):
    db_url = f'https://api.notion.com/v1/blocks/{database_id}'

def upload(args:argparse.Namespace):
    if args.folder_name:
        file_name = os.path.normpath(os.path.join(args.folder_name, args.file_name))
    else:
        file_name = args.file_name

    if args.insert_method == 'append':
        # handle cases for appending to an existing table, if table does not exist then a new one will be created
        try:
            subprocess.run(['csv2notion', '--token', args.token, file_name])
        except Exception as e:
            print('Could not load csv')
            print(e)
    if args.insert_method == 'replace':
        # handle cases for replacing the existing table, if table does not exist then a new one will be created
        pass


def main():
    args = get_args()
    logger = get_shipyard_logger()

    try:
        client = csv_nc(token_v2= args.token)
    except Exception as e:
        logger.error("Could not connect to Notion with the token provided. Please ensure that you are grabbing the latest cookie value")
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)

    # setup the list of arguments to pass to the subprocess
    cmd_list = ['csv2notion', '--token', args.token]
    
    if args.folder_name:
        file_name = os.path.normpath(os.path.join(args.folder_name, args.file_name))
    else:
        file_name = args.file_name

    if args.url:
        cmd_list.append('--url')
        cmd_list.append(args.url)

    if args.insert_method == 'append':
        cmd_list.append(file_name)
        try:
            subprocess.run(cmd_list)
                
        except Exception as e:
            print(e)
            sys.exit(EXIT_CODE_UPLOAD_ERROR)

    elif args.insert_method == 'replace':
        cmd_list.append('--merge')
        cmd_list.append(file_name)

        try:
            subprocess.run(cmd_list)
        except Exception as e:
            print(e)
            sys.exit(EXIT_CODE_UPLOAD_ERROR)


if __name__ == "__main__":
    main()
