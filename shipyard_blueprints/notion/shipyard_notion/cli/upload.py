import argparse
import os
import pandas as pd
import sys
from typing import Union
from shipyard_notion import NotionClient

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', dest = 'token', required = True)
    parser.add_argument('--database-id', dest = 'database_id', required = False, default = '')
    parser.add_argument('--source-file-name', dest = 'file_name', required = True)
    parser.add_argument('--source-folder-name', dest = 'folder_name', required = False, default = '')
    parser.add_argument('--insert-method', dest = 'insert_method', required = False, choices = {'append','replace'}, default = 'append')
    return parser.parse_args()


def main():
    args = get_args()
    # check to see if a folder name is provided 
    notion = NotionClient(args.token)
    if args.folder_name != '':
        file_path = os.path.normpath(os.path.join(args.folder_name, args.file_name))
    else:
        file_path = args.file_name
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        notion.logger.error('File not found, ensure that the file name and folder path (if provided) is correct')
        sys.exit(notion.EXIT_CODE_FILE_NOT_FOUND)

    # check to see if a database id is provided 
    if args.insert_method == 'append':
        if args.database_id == '':
            notion.logger.error('Database id is required for append method')
            sys.exit(notion.EXIT_CODE_INVALID_DATABASE_ID)
        else:
            try:
                notion.upload(data = df, database_id = args.database_id)
            except Exception as e:
                notion.logger.error("Error uploading data to notion")
                notion.logger.exception(e)
                sys.exit(notion.EXIT_CODE_UPLOAD_ERROR)
    # handle replacements
    else:
        try:
            notion.upload(data = df, database_id = args.database_id)
        except Exception as e:
            notion.logger.error("Error uploading data to notion")
            notion.logger.exception(e)
            sys.exit(notion.EXIT_CODE_UPLOAD_ERROR)
        
if __name__ == "__main__":
    main()
