import argparse
import os
import pandas as pd
import sys
from shipyard_notion import NotionClient

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', dest = 'token', required = True)
    parser.add_argument('--database-id', dest = 'database_id', required = True)
    parser.add_argument('--source-file-name', dest = 'file_name', required = True)
    parser.add_argument('--source-folder-name', dest = 'folder_name', required = False, default = '')
    parser.add_argument('--insert-method', dest = 'insert_method', required = False, choices = {'append','replace'}, default = 'append')
    return parser.parse_args()


def main():
    args = get_args()
    notion = NotionClient(args.token)
    if not notion.is_accessible(args.database_id):
        notion.logger.error(f"Database provided is not accessible. Please ensure that the database is shared with the Integration created in the Notion developer portal")
        sys.exit(notion.EXIT_CODE_INVALID_DATABASE_ID)

    # check to see if a folder name is provided 
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
    if args.insert_method == 'append' and args.database_id == '':
        notion.logger.error('Database id is required for append method')
        sys.exit(notion.EXIT_CODE_INVALID_DATABASE_ID)

    try:
        notion.upload(data = df, database_id = args.database_id, insert_method = args.insert_method)
    except Exception as e:
        notion.logger.error(f"Error uploading data to notion. {str(e)}")
        sys.exit(notion.EXIT_CODE_UPLOAD_ERROR)

        
if __name__ == "__main__":
    main()
