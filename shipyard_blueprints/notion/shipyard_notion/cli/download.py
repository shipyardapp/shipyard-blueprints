import argparse
import os
import sys
import pandas as pd
import json
from shipyard_notion import NotionClient

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', dest = 'token', required = True)
    parser.add_argument('--database-id', dest = 'database_id', required = False, default = '')
    parser.add_argument('--destination-file-name', dest = 'file_name',required = True)
    parser.add_argument('--destination-folder-name', dest = 'folder_name', required = False, default = '')
    parser.add_argument('--file-type', dest = 'file_type', choices= {'csv','json'}, default = 'csv')
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
        data = notion.fetch(database_id= args.database_id)[0]
    except Exception as e:
        notion.logger.error("Error in downloading data from notion")
        notion.logger.exception(e)
        sys.exit(notion.EXIT_CODE_DOWNLOAD_ERROR)

    notion.logger.info("Successfully fetched data from notion")

    if args.file_type == 'csv':
        df = pd.DataFrame(data['properties'])
        df.to_csv(file_path, index = False)

    else:
        with open(file_path, 'w') as f:
            json.dump(data,f)

    notion.logger.info(f"Successfully wrote notion data to {file_path}")


if __name__ == "__main__":
    main()

