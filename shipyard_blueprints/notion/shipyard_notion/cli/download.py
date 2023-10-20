import argparse
import os
import sys
import pandas as pd
import json

from shipyard_templates import ExitCodeException
from shipyard_notion import NotionClient
from shipyard_notion.notion_utils import flatten_json


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", dest="token", required=True)
    parser.add_argument("--database-id", dest="database_id", required=True)
    parser.add_argument("--destination-file-name", dest="file_name", required=True)
    parser.add_argument(
        "--destination-folder-name", dest="folder_name", required=False, default=""
    )
    parser.add_argument(
        "--file-type", dest="file_type", choices={"csv", "json"}, default="csv"
    )
    return parser.parse_args()


def main():
    args = get_args()
    notion = NotionClient(args.token)
    # check to see the database is shared with integration
    if not notion.is_accessible(args.database_id):
        notion.logger.error(
            f"Database provided is not accessible. Please ensure that the database is shared with the Integration created in the Notion developer portal"
        )
        sys.exit(notion.EXIT_CODE_INVALID_DATABASE_ID)

    # check to see if a folder name is provided
    if args.folder_name != "":
        file_path = os.path.normpath(os.path.join(args.folder_name, args.file_name))
    else:
        file_path = args.file_name

    try:
        data = list(
            notion.fetch(database_id=args.database_id)
        )  # This will be a list with each element of the list containing at most 100 rows
    except ExitCodeException as ec:
        notion.logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        notion.logger.error("Error in downloading data from notion")
        notion.logger.exception(str(e))
        sys.exit(notion.EXIT_CODE_DOWNLOAD_ERROR)

    notion.logger.info("Successfully fetched data from notion")

    if args.file_type == "csv":
        try:
            flat = flatten_json(data)
        except ExitCodeException as ec:
            notion.logger.error(ec.message)
            sys.exit(ec.exit_code)
        except Exception as e:
            notion.logger.error("Error in parsing the json response")
            notion.logger.error(str(e))
            sys.exit(notion.EXIT_CODE_DOWNLOAD_ERROR)

        df = pd.DataFrame(flat)

        df.to_csv(file_path, index=False)

    else:
        with open(file_path, "w") as f:
            json.dump(data, f)

    notion.logger.info(f"Successfully wrote notion data to {file_path}")


if __name__ == "__main__":
    main()
