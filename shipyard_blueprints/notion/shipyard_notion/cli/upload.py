import argparse
import os
import pandas as pd
import sys

from shipyard_templates import ExitCodeException


from shipyard_notion import NotionClient


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", dest="token", required=True)
    parser.add_argument("--database-id", dest="database_id", required=False)
    parser.add_argument("--page-id", dest="page_id", required=False)
    parser.add_argument("--database-name", dest="database_name", required=False)
    parser.add_argument("--source-file-name", dest="file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="folder_name", required=False, default=""
    )
    parser.add_argument(
        "--insert-method",
        dest="insert_method",
        required=False,
        choices={"append", "replace"},
        default="append",
    )
    return parser.parse_args()


def main():
    args = get_args()

    try:
        notion = NotionClient(args.token)
    except ExitCodeException as err:
        print(f"Error connecting to Notion. {err.message}")
        sys.exit(err.exit_code)
    except Exception as e:
        print(f"An unexpected error connecting to Notion: {e}")
        sys.exit(1)

    try:
        if args.insert_method == "append" and args.database_id == "":
            notion.logger.error("Database id is required for append method")
            sys.exit(notion.EXIT_CODE_INVALID_DATABASE_ID)
        if args.insert_method == "replace" and (
            not args.database_id and not args.page_id
        ):
            notion.logger.error(
                "If `Replace` is selected as the insert method, and a database ID is not provided, the page ID is required to upload the data"
            )
            sys.exit(notion.EXIT_CODE_DB_CREATE_ERROR)

        # check to see if a folder name is provided
        if args.folder_name != "":
            file_path = os.path.normpath(os.path.join(args.folder_name, args.file_name))
        else:
            file_path = args.file_name
        df = pd.read_csv(file_path)

        notion.upload(
            data=df,
            database_id=args.database_id,
            insert_method=args.insert_method,
            page_id=args.page_id,
            database_name=args.database_name,
        )
    except FileNotFoundError:
        notion.logger.error(
            "File not found, ensure that the file name and folder path (if provided) is correct"
        )
        sys.exit(notion.EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as err:
        notion.logger.error(f"Error uploading data to notion. {err.message}")
        sys.exit(err.exit_code)
    except Exception as e:
        notion.logger.error(str(e))
        sys.exit(notion.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
