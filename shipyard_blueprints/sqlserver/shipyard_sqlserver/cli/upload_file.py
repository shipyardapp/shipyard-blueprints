import argparse
import os
import re
import sys
import pandas as pd
import shipyard_bp_utils as shipyard
from shipyard_sqlserver import SqlServerClient
from shipyard_templates import ExitCodeException, ShipyardLogger, Database

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--host", dest="host", required=False)
    parser.add_argument("--database", dest="database", required=False)
    parser.add_argument("--port", dest="port", default="1433", required=False)
    parser.add_argument("--url-parameters", dest="url_parameters", required=False)
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match"},
        required=False,
    )
    parser.add_argument(
        "--source-file-name",
        dest="source_file_name",
        default="output.csv",
        required=True,
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--table-name", dest="table_name", default=None, required=True)
    parser.add_argument(
        "--insert-method",
        dest="insert_method",
        choices={"replace", "append"},
        default="append",
        required=False,
    )
    args = parser.parse_args()

    if args.host and not (args.database or args.username):
        parser.error("--host requires --database and --username")
    if args.database and not (args.host or args.username):
        parser.error("--database requires --host and --username")
    if args.username and not (args.host or args.username):
        parser.error("--username requires --host and --username")
    return args


def main():
    client = None
    try:
        args = get_args()
        match_type = args.source_file_name_match_type
        file_name = args.source_file_name
        dir_name = args.source_folder_name
        file_path = shipyard.files.combine_folder_and_file_name(
            folder_name=dir_name, file_name=file_name
        )
        table_name = args.table_name
        insert_method = args.insert_method

        client = SqlServerClient(
            user=args.username,
            pwd=args.password,
            host=args.host,
            database=args.database,
            port=args.port,
            url_params=args.url_parameters,
        )

        if match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(dir_name)
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(file_name)
            )
            logger.info(
                f"{len(matching_file_names)} files found. Preparing to upload..."
            )

            for index, file in enumerate(matching_file_names):
                df = pd.read_csv(file)
                if index > 0:
                    insert_method = "append"
                client.upload(df=df, table_name=table_name, insert_method=insert_method)
                logger.info(f"Upload of {file} complete")

            logger.info(f"Successfully loaded all files to {table_name}")

        else:
            df = pd.read_csv(file_path)
            client.upload(df=df, table_name=table_name, insert_method=insert_method)
            logger.info(f"Successfully loaded {file_path} to {table_name}")

    except FileNotFoundError:
        logger.error(f"File {file_path} does not exist")
        sys.exit(Database.EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to upload data to SQL Server. Message from the server reads: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)

    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
