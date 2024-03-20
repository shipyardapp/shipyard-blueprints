import argparse
import re
import sys
import shipyard_bp_utils as shipyard
from shipyard_templates import (
    DatabricksDatabase,
    ShipyardLogger,
    Database,
    ExitCodeException,
)
from shipyard_postgresql import PostgresClient


logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=True)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--host", dest="host", required=True)
    parser.add_argument("--database", dest="database", required=True)
    parser.add_argument("--port", dest="port", default="5432", required=False)
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
    parser.add_argument("--schema", dest="schema", default=None, required=False)
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
    try:
        args = get_args()
        match_type = args.source_file_name_match_type
        src_file = args.source_file_name
        src_dir = args.source_folder_name
        src_path = shipyard.files.combine_folder_and_file_name(
            folder_name=src_dir, file_name=src_file
        )
        table_name = args.table_name
        insert_method = args.insert_method

        # args for client
        client_args = {
            "user": args.username,
            "pwd": args.password,
            "host": args.host,
            "database": args.database,
            "port": args.port,
            "schema": args.schema if args.schema != "" else None,
            "url_params": args.url_parameters if args.url_parameters != "" else None,
        }
        postgres = PostgresClient(**client_args)

        if match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(src_dir)
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(src_file)
            )
            if (n_matches := len(matching_file_names)) == 0:
                logger.error("No files matching files found for the supplied regex")
                sys.exit(Database.EXIT_CODE_NO_FILE_MATCHES)

            logger.info(f"{n_matches} files found. Preparing to upload...")

            for index, key_name in enumerate(matching_file_names):
                postgres.upload(
                    key_name, table_name=table_name, insert_method=insert_method
                )
                if insert_method == "replace":
                    insert_method = "append"

            else:
                logger.info(f"Successfully uploaded all files to {table_name}")

        else:
            postgres.upload(
                src_path, table_name=table_name, insert_method=insert_method
            )
            logger.info(f"Successfully uploaded data to {table_name}")

    except FileNotFoundError as fe:
        logger.error(
            "The file {file_name} could not be found. Ensure that the file name and extension are correct"
        )
        sys.exit(Database.EXIT_CODE_FILE_NOT_FOUND)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when trying to load data to Postgres. Message from the server reads: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)

    finally:
        postgres.close()


if __name__ == "__main__":
    main()
