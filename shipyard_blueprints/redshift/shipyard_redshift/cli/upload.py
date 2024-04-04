import argparse
import re
import sys
import shipyard_bp_utils as shipyard
import pandas as pd
from shipyard_templates import ShipyardLogger, Database, ExitCodeException
from shipyard_redshift import RedshiftClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--host", dest="host", required=False)
    parser.add_argument("--database", dest="database", required=False)
    parser.add_argument("--port", dest="port", default="5439", required=False)
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
    parser.add_argument("--schema", dest="schema", required=False, default="")
    args = parser.parse_args()

    if args.host and not (args.database or args.username):
        parser.error("--host requires --database and --username")
    if args.database and not (args.host or args.username):
        parser.error("--database requires --host and --username")
    if args.username and not (args.host or args.username):
        parser.error("--username requires --host and --username")
    return args


def main():
    args = get_args()
    match_type = args.source_file_name_match_type
    src_file = args.source_file_name
    src_dir = args.source_folder_name
    src_path = shipyard.files.combine_folder_and_file_name(
        folder_name=src_dir, file_name=src_file
    )
    table_name = args.table_name
    insert_method = args.insert_method
    redshift_args = {
        "host": args.host,
        "user": args.username,
        "pwd": args.password,
        "database": args.database,
        "port": args.port,
        "schema": args.schema if args.schema != "" else None,
        "url_params": args.url_parameters if args.url_parameters != "" else None,
    }

    redshift = RedshiftClient(**redshift_args)
    try:
        if match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(src_dir)
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(src_file)
            )
            if n_matches := len(matching_file_names) == 0:
                logger.error(f"No matches found for pattern {src_file}")
                sys.exit(Database.EXIT_CODE_NO_FILE_MATCHES)
            logger.info(
                f"{len(matching_file_names)} files found. Preparing to upload..."
            )

            for file_match in matching_file_names:
                redshift.upload(
                    file=file_match, table_name=table_name, insert_method=insert_method
                )
                if insert_method == "replace":
                    insert_method = "append"
            else:
                logger.info(f"Successfully loaded all files to {table_name}")
        else:
            redshift.upload(
                file=src_path, table_name=table_name, insert_method=insert_method
            )
            logger.info(f"Successfully loaded {src_path} to {table_name}")
    except FileNotFoundError:
        logger.error(
            f"File {src_path} was not found. Please ensure that file name and folder name (if provided) are correcet"
        )
        sys.exit(Database.EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unpexpected error occurred when attempting to upload into Redshift. Message from the server reads: {e}"
        )

    finally:
        redshift.close()


if __name__ == "__main__":
    main()
