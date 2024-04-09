import argparse
import os
import sys
import re
import ast
import shipyard_bp_utils as shipyard

from shipyard_templates import ExitCodeException, ShipyardLogger
from shipyard_databricks_sql import DatabricksSqlClient
from shipyard_databricks_sql.utils import exceptions as errs
from typing import Dict, List, Optional

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--server-host", dest="server_host", required=True)
    parser.add_argument("--http-path", dest="http_path", required=True)
    parser.add_argument("--catalog", dest="catalog", required=False, default="")
    parser.add_argument("--schema", dest="schema", required=False, default="")
    parser.add_argument("--table-name", dest="table_name", required=True)
    parser.add_argument("--volume", dest="volume", required=False, default=None)
    parser.add_argument("--data-types", dest="data_types", required=False, default="")
    parser.add_argument(
        "--insert-method",
        dest="insert_method",
        required=True,
        choices={"replace", "append"},
    )
    parser.add_argument(
        "--file-type",
        dest="file_type",
        required=False,
        default="csv",
        choices={"csv", "parquet"},
    )
    parser.add_argument("--file-name", dest="file_name", required=True)
    parser.add_argument("--folder-name", dest="folder_name", required=False)
    parser.add_argument(
        "--match-type",
        dest="match_type",
        choices={"exact_match", "glob_match"},
    )
    return parser.parse_args()


def main():
    args = get_args()
    catalog = args.catalog if args.catalog != "" else None
    schema = args.schema if args.schema != "" else None
    volume = args.volume if args.volume != "" else None
    # folder_name = args.folder_name if args.folder_name != "" else None
    folder_name = args.folder_name
    data_types = ast.literal_eval(args.data_types) if args.data_types != "" else None
    dir_path = None
    client = None

    try:
        if folder_name:
            full_path = os.path.join(os.getcwd(), folder_name, args.file_name)
            dir_path = os.path.join(os.getcwd(), folder_name)
            real_path = os.path.realpath(dir_path)
        else:
            full_path = os.path.join(os.getcwd(), args.file_name)
            real_path = os.path.realpath(full_path)

        client = DatabricksSqlClient(
            server_host=args.server_host,
            http_path=args.http_path,
            access_token=args.access_token,
            catalog=catalog,
            schema=schema,
            volume=volume,
            staging_allowed_local_path=real_path,
        )
        if args.match_type == "glob_match":
            local_files = shipyard.files.find_all_local_file_names(folder_name)
            file_matches = shipyard.files.find_matching_files(
                search_term=args.file_name,
                directory=folder_name,
                match_type=args.match_type,
            )

            if len(file_matches) == 0:
                logger.error(f"No files found matching {args.file_name} pattern")
                sys.exit(errs.EXIT_CODE_FILE_NOT_FOUND)

            client.upload(
                file_path=file_matches,
                file_format=args.file_type,
                table_name=args.table_name,
                datatypes=data_types,
                insert_method=args.insert_method,
                match_type=args.match_type,
                pattern=args.file_name,
            )
            logger.info(
                f"Successfully loaded all files within {file_matches} to Databricks"
            )

        else:
            client.upload(
                file_path=full_path,
                file_format=args.file_type,
                table_name=args.table_name,
                datatypes=data_types,
                insert_method=args.insert_method,
            )
            logger.info(f"Successfully loaded {full_path} into Databricks")

    except ExitCodeException as ec:
        logger.error(
            f"Error in attempting to upload {full_path}. Message from Databricks is: {ec.message}"
        )
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"Error in attempting to upload {full_path}. Mesasge from Databricks is: {str(e)}"
        )
        sys.exit(errs.EXIT_CODE_INVALID_QUERY)
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
