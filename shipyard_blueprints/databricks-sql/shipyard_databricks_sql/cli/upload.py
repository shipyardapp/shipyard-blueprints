import argparse
import os
import sys
import pandas as pd
import re
import ast

from shipyard_templates import ExitCodeException, ShipyardLogger
from shipyard_databricks_sql import DatabricksSqlClient
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
        choices={"exact_match", "regex_match", "glob"},
    )
    return parser.parse_args()


def list_local_files(directory: Optional[str] = None) -> List[str]:
    """Returns all the files located within the directory path. If the dirname is not provided, then the current working directory will be used and it will span all subdirectories

    Args:
        dirname: The optional directory to span

    Returns: List of all the files

    """
    if directory is None:
        directory = os.getcwd()

    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            files.append(file_path)

    return files


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all matching_file_names that matched the regular expression.
    """
    matching_file_names = []
    for file in file_names:
        if re.search(file_name_re, file):
            matching_file_names.append(file)

    return matching_file_names


def main():
    args = get_args()
    catalog = args.catalog if args.catalog != "" else None
    schema = args.schema if args.schema != "" else None
    volume = args.volume if args.volume != "" else None
    folder_name = args.folder_name if args.folder_name != "" else None
    data_types = ast.literal_eval(args.data_types) if args.data_types != "" else None
    dir_path = None

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
        if args.match_type == "regex_match":
            local_files = list_local_files(directory=dir_path)
            file_matches = find_all_file_matches(
                file_names=local_files, file_name_re=re.compile(args.file_name)
            )
            if len(file_matches) == 0:
                logger.error(f"No files found matching {args.file_name} pattern")
                sys.exit(client.EXIT_CODE_FILE_NOT_FOUND)

            insert_method = args.insert_method
            client.upload(
                file_path=file_matches,
                file_format=args.file_type,
                table_name=args.table_name,
                datatypes=data_types,
                insert_method=insert_method,
                match_type=args.match_type,
                pattern=args.file_name,
            )
            # for i, file_match in enumerate(file_matches, start=1):
            #     logger.info(f"Uploading {i} of {len(file_matches)} files")
            #     if i > 1:
            #         insert_method = "append"
            #     if args.file_type == "csv":
            #         data = pd.read_csv(file_match)
            #         file_format = "csv"
            #     else:
            #         data = pd.read_parquet(file_match)
            #         file_format = "parquet"
            #     client.upload(
            #         file_path=file_match,
            #         file_format=file_format,
            #         table_name=args.table_name,
            #         datatypes=data_types,
            #         insert_method=insert_method,
            #     )

        else:
            # only two choices are csv and parquet at this time
            if args.file_type == "csv":
                data = pd.read_csv(full_path)
                file_format = "csv"
            # for parquet files
            else:
                data = pd.read_parquet(full_path)
                file_format = "parquet"

            client.upload(
                file_path=full_path,
                file_format=file_format,
                table_name=args.table_name,
                datatypes=data_types,
                insert_method=args.insert_method,
            )

    except ExitCodeException as ec:
        logger.error(
            f"ExitCodeException: Error in attempting to upload {full_path}: {ec.message}"
        )
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"Error in attempting to upload {full_path}:{str(e)}")
        sys.exit(client.EXIT_CODE_INVALID_QUERY)
    else:
        logger.info(
            f"Successfully loaded {full_path} to Databricks table {args.table_name}"
        )

    finally:
        client.close()


if __name__ == "__main__":
    main()
