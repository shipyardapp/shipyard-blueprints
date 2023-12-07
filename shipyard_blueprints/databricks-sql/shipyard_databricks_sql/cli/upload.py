import argparse
import os
import sys
import pandas as pd

from shipyard_templates import ExitCodeException
from shipyard_databricks_sql import DatabricksSqlClient


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--server-host", dest="server_host", required=True)
    parser.add_argument("--http-path", dest="http_path", required=True)
    parser.add_argument("--catalog", dest="catalog", required=False, default="")
    parser.add_argument("--schema", dest="schema", required=False, default="")
    parser.add_argument("--table-name", dest="table_name", required=True)
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
    return parser.parse_args()


def main():
    args = get_args()
    catalog = args.catalog if args.catalog != "" else None
    schema = args.schema if args.schema != "" else None
    folder_name = args.folder_name if args.folder_name != "" else None
    data_types = args.data_types if args.data_types != "" else None
    if folder_name:
        full_path = os.path.join(os.getcwd(), folder_name, args.file_name)
    else:
        full_path = os.path.join(os.getcwd(), args.file_name)

    try:
        client = DatabricksSqlClient(
            server_host=args.server_host,
            http_path=args.http_path,
            access_token=args.access_token,
            catalog=catalog,
            schema=schema,
        )

        # only two choices are csv and parquet at this time
        if args.file_type == "csv":
            data = pd.read_csv(full_path)
        # for parquet files
        else:
            data = pd.read_parquet(full_path)

        client.upload(
            data=data,
            table_name=args.table_name,
            datatypes=args.data_types,
            insert_method=args.insert_method,
        )

    except ExitCodeException as ec:
        client.logger.error(
            f"ExitCodeException: Error in attempting to upload {full_path}: {ec.message}"
        )
        sys.exit(ec.exit_code)
    except Exception as e:
        client.logger.error(f"Error in attempting to upload {full_path}:{str(e)}")
        sys.exit(client.EXIT_CODE_INVALID_QUERY)
    else:
        client.logger.info(
            f"Successfully loaded {full_path} to Databricks table {args.table_name}"
        )


if __name__ == "__main__":
    main()
