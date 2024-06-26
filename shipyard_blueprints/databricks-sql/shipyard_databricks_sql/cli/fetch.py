import argparse
import os
import sys
import shipyard_bp_utils as shipyard

from shipyard_templates import ExitCodeException, ShipyardLogger
from shipyard_databricks_sql import DatabricksSqlClient
from shipyard_databricks_sql.utils import exceptions as errs


logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--server-host", dest="server_host", required=True)
    parser.add_argument("--http-path", dest="http_path", required=True)
    parser.add_argument("--catalog", dest="catalog", required=False, default="")
    parser.add_argument("--schema", dest="schema", required=False, default="")
    parser.add_argument("--query", dest="query", required=True)
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
    folder_name = args.folder_name
    client = None
    try:
        if folder_name:
            shipyard.files.create_folder_if_dne(folder_name)
            full_path = os.path.join(os.getcwd(), folder_name, args.file_name)
        else:
            full_path = os.path.join(os.getcwd(), args.file_name)

        client = DatabricksSqlClient(
            server_host=args.server_host,
            http_path=args.http_path,
            access_token=args.access_token,
            catalog=catalog,
            schema=schema,
        )
        data = client.fetch(args.query)
        logger.info(f"Successfully fetched {data.shape[0]} rows")

        if args.file_type == "csv":
            data.to_csv(full_path, index=False)
        # for parquet files
        else:
            data.to_parquet(full_path, index=False)

    except ExitCodeException as ec:
        logger.error(
            f"ExitCodeException: Error in attempting to fetch results: {ec.message}"
        )
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"Error in attempting to fetch query results:{str(e)}")
        sys.exit(errs.EXIT_CODE_INVALID_QUERY)
    else:
        logger.info(f"Downloaded results to {full_path}")
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
