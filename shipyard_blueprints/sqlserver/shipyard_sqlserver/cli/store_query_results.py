import argparse
import os
import sys
import pandas as pd
import shipyard_bp_utils as shipyard
from shipyard_sqlserver import SqlServerClient
from shipyard_templates import ExitCodeException, ShipyardLogger, Database
from sqlalchemy import text

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--host", dest="host", required=False)
    parser.add_argument("--database", dest="database", required=False)
    parser.add_argument("--port", dest="port", default="1433", required=False)
    parser.add_argument("--url-parameters", dest="url_parameters", required=False)
    parser.add_argument("--query", dest="query", required=True)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default="output.csv",
        required=True,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--file-header", dest="file_header", default="True", required=False
    )
    parser.add_argument("--db-connection-url", dest="db_connection_url", required=False)
    args = parser.parse_args()

    if (
        not args.db_connection_url
        and not (args.host or args.database or args.username)
        and not os.environ.get("DB_CONNECTION_URL")
    ):
        parser.error(
            """This Blueprint requires at least one of the following to be provided:\n
            1) --db-connection-url\n
            2) --host, --database, and --username\n
            3) DB_CONNECTION_URL set as environment variable"""
        )
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
        dest_file = args.destination_file_name
        dest_dir = args.destination_folder_name
        if dest_dir:
            shipyard.files.create_folder_if_dne(dest_dir)
        dest_path = shipyard.files.combine_folder_and_file_name(
            folder_name=dest_dir, file_name=dest_file
        )
        file_header = shipyard.args.convert_to_boolean(args.file_header)
        query = text(args.query)

        client = SqlServerClient(
            user=args.username,
            pwd=args.password,
            host=args.host,
            database=args.database,
            port=args.port,
            url_params=args.url_parameters,
        )
        client.connect()
        logger.info("Successfully connected to SQL Server")

        data = client.fetch(query)
        logger.info("Successfully fetched query results")

        data.to_csv(dest_path, index=False, header=file_header)

        logger.info(f"Successfully stored query results to {dest_path}")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to fetch data from SQL Server. Message from the server reads: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)

    finally:
        client.close()


if __name__ == "__main__":
    main()
