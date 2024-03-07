import argparse
import os
import sys
from sqlalchemy import create_engine, text
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
    parser.add_argument("--query", dest="query", required=True)
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

        client.execute_query(query)
        logger.info("Successfully executed query")

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
