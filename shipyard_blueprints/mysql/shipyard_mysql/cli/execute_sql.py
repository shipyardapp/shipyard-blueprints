import argparse
import os
import sys
from shipyard_mysql import MySqlClient
from shipyard_templates import ExitCodeException, Database, ShipyardLogger
from sqlalchemy import text

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--host", dest="host", required=False)
    parser.add_argument("--database", dest="database", required=False)
    parser.add_argument("--port", dest="port", default="3306", required=False)
    parser.add_argument("--url-parameters", dest="url_parameters", required=False)
    parser.add_argument("--query", dest="query", required=True)
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
        query = text(args.query)

        # client args
        client_args = {
            "username": args.username,
            "pwd": args.password,
            "host": args.host,
            "database": args.database,
            "port": args.port,
            "url_params": args.url_parameters if args.url_parameters != "" else None,
        }

        mysql = MySqlClient(**client_args)

        mysql.execute_query(query)

        logger.info("Successfully executed query")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to execute query against MySQL. Message from the server reads: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)

    finally:
        mysql.close()


if __name__ == "__main__":
    main()
