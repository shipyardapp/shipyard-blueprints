import argparse
import sys
from shipyard_templates import ExitCodeException, ShipyardLogger, Database
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
    args = get_args()
    query = args.query
    redshift_args = {
        "host": args.host,
        "user": args.username,
        "pwd": args.password,
        "database": args.database,
        "port": args.port,
        "url_params": args.url_parameters if args.url_parameters != "" else None,
    }

    redshift = RedshiftClient(**redshift_args)
    try:
        redshift.execute_query(query)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"An unepxected error occurred when attempting to execute query in Redshift. Message from the server reads {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)
    finally:
        redshift.close()


if __name__ == "__main__":
    main()
