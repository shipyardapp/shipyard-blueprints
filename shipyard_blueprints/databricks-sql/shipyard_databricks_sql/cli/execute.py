import argparse
import sys

from shipyard_templates import ExitCodeException
from shipyard_databricks_sql import DatabricksSqlClient


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--server-host", dest="server_host", required=True)
    parser.add_argument("--http-path", dest="http_path", required=True)
    parser.add_argument("--catalog", dest="catalog", required=False, default="")
    parser.add_argument("--schema", dest="schema", required=False, default="")
    parser.add_argument("--query", dest="query", required=True)
    return parser.parse_args()


def main():
    args = get_args()
    catalog = args.catalog if args.catalog != "" else None
    schema = args.schema if args.schema != "" else None

    client = DatabricksSqlClient(
        server_host=args.server_host,
        http_path=args.http_path,
        access_token=args.access_token,
        catalog=catalog,
        schema=schema,
    )

    try:
        client.execute_query(args.query)
    except ExitCodeException as ec:
        client.logger.error(f"ExitCodeException: Error in executing query {ec.message}")
        sys.exit(ec.exit_code)
    except Exception as e:
        client.logger.error(
            f"Error in attempting to execute query in Databricks:{str(e)}"
        )
        sys.exit(client.EXIT_CODE_INVALID_QUERY)

    finally:
        client.close()


if __name__ == "__main__":
    main()
