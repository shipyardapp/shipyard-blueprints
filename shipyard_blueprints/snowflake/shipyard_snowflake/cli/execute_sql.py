import argparse
import sys
from shipyard_snowflake import SnowflakeClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=True)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--private-key-path", dest="private_key_path", required=False)
    parser.add_argument("--account", dest="account", required=True)
    parser.add_argument("--warehouse", dest="warehouse", required=False)
    parser.add_argument("--database", dest="database", required=True)
    parser.add_argument("--schema", dest="schema", required=False)
    parser.add_argument("--query", dest="query", required=True)
    parser.add_argument("--user-role", dest="user_role", required=False, default="")
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    client_args = {
        "username": args.username,
        "pwd": None if args.password == "" else args.password,
        "account": None if args.account == "" else args.account,
        "warehouse": args.warehouse,
        "schema": args.schema,
        "database": args.database,
        "rsa_key": None if args.private_key_path == "" else args.private_key_path,
        "role": None if args.user_role == "" else args.user_role,
    }
    client = SnowflakeClient(**client_args)
    conn = client.connect()
    try:
        client.execute_query(conn=conn, query=args.query)
    except ExitCodeException as e:
        client.logger.error(e)
        sys.exit(client.EXIT_CODE_INVALID_QUERY)


if __name__ == "__main__":
    main()
