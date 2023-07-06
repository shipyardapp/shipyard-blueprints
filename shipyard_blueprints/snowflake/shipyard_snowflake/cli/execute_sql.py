import argparse
import sys
from shipyard_snowflake import SnowflakeClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=True)
    parser.add_argument("--password", dest="password", required=False, default="")
    parser.add_argument(
        "--private-key-path", dest="private_key_path", required=False, default=""
    )
    parser.add_argument(
        "--private-key-passphrase",
        dest="private_key_passphrase",
        required=False,
        default="",
    )
    parser.add_argument("--account", dest="account", required=True)
    parser.add_argument("--warehouse", dest="warehouse", required=False)
    parser.add_argument("--database", dest="database", required=True)
    parser.add_argument("--schema", dest="schema", required=False, default="")
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
        "schema": None if args.schema == "" else args.schema,
        "database": args.database,
        "rsa_key": None if args.private_key_path == "" else args.private_key_path,
        "role": None if args.user_role == "" else args.user_role,
    }
    private_key_passphrase = (
        None if args.private_key_passphrase == "" else args.private_key_passphrase
    )
    client = SnowflakeClient(**client_args)
    if client.rsa_key and not private_key_passphrase:
        client.logger.error(
            "Private key passphrase is required when using a private key"
        )
        sys.exit(client.EXIT_CODE_INVALID_CREDENTIALS)
    conn = client.connect()
    if conn == 1:
        sys.exit(client.EXIT_CODE_INVALID_CREDENTIALS)
    try:
        client.execute_query(conn=conn, query=args.query)
    except ExitCodeException as e:
        client.logger.error(e)
        sys.exit(client.EXIT_CODE_INVALID_QUERY)


if __name__ == "__main__":
    main()
