import argparse
import shipyard_bp_utils as shipyard
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

    destination_file_name = args.destination_file_name
    destination_folder_name = shipyard.files.clean_folder_name(
        args.destination_folder_name
    )
    destination_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name
    )
    file_header = shipyard.args.convert_to_boolean(args.file_header)
    client = SnowflakeClient(**client_args)
    try:
        conn = client.connect()
    except ExitCodeException as e:
        if client_args["password"] != "":
            client.logger.error(
                f"Ensure the password is correct for user {client_args['username']}"
            )
        if client_args["rsa_key"] != "":
            client.logger.error(
                f"Ensure the private key is correct for user {client_args['username']}"
            )
        sys.exit(client.EXIT_CODE_INVALID_CREDENTIALS)
    df = client.fetch(conn, args.query)
    try:
        df.to_csv(destination_full_path, index=False, header=file_header)
        client.logger.info(f"Successfuly wrote file to {destination_full_path}")
    except ExitCodeException as e:
        client.logger.error(f"Error writing file to {destination_full_path}")
        client.logger.error(e)
        sys.exit(client.EXIT_CODE_NO_RESULTS)


if __name__ == "__main__":
    main()
