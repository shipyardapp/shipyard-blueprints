import argparse
import sys
import ast
import shipyard_snowflake as ss  # this is to reference the utility functions provided in the package
import snowflake.connector
import re
from shipyard_snowflake import SnowflakeClient
from shipyard_templates import ExitCodeException
from shipyard_bp_utils import files as shipyard


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--private-key-path", dest="private_key_path", required=False)
    parser.add_argument("--account", dest="account", required=True)
    parser.add_argument("--warehouse", dest="warehouse", required=False)
    parser.add_argument("--database", dest="database", required=False)
    parser.add_argument("--schema", dest="schema", default=None, required=False)
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match"},
        required=False,
    )
    parser.add_argument(
        "--source-file-name",
        dest="source_file_name",
        default="output.csv",
        required=True,
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--table-name", dest="table_name", default=None, required=True)
    parser.add_argument(
        "--insert-method",
        dest="insert_method",
        choices={"fail", "replace", "append"},
        default="append",
        required=False,
    )
    parser.add_argument(
        "--snowflake-data-types",
        dest="snowflake_data_types",
        required=False,
        default="",
    )
    parser.add_argument("--user-role", dest="user_role", required=False, default="")
    args = parser.parse_args()

    return args


def upload_multiple(
    client: SnowflakeClient,
    conn: snowflake.connector.connection.SnowflakeConnection,
    files: list,
    table_name: str,
    insert_method: str,
    pandas_dtypes: dict = None,
):
    """Uploads multiple files to a snowflake table

    Args:
        client (SnowflakeClient): The snowflake client object
        insert_method (str): The method to use when inserting the data into the table. Options are replace or append. Defaults to 'append'
        index (int, optional): The index of the file to upload. Defaults to 0.
    """
    total_files = len(files)
    for i, file_match in enumerate(files, start=1):
        client.logger.info(f"Uploading file {i} of {total_files}")
        try:
            df = ss.read_file(file_match, pandas_dtypes)

        except ExitCodeException(
            f"Error in reading file {i}", client.EXIT_CODE_FILE_NOT_FOUND
        ) as e:
            sys.exit(e.exit_code)

        try:
            client.upload(conn, df, table_name, insert_method)
        except ExitCodeException(
            f"Error in uploading file {i}", client.EXIT_CODE_INVALID_UPLOAD_VALUE
        ) as e:
            sys.exit(e.exit_code)
        if insert_method != "append":
            insert_method = "append"


def main():
    args = get_args()
    client = SnowflakeClient(
        username=args.username,
        pwd=args.password,
        account=args.account,
        rsa_key=args.private_key_path,
        warehouse=args.warehouse,
        schema=args.schema,
        database=args.database,
        role=args.user_role,
    )

    if (not client.username and not client.pwd) and (
        not client.username and not client.rsa_key
    ):
        client.logger.error(
            "Error: Either a username and password must be provided, or a username and private key file"
        )
        sys.exit(client.EXIT_CODE_INVALID_CREDENTIALS)
    conn = client.connect()  # establish connection to snowflake
    if args.snowflake_data_types:
        snowflake_data_types = ast.literal_eval(args.snowflake_data_types)
    else:
        snowflake_data_types = None

    if args.source_file_name_match_type == "regex_match":
        # match files based on pattern
        file_names = shipyard.find_all_local_file_names(args.source_folder_name)
        matching_file_names = shipyard.find_all_file_matches(
            file_names, re.compile(args.source_file_name)
        )
        client.logger.info(
            f"{len(matching_file_names)} files found. Preparing to upload..."
        )
    else:
        fp = shipyard.combine_folder_and_file_name(
            args.source_folder_name, args.source_file_name
        )
        df = ss.read_file(fp, snowflake_data_types)
        success, nchunks, nrows, output = client.upload(
            conn, df, table_name=args.table_name, if_exists=args.insert_method
        )
        client.logger.info(f"Uploaded {nrows} rows to {args.table_name}")


if __name__ == "__main__":
    main()
