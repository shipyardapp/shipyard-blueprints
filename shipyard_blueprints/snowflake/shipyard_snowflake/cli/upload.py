import argparse
import sys
import ast
import re
import json
from shipyard_snowflake import SnowflakeClient, utils
from shipyard_templates import ExitCodeException
from shipyard_bp_utils import files as shipyard
from typing import Optional, Dict


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
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
    parser.add_argument("--warehouse", dest="warehouse", required=False, default="")
    parser.add_argument("--database", dest="database", required=False, default="")
    parser.add_argument("--schema", dest="schema", required=False, default="")
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
    return parser.parse_args()


def create_if_not_exists(
    client: SnowflakeClient, table_name: str, snowflake_data_types: Dict[str, str]
):
    """Helper function to create the table if it doesn't already exist. This is only necessary when the
    append method is used

    Args:
        client: The snowflake client
        table_name: The name of the table
        snowflake_data_types: The optional snowflake datatypes to be used to create the table
    """
    if not client._exists(table_name):
        client.logger.info(f"Creating new table {table_name}")
        create_statement = client._create_table_sql(
            table_name=table_name, columns=snowflake_data_types
        )
        client.create_table(create_statement)
    else:
        client.logger.info("Table exists, beginning append job")


def upload_multiple(
    client: SnowflakeClient,
    files: list,
    table_name: str,
    insert_method: str,
    pandas_dtypes: Optional[Dict[str, str]] = None,
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
            client.upload(file_match, table_name, insert_method)
        except ExitCodeException as ec:
            client.logger.error(ec.message)
            sys.exit(ec.exit_code)


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
    snowflake_client = SnowflakeClient(**client_args)
    try:
        snowflake_client.connect()
    except ExitCodeException as ec:
        snowflake_client.logger.error(ec.message)
        sys.exit(ec.exit_code)

    # TODO: put this above the initialization of the client and adjust the logger to use the
    # shipyard logger from the newest version of shipyard-templates

    # private_key_passphrase = (
    #     None if args.private_key_passphrase == "" else args.private_key_passphrase
    # )
    #
    # if snowflake_client.rsa_key and not private_key_passphrase:
    #     snowflake_client.logger.error(
    #         "Error: A private key passphrase must be provided if using a private key"
    #     )
    #     sys.exit(snowflake_client.EXIT_CODE_INVALID_ARGUMENTS)
    # if not snowflake_client.pwd and not snowflake_client.rsa_key:
    #     snowflake_client.logger.error(
    #         "Error: Either a username and password must be provided, or a username and private key file"
    #     )
    #     sys.exit(snowflake_client.EXIT_CODE_INVALID_CREDENTIALS)

    try:
        if args.snowflake_data_types != "":
            snowflake_data_types = ast.literal_eval(args.snowflake_data_types)
        else:
            snowflake_data_types = None
    except Exception as e:
        try:
            snowflake_data_types = json.dumps(args.snowflake_data_types)
        except Exception as e:
            snowflake_client.logger.error(
                "Error in reading in datatypes. Ensure that the provided input is an array of arrays, or a JSON representation"
            )
            sys.exit(snowflake_client.EXIT_CODE_INVALID_ARGUMENTS)
        snowflake_client.logger.error(
            "Error in reading in datatypes. Ensure that the provided input is an array of arrays, or a JSON representation"
        )
        sys.exit(snowflake_client.EXIT_CODE_INVALID_ARGUMENTS)

    # for loading multiple files
    if args.source_file_name_match_type == "regex_match":
        # match files based on pattern
        file_names = shipyard.find_all_local_file_names(args.source_folder_name)
        matching_file_names = shipyard.find_all_file_matches(
            file_names, re.compile(args.source_file_name)
        )
        snowflake_client.logger.info(
            f"{len(matching_file_names)} files found. Preparing to upload..."
        )

        # infer the schema from the first file
        if not snowflake_data_types:
            pandas_datatypes = utils.infer_schema(
                matching_file_names[0]
            )  # take the first file
            snowflake_data_types = utils.map_pandas_to_snowflake(pandas_datatypes)

        # create the table if it doesn't exist
        if args.insert_method == "replace":
            create_table_sql = snowflake_client._create_table_sql(
                table_name=args.table_name, columns=snowflake_data_types
            )
            snowflake_client.create_table(create_table_sql)
        else:
            create_if_not_exists(
                client=snowflake_client,
                table_name=args.table_name,
                snowflake_data_types=snowflake_data_types,
            )

        # upload each file to the target
        # NOTE: This should be reworked to both PUT multiple files at once using a glob pattern, and COPY INTO multiple
        # files at once using the FILES() parameter
        for i, file_match in enumerate(matching_file_names, start=1):
            insert_method = args.insert_method
            snowflake_client.upload(
                file_path=file_match,
                table_name=args.table_name,
                insert_method=insert_method,
            )
            if i == 1:
                insert_method = "append"

    # for single file uploads
    else:
        try:
            fp = shipyard.combine_folder_and_file_name(
                args.source_folder_name, args.source_file_name
            )
            # if the datatypes are not provided, then infer them
            if not snowflake_data_types:
                pandas_datatypes = utils.infer_schema(fp)
                snowflake_data_types = utils.map_pandas_to_snowflake(pandas_datatypes)

            if args.insert_method == "replace":
                create_table_sql = snowflake_client._create_table_sql(
                    table_name=args.table_name, columns=snowflake_data_types
                )
                snowflake_client.create_table(create_table_sql)
            else:
                create_if_not_exists(
                    client=snowflake_client,
                    table_name=args.tabel_name,
                    snowflake_data_types=snowflake_data_types,
                )

            snowflake_client.upload(
                file_path=fp,
                table_name=args.table_name,
                insert_method=args.insert_method,
            )
        except ExitCodeException as ec:
            snowflake_client.logger.error(ec.message)
            sys.exit(ec.exit_code)
        except Exception as e:
            snowflake_client.logger.error(
                f"An error occurred when attempting to upload to Snowflake: {str(e)}"
            )
            # TODO: this should be an unknown error code
            sys.exit(snowflake_client.EXIT_CODE_INVALID_UPLOAD_VALUE)
        else:
            snowflake_client.logger.info(
                f"Successfully loaded file {args.source_file_name} to Snowflake"
            )


if __name__ == "__main__":
    main()
