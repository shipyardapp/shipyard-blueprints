import argparse
import os
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
    parser.add_argument(
        "--storage-credential", dest="storage_credential", required=True
    )
    parser.add_argument("--table-name", dest="table_name", required=True)
    parser.add_argument("--s3-bucket-path", dest="s3_bucket_path", required=True)
    parser.add_argument("--file-name", dest="file_name", required=True)
    parser.add_argument("--folder-name", dest="folder_name", required=False)
    parser.add_argument(
        "--insert-method",
        dest="insert_method",
        required=True,
        choices={"replace", "append"},
    )
    parser.add_argument(
        "--file-type",
        dest="file_type",
        required=False,
        default="csv",
        choices={"csv", "parquet"},
    )

    return parser.parse_args()


def main():
    args = get_args()
    catalog = args.catalog if args.catalog != "" else None
    schema = args.schema if args.schema != "" else None
    storage_credential = args.storage_credential
    bucket_path = args.s3_bucket_path
    insert_method = args.insert_method
    file_type = str(
        args.file_type
    ).upper()  # uppercase so that it can be interpolated in the COPY INTO QUERY
    table_name = args.table_name
    folder_name = args.folder_name if args.folder_name != "" else None

    # if the insert method is append, create a temp table, copy into temp table, insert into target table from temp table, drop temp table
    if insert_method == "append":
        pass

    try:
        if folder_name:
            full_path = os.path.join(folder_name, args.file_name)
        else:
            full_path = args.file_name

        s3_path = os.path.join(bucket_path, full_path)

        client = DatabricksSqlClient(
            server_host=args.server_host,
            http_path=args.http_path,
            access_token=args.access_token,
            catalog=catalog,
            schema=schema,
        )
        if insert_method == "append":
            # create the temp table
            tmp_table_name = f"tmp_{table_name}"
            create_sql = f"CREATE TABLE IF NOT EXISTS {tmp_table_name}"
            client.execute_query(create_sql)
            # copy data into tmp table
            copy_query = f"""COPY INTO {table_name} FROM '{s3_path}' WITH (CREDENTIAL `{storage_credential}`)
            FILEFORMAT = {file_type}
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true')
            """
            client.execute_query(copy_query)
            # insert the data from the temp table
            insert_sql = (
                f"INSERT INTO {table_name} FROM (SELECT * FROM {tmp_table_name})"
            )
            client.execute_query(insert_sql)

            # drop the tmp table
            drop_query = f"DROP TABLE {tmp_table_name}"
            client.execute_query(drop_query)

        else:
            query = f"""COPY INTO {table_name} FROM '{s3_path}' WITH (CREDENTIAL `{storage_credential}`)
            FILEFORMAT = {file_type}
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true')
            """
            client.execute_query(query)
    except ExitCodeException as ec:
        client.logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        client.logger.error(f"Error in attempting to execute query: {str(e)}")
        sys.exit(client.EXIT_CODE_UNKNOWN_ERROR)

    else:
        client.logger.info(f"Successfully copied data from {bucket_path} to Databricks")


if __name__ == "__main__":
    main()
