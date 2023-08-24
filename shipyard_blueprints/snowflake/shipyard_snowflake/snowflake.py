import snowflake.connector
import pandas as pd
from dask import dataframe as dd
from typing import Dict, List, Optional, Tuple, Union
from shipyard_templates import Database, ExitCodeException
from snowflake.connector import pandas_tools as pt
from .utils import (
    _decode_rsa,
    _file_fits_in_memory,
    _get_file_size,
    _get_memory,
    map_snowflake_to_pandas,
    read_file,
)


class SnowflakeClient(Database):
    def __init__(
        self,
        username,
        pwd,
        database=None,
        account=None,
        warehouse=None,
        schema=None,
        rsa_key=None,
        role=None,
    ) -> None:
        self.username = username
        self.pwd = pwd
        self.account = account
        self.warehouse = warehouse
        self.schema = schema
        self.database = database
        self.rsa_key = rsa_key
        self.role = role
        super().__init__(
            username,
            pwd,
            account=account,
            warehouse=warehouse,
            schema=schema,
            database=database,
            rsa_key=rsa_key,
        )

    def connect(self):
        """Helper function for authentication tests to see if provided credentials are valid

        Returns: 0 for success, 1 for error

        """
        if self.rsa_key:
            if self.pwd:
                self.logger.warning(
                    "Private Key was provided in addition to Password. Using the Private Key to login"
                )
            private_key = _decode_rsa(self.rsa_key)
            try:
                con = snowflake.connector.connect(
                    user=self.username,
                    account=self.account,
                    private_key=private_key,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema,
                    role=self.role,
                )
                self.logger.info("Successfully connected to Snowflake")
                return con
            except Exception as e:
                raise ExitCodeException(
                    message=f"Could not authenticate to Snowflake for user {self.username} with credentials in {self.rsa_key}",
                    exit_code=self.EXIT_CODE_INVALID_CREDENTIALS,
                )
        else:
            try:
                con = snowflake.connector.connect(
                    user=self.username,
                    password=self.pwd,
                    account=self.account,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema,
                    role=self.role,
                )
                self.logger.info("Successfully connected to snowflake")
                return con
            except Exception as e:
                raise ExitCodeException(
                    f"Could not authenticate to Snowflake due to {e}",
                    self.EXIT_CODE_INVALID_CREDENTIALS,
                )

    def upload(
        self,
        conn: snowflake.connector.SnowflakeConnection,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
    ):
        """Uploads a pandas dataframe to a snowflake table

        Args:
            conn (snowflake.connector.SnowflakeConnection): The snowflake connection object
            df (pd.DataFrame): The dataframe to be uploaded
            table_name (str): The name of the Snowflake Table to, if it doesn't exist, it will be created
            if_exists (str): The method to use when inserting the data into the table. Options are replace or append Defaults to 'replace'

        Returns:
            _type_: A tuple of the success of the upload, the number of chunks, the number of rows, and the output
        """
        if if_exists not in ["replace", "append"]:
            raise ExitCodeException(
                f"Invalid insert method: {if_exists} is not a valid insert method. Choose between 'replace' or 'append'",
                self.EXIT_CODE_INVALID_ARGUMENTS,
            )

        if if_exists == "replace":
            self.logger.info("Uploading data to Snowflake via replace")
            self.execute_query(conn, f"DROP TABLE IF EXISTS {table_name}")
            success, nchunks, nrows, output = pt.write_pandas(
                conn=conn, df=df, table_name=table_name, auto_create_table=True
            )
        else:
            self.logger.info("Uploading data to Snowflake via append")
            success, nchunks, nrows, output = pt.write_pandas(
                conn=conn, df=df, table_name=table_name
            )
        self.logger.info("Successfully uploaded data to Snowflake")
        return success, nchunks, nrows, output

    def execute_query(
        self, conn: snowflake.connector.SnowflakeConnection, query: str
    ) -> snowflake.connector.cursor.SnowflakeCursor:
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            self.logger.info(f"Successfully executed query: {query} in Snowflake")
            return cursor
        except Exception as e:
            self.logger.error("Could not execute the provided query in Snowflake")
            raise ExitCodeException(e, self.EXIT_CODE_INVALID_QUERY)

    def fetch(
        self, conn: snowflake.connector.SnowflakeConnection, query: str
    ) -> pd.DataFrame:
        cursor = self.execute_query(conn, query)
        results = cursor.fetchall()
        return pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])

    def put(
        self,
        conn: snowflake.connector.SnowflakeConnection,
        file_path: str,
        table_name: str,
    ):
        """Executes a PUT command to load a file to internal staging. This is the fastest way to load a large file and should be followed by a copy into command

        Args:
            conn: The established Snowflake Connection
            file: The file to load
            table_name: The table in Snowflake to write to
        """
        put_statement = f"""PUT file://{file_path}* '@%"{table_name}"' """
        self.execute_query(conn, put_statement)

    def copy_into(self, conn: snowflake.connector.SnowflakeConnection, table_name: str):
        """Executes a COPY INTO command to load a file from internal staging to a table"""
        copy_statement = f"""COPY INTO "{table_name}" FROM '@%\"{table_name}\"' PURGE=TRUE FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' COMPRESSION=GZIP, PARSE_HEADER=TRUE) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"""
        self.execute_query(conn, copy_statement)

    def _create_table(
        self,
        table_name: str,
        columns=None,
    ) -> str:
        """Returns the SQL for to create or replace a table in Snowflake
        Args:
            conn (snowflake.connector.SnowflakeConnection): The snowflake connection object
            table_name (str): The name of the table to create
            columns (List[List[str,str]]): A list of lists of the column name and the data type. Example: [("column1","varchar(100)"),("column2","varchar(100)")]. Defaults to None
        """
        column_string = ",".join([f"{col[0]} {col[1]}" for col in columns])
        create_statement = f"""CREATE OR REPLACE TABLE {table_name} ({column_string})"""
        return create_statement
