import snowflake.connector
import pandas as pd
from dask import dataframe as dd
from typing import Dict, List, Optional, Tuple, Union
from shipyard_templates import Database, ExitCodeError, ExitCodeException
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
                self.logger.info(f"Successfully connected to Snowflake")
                return con
            except Exception as e:
                self.logger.error(
                    f"Could not authenticate to Snowflake for user {self.username} with credentials in {self.rsa_key}"
                )
                return 1
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
                self.logger.info(f"Successfully connected to snowflake")
                return con
            except Exception as e:
                self.logger.error(
                    f"Could not authenticate to account {self.account} for user {self.username}"
                )
                return 1  # failed connection

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
            insert_method (str): The method to use when inserting the data into the table. Options are replace or append Defaults to 'replace'

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
            self.logger.info("Successfully uploaded data to Snowflake")
            return success, nchunks, nrows, output
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
