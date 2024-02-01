from pytest import ExitCode
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
        """Helper function for authentication tests to see if provided credentials are valid"""
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
                self.conn = con
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
                self.conn = con
                return con
            except Exception as e:
                raise ExitCodeException(
                    f"Could not authenticate to Snowflake due to {e}",
                    self.EXIT_CODE_INVALID_CREDENTIALS,
                )

    def upload(
        self,
        file_path: str,
        table_name: str,
        insert_method: str = "replace",
    ):
        """Uploads a pandas dataframe to a snowflake table

        Args:
            conn (snowflake.connector.SnowflakeConnection): The snowflake connection object
            df (pd.DataFrame): The dataframe to be uploaded
            table_name (str): The name of the Snowflake Table to, if it doesn't exist, it will be created
            insert_method (str): The method to use when inserting the data into the table. Options are replace or append Defaults to 'replace'
        """
        if insert_method not in ["replace", "append"]:
            raise ExitCodeException(
                f"Invalid insert method: {insert_method} is not a valid insert method. Choose between 'replace' or 'append'",
                self.EXIT_CODE_INVALID_ARGUMENTS,
            )

        try:
            self.put(file_path=file_path, table_name=table_name)
            self.copy_into(table_name=table_name, insert_method=insert_method)
        except ExitCodeException as ec:
            self.logger.error("Error in uploading file")
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            self.logger.error(f"Unknown error in uploading file: {str(e)}")
            raise ExitCodeException(str(e), self.EXIT_CODE_INVALID_UPLOAD_VALUE)

        # if insert_method == "replace":
        #     self.logger.info("Uploading data to Snowflake via replace")
        #     self.execute_query(self.conn, f"DROP TABLE IF EXISTS {table_name}")
        #     success, nchunks, nrows, output = pt.write_pandas(
        #         conn=self.conn, df=df, table_name=table_name, auto_create_table=True
        #     )
        # else:
        #     self.logger.info("Uploading data to Snowflake via append")
        #     success, nchunks, nrows, output = pt.write_pandas(
        #         conn=self.conn, df=df, table_name=table_name
        #     )
        # self.logger.info("Successfully uploaded data to Snowflake")
        # return success, nchunks, nrows, output

    def execute_query(
        self, query: str, message: Optional[str] = None
    ) -> snowflake.connector.cursor.SnowflakeCursor:
        try:
            cursor = self.conn.cursor()
            res = cursor.execute(query)
            self.logger.debug(f"Successfully executed query: `{query}` in Snowflake")
            return res
        except Exception as e:
            if not message:
                self.logger.error("Could not execute the provided query in Snowflake")
            else:
                self.logger.error(message)
            raise ExitCodeException(e, self.EXIT_CODE_INVALID_QUERY)

    def fetch(self, query: str) -> pd.DataFrame:
        cursor = self.execute_query(query)
        results = cursor.fetchall()
        return pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])

    def put(
        self,
        file_path: str,
        table_name: str,
    ):
        """Executes a PUT command to load a file to internal staging. This is the fastest way to load a large file and should be followed by a copy into command

        Args:
            file_path: The file to load
            table_name: The table in Snowflake to write to
        """
        put_statement = f"""PUT file://{file_path} '@%{table_name}' OVERWRITE=TRUE """
        self.execute_query(put_statement, "Could not execute PUT statement")

    def copy_into(self, table_name: str, insert_method: str):
        """
        Executes a COPY INTO command to load a file from internal staging to a table

        Args:
            table_name: The name of the destination table to copy into
            insert_method: Whether th replace or append to the table
        """
        copy_statement = f"""COPY INTO {table_name} FROM '@%{table_name}' PURGE=TRUE FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' COMPRESSION=GZIP, PARSE_HEADER=TRUE) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"""
        if insert_method == "append":
            copy_statement += f" ON_ERROR = CONTINUE"
        self.execute_query(copy_statement, "Could not copy into")

    def _create_table_sql(
        self,
        table_name: str,
        columns: Union[List[List[str]], Dict[str, str]],
    ) -> str:
        """Returns the SQL for to create or replace a table in Snowflake
        Args:
            table_name (str): The name of the table to create
            columns (List[List[str,str]]): A list of lists of the column name and the data type. Example: [("column1","varchar(100)"),("column2","varchar(100)")]. Defaults to None
        """
        # this is for backwards compatibility
        if isinstance(columns, list):
            column_string = ",".join([f"{col[0]} {col[1]}" for col in columns])
        else:
            column_string = ",".join([f"{k} {v}" for k, v in columns.items()])

        create_statement = f"""CREATE OR REPLACE TABLE {table_name} ({column_string})"""
        return create_statement

    def create_table(self, sql: str):
        try:
            self.execute_query(query=sql)
        except Exception as e:
            raise ExitCodeException(
                "Error in creating table", exit_code=self.EXIT_CODE_INVALID_QUERY
            )

    def _exists(self, table_name: str) -> bool:
        """Helper function to check if a given table exists

        Args:
            table_name (): The name of the table

        Returns: True if exists, False if not
        """
        query = f"""SELECT * FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = {str(table_name).upper()}
        """
        if self.database:
            query += f" AND TABLE_CATALOG = {str(self.database).upper()}"
        if self.schema:
            query += f" AND TABLE_SCHEMA = {str(self.schema).upper()}"

        res = self.fetch(query)

        if res.shape[0] == 1:
            return True

        return False
