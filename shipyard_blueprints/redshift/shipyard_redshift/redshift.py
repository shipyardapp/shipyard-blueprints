import redshift_connector
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from shipyard_templates import Database, ShipyardLogger, ExitCodeException
from shipyard_templates.database import (
    UploadError,
    FetchError,
    QueryError,
    ConnectionError,
)
from typing import Optional

logger = ShipyardLogger.get_logger()


class RedshiftClient(Database):
    CHUNKSIZE = 10_000
    MAX_FILE_SIZE = 50_000_000

    def __init__(
        self,
        user: str,
        pwd: str,
        host: str,
        database: str,
        schema: Optional[str] = None,
        port: Optional[int] = 5439,
        url_params: Optional[str] = None,
    ) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.database = database
        self.schema = schema
        self.port = port
        self.url_params = url_params
        self._conn = None
        super().__init__(
            user, pwd, host=host, database=database, port=port, url_params=url_params
        )

    @property
    def conn(self):
        if not self._conn:
            self._conn = self.connect()
        return self._conn

    def connect(self):
        """
        Connects to the Redshift database using the provided credentials.

        Returns:
            sqlalchemy.engine.Connection: A connection object representing the connection to the Redshift database.

        Raises:
            ConnectionError: If there is an error connecting to the Redshift database.
        """
        try:
            con_str = URL.create(
                drivername="redshift+redshift_connector",
                host=self.host,
                password=self.pwd,
                username=self.user,
                port=self.port,
                database=self.database,
            )
            conn = create_engine(con_str)
        except Exception as e:
            logger.error("Error in connecting to Redshift")
            raise ConnectionError(
                f"Error connecting to Redshift. Message from the server reads: {e}"
            )
        else:
            logger.info("Successfully connected to Redshift")
            return conn.connect()

    def execute_query(self, query: str):
        """
        Executes the given query on the Redshift database.

        Args:
            query (str): The SQL query to be executed.

        Raises:
            QueryError: If an error occurs while executing the query.

        Returns:
            None
        """
        try:
            query_text = text(query)
            self.conn.execute(query_text)
        except Exception as e:
            raise QueryError(e)
        else:
            logger.info("Successfully executed query")

    def fetch(self, query: str) -> pd.DataFrame:
        """
        Executes the given SQL query and returns the results as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: The results of the query as a pandas DataFrame.

        Raises:
            FetchError: If an error occurs while fetching the results.
        """
        try:
            query_text = text(query)
            df = pd.read_sql(sql=query_text, con=self.conn)
            logger.debug("Successfully fetched results")
        except Exception as e:
            raise FetchError(e)
        else:
            return df

    def upload(self, file: str, table_name: str, insert_method: str = "replace"):
        """
        Uploads data from a file to a Redshift table.

        Args:
            file (str): The path to the file containing the data to be uploaded.
            table_name (str): The name of the Redshift table to upload the data to.
            insert_method (str, optional): The method to use for inserting the data into the table.
                Defaults to "replace".

        Raises:
            ExitCodeException: If an exit code exception occurs during the execution of the method.
            UploadError: If an error occurs during the upload process.

        """
        try:
            if self.schema:
                self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            if os.path.getsize(file) < self.MAX_FILE_SIZE:
                df = pd.read_csv(file)
                self.upload_df(df, table_name=table_name, insert_method=insert_method)
            else:
                self.upload_file(
                    file, table_name=table_name, insert_method=insert_method
                )
        except ExitCodeException:
            raise
        except Exception as e:
            raise UploadError(table=table_name, error_msg=e)

    def read_chunks(self, query: str, dest_path: str, header: bool = True):
        """
        Reads data from the database in chunks and saves it to a CSV file.

        Args:
            query (TextClause): The SQL query to execute.
            dest_path (str): The path to the destination CSV file.
            header (bool, optional): Whether to include a header row in the CSV file. Defaults to True.

        Raises:
            ChunkDownloadError: If an error occurs while downloading the chunks.

        Returns:
            None
        """
        try:
            chunksize = self.CHUNKSIZE
            first_write = False
            for chunk in pd.read_sql_query(query, self.conn, chunksize=chunksize):
                if not first_write:
                    chunk.to_csv(dest_path, mode="w", header=header, index=False)
                    first_write = True
                else:
                    chunk.to_csv(dest_path, mode="a", header=False, index=False)
        except Exception as e:
            raise FetchError(e)

    def upload_df(
        self, df: pd.DataFrame, table_name: str, insert_method: str = "replace"
    ):
        """
        Uploads a pandas DataFrame to a PostgreSQL table.

        Args:
            df (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the table in the PostgreSQL database.
            insert_method (str, optional): The method to use for inserting the data into the table.
                Defaults to "replace".

        Raises:
            UploadError: If an error occurs during the upload process.

        """
        try:
            df.to_sql(
                table_name,
                con=self.conn,
                index=False,
                if_exists=insert_method,
                method="multi",
            )
        except Exception as e:
            raise UploadError(table=table_name, error_msg=e)

    def upload_file(
        self, file_path: str, table_name: str, insert_method: str = "replace"
    ):
        """
        Uploads a file to a PostgreSQL table.

        Args:
            file_path (str): The path to the file to be uploaded.
            table_name (str): The name of the table to upload the file to.
            insert_method (str, optional): The method to use for inserting the data into the table.
                Defaults to "replace".

        Raises:
            UploadError: If an error occurs during the upload process.

        """
        try:
            for chunk in pd.read_csv(file_path, chunksize=self.CHUNKSIZE):
                chunk.to_sql(
                    table_name,
                    con=self.conn,
                    index=False,
                    if_exists=insert_method,
                    method="multi",
                )
                if insert_method == "replace":
                    insert_method = "append"
        except Exception as e:
            raise UploadError(table=table_name, error_msg=e)

    def close(self):
        """
        Closes the connection to the Redshift database.

        This method closes the connection to the Redshift database if it is open.
        """
        if self._conn is not None:
            logger.info("Closing connection")
            self._conn.close()
