import pandas as pd
import os
from sqlalchemy import create_engine
from shipyard_templates import Database, ExitCodeException, ShipyardLogger
from shipyard_templates.database import (
    FetchError,
    UploadError,
    QueryError,
    ConnectionError,
)
from sqlalchemy import create_engine, TextClause
from typing import Optional

logger = ShipyardLogger.get_logger()


class MySqlClient(Database):
    CHUNKSIZE = 10_000
    MAX_FILE_SIZE = 50_000_000  # about 50 MB

    def __init__(
        self,
        username: str,
        pwd: str,
        host: str,
        database: str,
        port: int = 3306,
        url_params: Optional[str] = None,
    ) -> None:
        self.username = username
        self.pwd = pwd
        self.host = host
        self.port = port
        self.database = database
        self.url_params = url_params
        self._conn = None
        super().__init__(
            username=username,
            pwd=pwd,
            host=host,
            database=database,
            port=port,
            url_params=url_params,
        )

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.connect()
        return self._conn

    def connect(self):
        con_str = f"mysql+mysqlconnector://{self.username}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_params}"
        try:
            conn = create_engine(con_str).connect()
        except Exception as e:
            raise ConnectionError(
                f"Error connecting to MySQL. Message from the server reads: {e}"
            )
        else:
            logger.info("Successfully connected to MySQL")
            return conn

    def execute_query(self, query: TextClause):
        """
        Executes the given SQL query on the database connection.

        Args:
            query (TextClause): The SQL query to execute.

        Raises:
            QueryError: If an error occurs while executing the query.

        Returns:
            None
        """
        try:
            self.conn.execute(query)
            logger.debug("Executed query")
            self.conn.commit()
            logger.debug("Committing transaction")
        except Exception as e:
            raise QueryError(e)

    def upload(self, file: str, table_name: str, insert_method: str = "replace"):
        """
        Uploads data from a file to a PostgreSQL table.

        Args:
            file (str): The path to the file containing the data to be uploaded.
            table_name (str): The name of the PostgreSQL table to upload the data to.
            insert_method (str, optional): The method to use for inserting the data into the table.
                Defaults to "replace".

        Raises:
            ExitCodeException: If an exit code exception occurs during the upload process.
            Exception: If any other exception occurs during the upload process.
        """
        try:
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

    def fetch(self, query: TextClause) -> pd.DataFrame:
        """
        Fetches data from the database using the provided SQL query.

        Args:
            query (TextClause): The SQL query to execute.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the fetched results.

        Raises:
            FetchError: If an error occurs while fetching the results.
        """
        try:
            df = pd.read_sql(sql=query, con=self.conn)
            logger.debug("Successfully fetched results")
        except Exception as e:
            raise FetchError(e)
        else:
            return df

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
            for index, chunk in enumerate(
                pd.read_csv(file_path, chunksize=self.CHUNKSIZE)
            ):
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

    def read_chunks(self, query: TextClause, dest_path: str, header: bool = True):
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

    def close(self):
        """
        Closes the database connection if it is open.

        This function closes the connection to the PostgreSQL database if it is currently open.
        If the connection is closed successfully, it logs a message indicating that the connection has been closed.

        """
        if self.conn:
            self.conn.close()
            logger.info("Connection closed")
