import pyodbc
import pandas as pd
from sqlalchemy import create_engine, TextClause
from shipyard_templates import Database, ShipyardLogger
from shipyard_templates.database import QueryError, FetchError, UploadError
from typing import Optional

from shipyard_sqlserver.exceptions import SqlServerConnectionError

logger = ShipyardLogger.get_logger()


class SqlServerClient(Database):
    def __init__(
        self,
        user: str,
        pwd: str,
        host: str,
        database: Optional[str] = None,
        port: int = 1433,
        url_params=None,
    ) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.database = database
        self.port = port
        self.url_params = url_params
        self._conn = None
        super().__init__(
            user, pwd, host=host, database=database, port=port, url_params=url_params
        )

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.connect()
        return self._conn

    def connect(self):
        """
        Connects to the SQL Server database using the provided credentials.

        Raises:
            SqlServerConnectionError: If an error occurs while connecting to the database.

        Returns:
            None
        """
        try:
            connection_string = f"mssql+pyodbc://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&{self.url_params}"
            engine = create_engine(connection_string, fast_executemany=True).connect()
        except Exception as e:
            raise SqlServerConnectionError(e)
        else:
            logger.info("Successfully connected to SQL Server")
            return engine

    def close(self):
        """
        Closes the database connection if it is open.

        This method checks if the connection is open and then closes it. It also logs a debug message indicating that the connection is being closed.
        """
        if self.conn:
            logger.debug("Closing connection")
            self.conn.close()

    def execute_query(self, query: TextClause):
        """
        Executes the given SQL query.

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

    def fetch(self, query: TextClause) -> pd.DataFrame:
        """
        Fetches data from the SQL server using the provided query.

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

    def upload(
        self,
        df: pd.DataFrame,
        table_name: str,
        insert_method: Optional[str] = "replace",
    ):
        """
        Uploads a pandas DataFrame to a SQL Server table.

        Args:
            df (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the SQL Server table.
            insert_method (str, optional): The method to use when inserting the data into the table.
                Defaults to "replace", which replaces the existing table if it already exists.

        Raises:
            UploadError: If an error occurs during the upload process.

        Returns:
            None
        """
        try:
            df.to_sql(table_name, con=self.conn, index=False, if_exists=insert_method)
            logger.debug(f"Successfully loaded data to {table_name}")
        except Exception as e:
            raise UploadError(table_name, e)

    def download_chunks(self, query: TextClause, dest_path: str, header: bool = True):
        chunksize = 10_000
        first_write = False
        for chunk in pd.read_sql_query(query, self.conn, chunksize=chunksize):
            if not first_write:
                chunk.to_csv(dest_path, mode="w", header=header, index=False)
                first_write = True
            else:
                chunk.to_csv(dest_path, mode="a", header=False, index=False)
