import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text, TextClause
from shipyard_templates import Database, ShipyardLogger

from shipyard_sqlserver.errors.exceptions import (
    FetchError,
    QueryError,
    SqlServerConnectionError,
    UploadError,
)

logger = ShipyardLogger.get_logger()


class SqlServerClient(Database):
    def __init__(
        self,
        user: str,
        pwd: str,
        host: str = None,
        database: str = None,
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
            self.connect()
        return self._conn

    def connect(self):
        try:
            connection_string = f"mssql+pyodbc://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&{self.url_params}"
            logger.debug("Successfully connected to SQL Server")
            self._conn = create_engine(
                connection_string, fast_executemany=True
            ).connect()
        except Exception as e:
            raise SqlServerConnectionError(e)

    def close(self):
        if self.conn:
            logger.debug("Closing connection")
            self.conn.close()

    def execute_query(self, query: TextClause):
        try:
            self.conn.execute(query)
            logger.debug("executed query")
        except Exception as e:
            raise QueryError(e)

    def fetch(self, query: TextClause) -> pd.DataFrame:
        try:
            df = pd.read_sql(sql=query, con=self.conn)
            logger.debug("Successfully fetched results")
        except Exception as e:
            raise FetchError(e)
        else:
            return df

    def upload(self, df: pd.DataFrame, table_name: str, insert_method: str = "replace"):
        try:
            df.to_sql(table_name, con=self.conn, index=False, if_exists=insert_method)
            logger.debug(f"Successfully loaded data to to {table_name}")
        except Exception as e:
            raise UploadError(table_name, e)
