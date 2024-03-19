import pandas as pd
import os
from shipyard_templates import Database, ExitCodeException, ShipyardLogger
from shipyard_templates.database import FetchError, UploadError, QueryError
from shipyard_postgresql.errors.exceptions import ChunkDownloadError, InvalidCredentials
from sqlalchemy import create_engine, TextClause
from typing import Optional

logger = ShipyardLogger.get_logger()


class PostgresClient(Database):
    CHUNKSIZE = 10_000
    MAX_FILE_SIZE = 50_000_000

    def __init__(
        self,
        user: str,
        pwd: str,
        host: str,
        database: str,
        port: int = 5432,
        schema: Optional[str] = None,
        url_params=None,
    ) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.database = database
        self.url_params = url_params
        self.schema = schema
        self._conn = None
        super().__init__(
            user,
            pwd,
            host=host,
            port=port,
            database=database,
            url_params=url_params,
            schema=schema,
        )

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.connect()
        return self._conn

    def connect(self):
        try:
            con_str = f"postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_params}"
            engine = create_engine(con_str).connect()
            logger.info("Successfully connected to Postgres")
        except Exception as e:
            raise InvalidCredentials(e)
        else:
            return engine

    def execute_query(self, query: TextClause):
        try:
            self.conn.execute(query)
            logger.debug("Executed query")
            self.conn.commit()
            logger.debug("Committing transaction")
        except Exception as e:
            raise QueryError(e)

    def upload(self, file: str, table_name: str, insert_method: str = "replace"):
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
            raise

    def fetch(self, query: TextClause) -> pd.DataFrame:
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
        try:
            df.to_sql(
                table_name,
                con=self.conn,
                index=False,
                if_exists=insert_method,
                schema=self.schema,
                method="multi",
            )
        except Exception as e:
            raise UploadError(table=table_name, error_msg=e)

    def upload_file(
        self, file_path: str, table_name: str, insert_method: str = "replace"
    ):
        try:
            for index, chunk in enumerate(
                pd.read_csv(file_path, chunksize=self.CHUNKSIZE)
            ):
                chunk.to_sql(
                    table_name,
                    con=self.conn,
                    index=False,
                    schema=self.schema,
                    if_exists=insert_method,
                    method="multi",
                )
                if insert_method == "replace":
                    insert_method = "append"
        except Exception as e:
            raise UploadError(table=table_name, error_msg=e)

    def read_chunks(self, query: TextClause, dest_path: str, header: bool = True):
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
            raise ChunkDownloadError(e)

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Connection closed")
