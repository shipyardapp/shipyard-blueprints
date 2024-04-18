import duckdb
from shipyard_templates import ShipyardLogger, Database, ExitCodeException
from shipyard_templates.database import (
    FetchError,
    UploadError,
    ConnectionError,
    QueryError,
)
from shipyard_motherduck.exceptions import (
    TempTableCreationFailed,
    TempTableInsertFailed,
    TempTableDropFailed,
)
from typing import Optional

logger = ShipyardLogger().get_logger()


class MotherDuckClient(Database):
    def __init__(self, token: str, database: Optional[str] = None):
        self.token = token
        self.database = database
        if database:
            self._conn_str = f"md:{database}?[motherduck_token={token}]"
        else:
            self._conn_str = f"md:?motherduck_token={token}"
        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = self.connect()
        return self._conn

    def connect(self):
        """
        Connects to the MotherDuck database.

        Returns:
            duckdb.Connection: The connection object for interacting with the MotherDuck database.

        Raises:
            ConnectionError: If there is an error connecting to the MotherDuck database.
        """
        try:
            conn = duckdb.connect(self._conn_str)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MotherDuck: {e}")
        else:
            logger.info("Successfully connected to MotherDuck")
            return conn

    def upload(self, table_name: str, file_path: str, insert_method: str = "replace"):
        """
        Uploads data from a file to a specified table in the database.

        Args:
            table_name (str): The name of the table to upload the data to.
            file_path (str): The path to the file containing the data to be uploaded.
            insert_method (str, optional): The method to use for inserting the data into the table.
                Valid options are 'replace' (default) and 'append'.

        Raises:
            ValueError: If an invalid insert method is provided.
            UploadError: If the upload fails.

        Returns:
            None
        """
        try:
            if insert_method == "replace":
                sql = f"CREATE OR REPLACE TABLE {table_name} as SELECT * FROM '{file_path}'"
                self.conn.sql(sql)
            elif insert_method == "append":
                temp_table = self._create_temp_table(table_name, file_path)
                self._insert(temp_table, table_name)
                logger.debug(
                    f"Successfully inserted data into {table_name} from {temp_table}"
                )
                self._drop(temp_table)
                logger.debug(f"Successfully dropped temp table {temp_table}")
            else:
                raise ValueError(f"Invalid insert method: {insert_method}")
        except ExitCodeException:
            raise
        except Exception as e:
            logger.error(f"Failed to upload {file_path} to MotherDuck")
            raise UploadError(table_name, e)
        else:
            logger.info(f"Successfully uploaded {file_path} to MotherDuck")

    def fetch(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Fetches data from MotherDuck using the provided query.

        Args:
            query (str): The SQL query to execute.

        Returns:
            duckdb.DuckDBPyRelation: The result of the query execution.

        Raises:
            FetchError: If there is an error while fetching the data.
        """
        try:
            result = self.conn.sql(query)

        except Exception as e:
            logger.error(f"Failed to fetch data from MotherDuck")
            raise FetchError(e)

        else:
            logger.info("Successfully fetched data from MotherDuck")
            return result

    def execute_query(self, query: str):
        """
        Executes the given SQL query on the database connection.

        Args:
            query (str): The SQL query to be executed.

        Raises:
            QueryError: If an error occurs while executing the query.

        Returns:
            None
        """
        try:
            self.conn.sql(query)
        except Exception as e:
            raise QueryError(e)
        else:
            logger.info("Successfully executed query")

    def close(self):
        """
        Closes the connection to MotherDuck.

        This method closes the connection to the MotherDuck server if it is open.
        """
        if self._conn:
            self._conn.close()
            logger.info("Closed connection to MotherDuck")

    def _create_temp_table(self, table_name: str, file_path: str):
        """
        Creates a temporary table in the specified database (if provided) or in the current database.

        Args:
            table_name (str): The name of the temporary table to be created.
            file_path (str): The path to the file from which the data will be loaded into the temporary table.

        Returns:
            str: The name of the created temporary table.
        """
        try:
            temp_table = f"temp_{table_name}"
            cmd = f"CREATE OR REPLACE TABLE {temp_table} as SELECT * FROM '{file_path}'"
            self.conn.sql(cmd)
            logger.debug(f"Successfully created temp_table {temp_table}")
        except Exception:
            raise TempTableCreationFailed(temp_table)
        else:
            return temp_table

    def _insert(
        self,
        src_table: str,
        target_table: str,
    ):
        """
        Inserts data from the source table into the target table.

        Args:
            src_table (str): The name of the source table.
            target_table (str): The name of the target table.

        Returns:
            None
        """
        try:
            if not self._exists(target_table):
                cmd = f"CREATE TABLE {target_table} AS SELECT * FROM '{src_table}'"
                self.conn.sql(cmd)
            else:
                cmd = f"INSERT INTO {target_table} SELECT * FROM '{src_table}'"
                self.conn.sql(cmd)
        except Exception:
            raise TempTableInsertFailed(src_table, target_table)

    def _drop(self, table_name: str):
        """
        Drops a table from the specified database.

        Args:
            table_name (str): The name of the table to be dropped.

        Returns:
            None
        """
        try:
            cmd = f"DROP TABLE {table_name}"
            self.conn.sql(cmd)
        except Exception:
            raise TempTableDropFailed(table_name)

    def _exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in the database.

        Args:
            table_name (str): The name of the table to check for existence.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            cmd = f"SELECT * FROM {table_name} LIMIT 1"
            self.conn.sql(cmd)
            return True
        except Exception:
            return False
