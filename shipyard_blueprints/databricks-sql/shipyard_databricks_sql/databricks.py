import pandas as pd
import os
from pathlib import Path
from databricks.sql.client import Connection
from shipyard_templates import DatabricksDatabase, ExitCodeException, ShipyardLogger
from databricks import sql
from databricks.sql.client import Connection  # for type hints
from typing import Optional, Dict, List, Any, Union
from shipyard_databricks_sql.utils import exceptions as errs
from shipyard_databricks_sql.utils.exceptions import (
    TableDNE,
    VolumeSqlError,
    VolumeUploadError,
    VolumeUploadError,
    CopyIntoError,
    RemoveVolumeError,
)

logger = ShipyardLogger.get_logger()


class DatabricksSqlClient(DatabricksDatabase):
    # EXIT_CODE_VOLUME_CREATION = 101
    # EXIT_CODE_VOLUME_SQL = 102
    # EXIT_CODE_VOLUME_UPLOAD_ERROR = 103
    # EXIT_CODE_COPY_INTO_ERROR = 104
    # EXIT_CODE_REMOVE_VOLUME_ERROR = 105
    # EXIT_CODE_SCHEMA_CREATION_ERROR = 106
    SPARK_TYPES = {
        "array": "numpy.ndarray",
        "bigint": "int",
        "binary": "bytearray",
        "boolean": "bool",
        "date": "datetime.date",
        "decimal": "decimal",
        "double": "float",
        "int": "int",
        "map": "str",
        "null": None,
        "smallint": "int",
        "string": "str",
        "struct": "str",
        "timestamp": "datetime.datetime",
        "tinyint": "int",
    }

    def __init__(
        self,
        server_host: str,
        http_path: str,
        access_token: str,
        port: Optional[int] = 443,
        catalog: Optional[
            str
        ] = None,  # will default to the default catalog (typically hive metastore)
        schema: Optional[str] = None,  # will default to the 'default' schema
        volume: Optional[str] = None,
        staging_allowed_local_path: Optional[str] = None,
    ) -> None:
        self.server_host = server_host
        self.http_path = http_path
        self.access_token = access_token
        self.port = port
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.volume_path = None
        self.user_agent = os.environ.get("SHIPYARD_USER_AGENT", None)
        self.staging_allowed_local_path = staging_allowed_local_path
        self._connection = None
        self._cursor = None
        super().__init__(
            server_host,
            http_path,
            access_token,
            port=port,
            catalog=catalog,
            schema=schema,
            volume=volume,
            volume_path=self.volume_path,
            user_agent=self.user_agent,
            staging_allowed_local_path=staging_allowed_local_path,
            _connection=None,
            _cursor=None,
        )

    def connect(self) -> Connection:
        try:
            conn = sql.connect(
                server_hostname=self.server_host,
                http_path=self.http_path,
                access_token=self.access_token,
                catalog=self.catalog,
                schema=self.schema,
                _user_agent_entry=self.user_agent,
                staging_allowed_local_path=self.staging_allowed_local_path,
            )
        except Exception as e:
            raise ExitCodeException(
                f"Error in connecting to Databricks: {str(e)}",
                self.EXIT_CODE_INVALID_CREDENTIALS,
            )
        else:
            logger.info("Successfully connected to Databricks")
            return conn

    @property
    def connection(self):
        if not self._connection:
            self._connection = self.connect()
        return self._connection

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.connection.cursor()
        return self._cursor

    def upload(
        self,
        file_path: Union[str, List[str]],
        table_name: str,
        file_format: str,
        datatypes: Optional[Dict[str, str]] = None,
        insert_method: str = "replace",
        match_type: str = "exact_match",
        pattern: Optional[str] = None,
    ):
        """
        Uploads a pandas dataframe to a table in Databricks SQL Warehouse. If the volume is provided upon instantiation (which is the recommended approach), the file(s) will be uploaded via a volume,
        otherwise it will be uploaded via an insert statement through the SQL connector

        Args:
            file_path: The location of the file to load
            table_name: The name of the table in Databricks to write to
            datatypes: The optional Spark SQL data types to use. If omitted, the schema will be inferred
            insert_method: Whether a table should be overwritten or appended to. If creating a new table, provide the `replace` option
            file_format: The format of the file (choices are csv and parquet at the moment)

        """
        try:
            if self.schema:
                self._create_schema()

            if self.volume:
                self.load_via_volume(
                    file_path=file_path,
                    table_name=table_name,
                    file_format=file_format,
                    data_types=datatypes,
                    insert_method=insert_method,
                    match_type=match_type,
                    pattern=pattern,
                )

            else:
                self.load_via_insert(
                    table_name=table_name,
                    file_format=file_format,
                    datatypes=datatypes,
                    file_path=file_path,
                    insert_method=insert_method,
                )
        except ExitCodeException as ec:
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            raise ExitCodeException(
                f"Error in upload process encountered: {str(e)}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def fetch(self, query: str) -> pd.DataFrame:
        """Retrieves the results of a Databricks SQL query as a pandas dataframe

        Args:
            query: A valid Databricks SELECT SQL statement


        Returns: a pandas dataframe

        """
        try:
            query_results = self.execute_query(query)
            results = query_results.fetchall()
            cols = results[0].asDict().keys()
            df = pd.DataFrame(results, columns=cols)
        except ExitCodeException as ec:
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            raise ExitCodeException(
                f"Error fetching querying results {str(e)}",
                self.EXIT_CODE_INVALID_QUERY,
            )
        else:
            return df

    def execute_query(self, query: str):
        """Execute a query in Databricks SQL warehouse. Intended use is for queries that do not return results

        Args:
            query: A valid Databricks SQL query
        """
        try:
            results = self.cursor.execute(query)
        except Exception as e:
            logger.error("Error in executing query")
            raise ExitCodeException(
                f"Could not execute query in Databricks: {str(e)}",
                self.EXIT_CODE_INVALID_QUERY,
            )
        else:
            return results

    def __exit__(self):
        self.connection.close()

    def close(self):
        self.connection.close()
        logger.info("Closed connection")

    def _create_table_sql(
        self, table_name: str, data_types: Optional[Dict[str, str]]
    ) -> str:
        """Helper function that generates the SQL to create a new table

        Args:
            table_name: The name of the table to create
            data_types: The optional dictionary of datatypes to use. Format should be {'column' : 'datatype'}. If omitted, a schemaless table will be created

        Returns: The CREATE TABLE... SQL

        """
        if not data_types:
            return f"CREATE TABLE IF NOT EXISTS {table_name}"

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for column, data_type in data_types.items():
            query += f"{column} {data_type},"
        query = query[:-1] + ")"  # remove the last comma and close the open parenthesis
        return query

    def _create_table(self, sql: str):
        """Helper function that creates a table from the SQL generated by _create_table_sql
        Args:
            sql: The SQL generated from _create_table_sql
        """
        try:
            self.execute_query(sql)
        except Exception as e:
            raise ExitCodeException(
                f"Error in creating table: {str(e)}", self.EXIT_CODE_INVALID_QUERY
            )

    def _replace_table(
        self, table_name: str, data_types: Dict[str, str], df: pd.DataFrame
    ):
        """Helper function to

        Args:
            table_name: The name of the SQL table to write to
            data_types: The datatypes to be used in the SQL table
            df: The dataframe to write to the SQL table
        """

        try:
            if self._table_exists(table_name):
                # drop the existing table
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            # create a new table
            create_table_sql = self._create_table_sql(
                table_name=table_name, data_types=data_types
            )
            insert_sql_statement = self.create_insert_statement(
                table_name, df, data_types
            )
            self.cursor.execute(create_table_sql)
            # populate the table
            self.cursor.execute(insert_sql_statement)
        except ExitCodeException as ec:
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            raise ExitCodeException(
                message=f"Error in attempting to replace table: {str(e)}",
                exit_code=self.EXIT_CODE_INVALID_QUERY,
            )

    def _append_table(
        self, table_name: str, data_types: Dict[str, str], df: pd.DataFrame
    ):
        """Helper function to append data from a dataframe to an existing table in Databricks

        Args:
            table_name: The name of the table to write to
            data_types: The converted Spark data types of the data
            df: The data to load

        """
        if not self._table_exists(table_name):
            raise TableDNE(table_name)
        try:
            insert_sql_statement = self.create_insert_statement(
                table_name, df, data_types
            )
            # populate the table
            self.cursor.execute(insert_sql_statement)
        except ExitCodeException:
            raise
        except Exception as e:
            raise ExitCodeException(
                message=f"Error in attempting to append to table: {str(e)}",
                exit_code=errs.EXIT_CODE_INVALID_QUERY,
            )

    def convert_to_spark_type(self, pandas_data_type: str) -> Optional[str]:
        """Helper function which converts a pandas data type to the equivalent Spark datatype
        Args:
            pandas_data_type: The associated pandas data type

        Returns: The equivalent Spark datatype. If not match, then will default to a string

        """
        MAPPING = {
            "int64": "bigint",
            "int32": "int",
            "int16": "smallint",
            "int8": "tinyint",
            "float64": "double",
            "float32": "float",
            "object": "string",
            "bool": "boolean",
            "datetime64[ns]": "timestamp",
            "datetime64[ns, UTC]": "timestamp",
            "timedelta64[ns]": "string",  # Adjust as needed
            "category": "string",  # Adjust as needed
        }

        # Convert Pandas data type to Spark data type
        spark_type = MAPPING.get(str(pandas_data_type).lower())
        if not spark_type:
            return "string"
        return spark_type

    def create_insert_statement(
        self, table_name: str, df: pd.DataFrame, datatypes: Dict[str, str]
    ) -> str:
        """Helper function to generate the `INSERT` SQL statement

        Args:
            table_name: The name of the table to load to
            df: The dataframe to be loaded
            datatypes: The datatypes to be used

        Returns: The SQL statement

        """
        try:
            # Start building the insert statement
            insert_statement = f"INSERT INTO {table_name} ("

            # Extract column names
            columns = df.columns.tolist()

            # Join column names with commas and append to the insert statement
            insert_statement += ", ".join(columns)

            # Continue building the insert statement
            insert_statement += ") VALUES\n"

            # Iterate through the DataFrame rows and format the values
            for index, row in df.iterrows():
                values = []

                # Iterate through columns and format values based on datatype
                for col in columns:
                    value = row[col]
                    data_type = datatypes.get(col, "string")
                    # Format the value based on data type
                    if data_type in [
                        "int",
                        "decimal",
                        "bigint",
                        "double",
                        "smallint",
                        "tinyint",
                    ]:
                        values.append(str(value))
                    else:
                        values.append(f"'{value}'")
                # Join values with commas, wrap in parentheses, and append to the insert statement
                insert_statement += f"({', '.join(values)}),\n"

            # Remove the trailing comma and newline
            insert_statement = insert_statement.rstrip(",\n")
        except Exception as e:
            raise ExitCodeException(
                message=f"Error in generating insert statement {str(e)}",
                exit_code=self.EXIT_CODE_INSERT_STATEMENT_ERROR,
            )

        else:
            return insert_statement + ";"

    def _table_exists(self, table_name: str) -> bool:
        """Helper function to determine if the table exists

        Args:
            table_name: The name of the SQL table

        Returns: True if exists, False if not

        """
        try:
            tables = self.cursor.tables(
                catalog_name=self.catalog, schema_name=self.schema
            ).fetchall()
            for table in tables:
                if table["TABLE_NAME"] == table_name:
                    return True
            return False
        except Exception as e:
            return False

    def _create_schema(self):
        if not self.catalog:
            logger.warning(
                "Catalog was not provided, creating new schema in the default catalog"
            )

        create_sql = (
            f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}"
            if self.catalog
            else f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
        )

        logger.info(f"Creating schema {self.schema} if it doesn't already exist")
        self.cursor.execute(create_sql)

    def _volume_sql(self) -> str:
        """Helper function to generate the SQL necessary to create a volume in Databricks

        Returns: The `CREATE VOLUME` SQL statement

        """
        if not self.catalog:
            logger.warning(
                "Catalog was not provided, creating new volume in the default catalog"
            )
        if not self.schema:
            logger.warning(
                "Schema was not provided, creating new volume in the default schema"
            )

        if not self.catalog and self.schema:
            return f"CREATE VOLUME IF NOT EXISTS {self.schema}.{self.volume}"
        elif not self.schema:
            raise VolumeSqlError
        else:
            return f"CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.schema}.{self.volume}"

    def _create_volume(self, volume_sql: str):
        """Creates a volume for uploading files to

        Args:
            volume_sql: The SQL string generated by _volume_sql
        """
        try:
            self.cursor.execute(volume_sql)
        except Exception as e:
            raise ExitCodeException(
                f"Error in creating volume: {str(e)}", errs.EXIT_CODE_VOLUME_CREATION
            )

        else:
            logger.debug(f"Successfully created volume {self.volume}")

    def _load_volume(
        self,
        file_path: Union[str, List[str]],
        table_name: str,
        file_type: str,
        match_type: str = "exact_match",
    ):
        """Helper function to load a file into a volume in Databricks

        Args:
            file_path: The file path of the file to load
            table_name: The name of the destination table
            file_type: The file type (choices are CSV and PARQUET at the moment)
        """
        tmp_table = f"{table_name}_tmp"
        logger.debug(f"temporary table being loaded to volume is {tmp_table}")
        if match_type == "glob_match":
            self.volume_path = []
            for file in file_path:
                file_name = os.path.basename(file)
                if not self.catalog and self.schema:
                    self.volume_dir = (
                        f"/Volumes/{self.schema}/{self.volume}/{tmp_table}/"
                    )
                    volume_path = (
                        f"/Volumes/{self.schema}/{self.volume}/{tmp_table}/{file_name}"
                    )
                    self.volume_path.append(volume_path)
                elif not self.schema:
                    raise VolumeSqlError
                else:
                    volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{tmp_table}/{file_name}"
                    self.volume_path.append(volume_path)
                    self.volume_dir = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{tmp_table}/"

                    upload_sql = f"PUT '{file}' INTO '{volume_path}' OVERWRITE"
                    try:
                        self.cursor.execute(upload_sql)

                    except Exception as e:
                        raise VolumeUploadError(volume=self.volume, error_msg=str(e))
                    else:
                        logger.debug(
                            f"Successfully loaded {file} to to volume {volume_path}"
                        )

        # for exact match cases
        else:
            file_name = Path(file_path).name
            if not self.catalog and self.schema:
                volume_path = (
                    f"/Volumes/{self.schema}/{self.volume}/{tmp_table}/{file_name}"
                )
                self.volume_path = volume_path
            elif not self.schema:
                raise VolumeSqlError
            else:
                volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{tmp_table}/{file_name}"
                self.volume_path = volume_path

                upload_sql = f"PUT '{file_path}' INTO '{self.volume_path}' OVERWRITE"
                try:
                    self.cursor.execute(upload_sql)

                except Exception as e:
                    raise VolumeUploadError(volume=self.volume, error_msg=str(e))
                else:
                    logger.debug(
                        f"Successfully loaded {file_path} to volume {self.volume_path}"
                    )

    def _copy_into(
        self,
        table_name: str,
        file_format: str,
        pattern: Optional[str] = None,
        datatypes: Optional[Dict[str, str]] = None,
        match_type: Optional[str] = "exact_match",
    ):
        """Helper function that executes a COPY INTO statement from a volume into a destination table

        Args:
            table_name: The name of the destination table
            file_format: The format of the file being loaded (options right now are CSV and PARQUET)
            files: The file pattern to use, if multiple files are being loaded

        """
        if not self.catalog and self.schema:
            table_path = f"{self.schema}.{table_name}"
        elif not self.schema:
            table_path = table_name
        else:
            table_path = f"{self.catalog}.{self.schema}.{table_name}"

        if match_type == "glob_match":
            copy_sql = f"""COPY INTO {table_path} FROM '{self.volume_dir}' 
            FILEFORMAT = {file_format}
            PATTERN = '{pattern}'
            """

        else:
            copy_sql = f"""COPY INTO {table_path} FROM '{self.volume_path}' 
            FILEFORMAT = {file_format}
            """
        logger.debug(f"COPY SQL to be executed is {copy_sql}")

        # TODO: Make sure this works for uploading multiple csvs and parquets. The pattern option should be used
        if file_format == "csv":
            copy_sql += " FORMAT_OPTIONS ('mergeSchema' = 'true', 'header' = 'true', 'inferSchema' = 'true') COPY_OPTIONS ('mergeSchema' = 'true')"
        elif file_format == "parquet":
            copy_sql += " FORMAT_OPTIONS ('mergeSchema' = 'true') COPY_OPTIONS('mergeSchema' = 'true')"

        try:
            self.cursor.execute(copy_sql)
        except Exception as e:
            raise CopyIntoError(
                table_path=table_path, volume_path=self.volume_path, error_msg=str(e)
            )
        else:
            logger.debug("Successfully copied data from volume into table")

    def _remove_volume(self):
        """Helper function to remove the volume that was used for ingestion. This should be wiped after a a successful run of _copy_into()."""
        try:
            if isinstance(self.volume_path, list):
                for vol in self.volume_path:
                    self.cursor.execute(f"REMOVE '{vol}'")
            else:
                self.cursor.execute(f"REMOVE '{self.volume_path}'")
        except Exception as e:
            raise RemoveVolumeError(volume_path=self.volume_path, error_msg=str(e))
        else:
            logger.debug(f"Successfully removed files from volume {self.volume_path}")

    def _append_from_volume(self, table_name: str, file_format: str):
        """Helper function to append to an existing delta table

        Args:
            table_name: The name of the delta table to append to
            file_format: The format of the data in the volume (either parquet or csv)
        """
        # create the temp table
        tmp_table = f"tmp_{table_name}"
        self.execute_query(f"CREATE TABLE {tmp_table}")

        # copy into the temp table from the volume
        self._copy_into(table_name=tmp_table, file_format=file_format)

        # insert into the target table from the temp table

        insert_sql = f"INSERT INTO {table_name} TABLE {tmp_table}"
        self.execute_query(insert_sql)

        # drop the temp table
        self.execute_query(f"DROP TABLE {tmp_table}")

    def load_via_volume(
        self,
        file_path: Union[str, List[str]],
        table_name: str,
        file_format: str,
        data_types: Optional[Dict[str, str]] = None,
        insert_method: Optional[str] = "replace",
        match_type: str = "exact_match",
        pattern: Optional[str] = None,
    ):
        """This function calls four separate helper functions to create the volume, load the file into that volume,
        copy the data from the volume to the destination table, remove the volume

        Args:
            file_path: The file path of the file to be loaded.
            table_name: The name of the destination table
            file_format: The format of the file (choices are CSV and PARQUET at the moment)
        """

        try:
            volume_sql = self._volume_sql()
            self._create_volume(volume_sql)
            self._create_table(self._create_table_sql(table_name, data_types))
            if match_type == "glob_match":
                logger.debug("Loading via glob")
                self._load_volume(
                    file_path=file_path,
                    table_name=table_name,
                    file_type=file_format,
                    match_type=match_type,
                )
                self._copy_into(
                    table_name=table_name,
                    file_format=file_format,
                    pattern=pattern,
                    match_type=match_type,
                )
                self._remove_volume()

            else:
                self._load_volume(
                    file_path=file_path,
                    table_name=table_name,
                    file_type=file_format,
                    match_type=match_type,
                )
                if insert_method == "replace":
                    self._copy_into(
                        table_name=table_name,
                        file_format=file_format,
                        match_type=match_type,
                    )
                else:
                    # append to table by using an insert statement
                    self._append_from_volume(
                        table_name=table_name, file_format=file_format
                    )
                self._remove_volume()

        except ExitCodeException as ec:
            logger.error(ec.message)
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            raise VolumeUploadError(volume=self.volume, error_msg=str(e))

    def load_via_insert(
        self,
        table_name: str,
        file_format: str,
        file_path: str,
        datatypes: Optional[Dict[Any, Any]],
        insert_method: str = "replace",
    ):
        if file_format == "csv":
            data = pd.read_csv(file_path)
        elif file_format == "parquet":
            data = pd.read_parquet(file_path)
        else:
            raise ExitCodeException(
                f"Invalid file type provided. Must be either csv or parquet",
                self.EXIT_CODE_FILE_NOT_FOUND,
            )
        try:
            if not datatypes:
                # need to infer datatypes
                datatypes = {}
                pd_types = dict(data.dtypes)
                for column, dtype in pd_types.items():
                    str_dtype = str(dtype)
                    if str_dtype == "object":
                        datatypes[column] = "string"
                    else:
                        spark_type = self.convert_to_spark_type(str_dtype)
                        datatypes[column] = spark_type
            else:
                # check to see that the datatypes are valid
                for key, value in datatypes.items():
                    if str(value).lower() not in self.SPARK_TYPES.keys():
                        raise ExitCodeException(
                            f"Error: {value} is not a valid spark data type. Please provide a valid data type",
                            self.EXIT_CODE_INVALID_DATA_TYPES,
                        )
            if insert_method == "replace":
                self._replace_table(
                    table_name=table_name, data_types=datatypes, df=data
                )

            elif insert_method == "append":
                self._append_table(table_name=table_name, data_types=datatypes, df=data)

            else:
                raise ExitCodeException(
                    "Invalid insert_method provided. Options are `replace` and `append`",
                    self.EXIT_CODE_INVALID_ARGUMENTS,
                )
        except ExitCodeException as ec:
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            raise ExitCodeException(
                message=f"Error in attempting to upload data to databricks {str(e)}",
                exit_code=self.EXIT_CODE_INVALID_UPLOAD_VALUE,
            )
