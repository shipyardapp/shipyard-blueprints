import pandas as pd
import os
from pathlib import Path
from databricks.sql.client import Connection
from shipyard_templates import DatabricksDatabase, ExitCodeException
from databricks import sql
from databricks.sql.client import Connection  # for type hints
from typing import Optional, Dict, List, Any


class DatabricksSqlClient(DatabricksDatabase):
    EXIT_CODE_VOLUME_CREATION = 101
    EXIT_CODE_VOLUME_SQL = 102
    EXIT_CODE_VOLUME_UPLOAD_ERROR = 103
    EXIT_CODE_COPY_INTO_ERROR = 104
    EXIT_CODE_REMOVE_VOLUME_ERROR = 105
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
        )

    def connect(self) -> Connection:
        return sql.connect(
            server_hostname=self.server_host,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog,
            schema=self.schema,
            _user_agent_entry=self.user_agent,
        )

    @property
    def connection(self):
        return self.connect()

    @property
    def cursor(self):
        return self.connection.cursor()

    def upload(
        self,
        data: pd.DataFrame,
        table_name: str,
        datatypes: Optional[Dict[str, str]] = None,
        insert_method: str = "replace",
    ):
        """
        Uploads a pandas dataframe to a table in Databricks SQL Warehouse.

        Args:
            data: The dataframe to load
            table_name: The name of the table in Databricks to write to
            datatypes: The optional Spark SQL data types to use. If omitted, the schema will be inferred
            insert_method: Whether a table should be overwritten or appended to. If creating a new table, provide the `replace` option
        """
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
            self.logger.error("Error in executing query")
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
        self.logger.info("Closed connection")

    def _create_table(self, table_name: str, data_types: Dict[str, str]) -> str:
        """Helper function that generates the SQL to create a new table

        Args:
            table_name: The name of the table to create
            data_types: The Dictionary of datatypes to use. Format should be {'column' : 'datatype'}

        Returns: The CREATE TABLE... SQL

        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for column, data_type in data_types.items():
            query += f"{column} {data_type},"
        query = query[:-1] + ")"  # remove the last comma and close the open parenthesis
        return query

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
            create_table_sql = self._create_table(
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
            raise ExitCodeException(
                f"Error in attempting to append to {table_name}, table does not exist. Please use the `replace` option for insert_method",
                exit_code=self.EXIT_CODE_INVALID_UPLOAD_VALUE,
            )  # may need to use a different exit code
        try:
            insert_sql_statement = self.create_insert_statement(
                table_name, df, data_types
            )
            # populate the table
            self.cursor.execute(insert_sql_statement)
        except ExitCodeException as ec:
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            raise ExitCodeException(
                message=f"Error in attempting to replace table: {str(e)}",
                exit_code=self.EXIT_CODE_INVALID_QUERY,
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
            self.logger.warning(
                "Catalog was not provided, creating new schema in the default catalog"
            )

        create_sql = (
            f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}"
            if self.catalog
            else f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
        )

        self.logger.info(f"Creating schema {self.schema} if it doesn't already exist")
        self.cursor.execute(create_sql)

    def _volume_sql(self) -> str:
        """Helper function to generate the SQL necessary to create a volume in Databricks

        Returns: The `CREATE VOLUME` SQL statement

        """
        if not self.catalog:
            self.logger.warning(
                "Catalog was not provided, creating new volume in the default catalog"
            )
        if not self.schema:
            self.logger.warning(
                "Schema was not provided, creating new volume in the default schema"
            )

        if not self.catalog and self.schema:
            return f"CREATE VOLUME {self.schema}.{self.volume}"
        elif not self.schema:
            raise ExitCodeException(
                "Schema must be provided if the catalog is provided",
                exit_code=self.EXIT_CODE_VOLUME_SQL,
            )
        else:
            return f"CREATE VOLUME {self.catalog}.{self.schema}.{self.volume}"

    def _create_volume(self, volume_sql: str):
        """Creates a volume for uploading files to

        Args:
            volume_sql: The SQL string generated by _volume_sql
        """
        try:
            self.cursor.execute(volume_sql)
        except Exception as e:
            raise ExitCodeException(
                f"Error in creating volume: {str(e)}", self.EXIT_CODE_VOLUME_CREATION
            )

        else:
            self.logger.info(f"Successfully created volume {self.volume}")

    def _load_volume(self, file_path: str, table_name: str, file_type: str):
        """Helper function to load a file into a volume in Databricks

        Args:
            file_path: The file path of the file to load
            table_name: The name of the destination table
            file_type: The file type (choices are CSV and PARQUET at the moment)
        """
        tmp_table = f"{table_name}_tmp"
        file_name = Path(file_path).name
        if not self.catalog and self.schema:
            volume_path = (
                f"/Volumes/{self.schema}/{self.volume}/{tmp_table}/{file_name}"
            )
            self.volume_path = volume_path
        elif not self.schema:
            raise ExitCodeException(
                "Schema must be provided if the catalog is provided",
                exit_code=self.EXIT_CODE_VOLUME_SQL,
            )
        else:
            volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{tmp_table}/{file_name}"
            self.volume_path = volume_path

        # one last check that the volume path is legit
        if self.volume_path is None:
            raise ExitCodeException(
                "The volume path cannot be None, aborting upload",
                self.EXIT_CODE_VOLUME_UPLOAD_ERROR,
            )

        upload_sql = f"PUT {file_path} INTO '{self.volume_path}' OVERWRITE"
        try:
            self.cursor.execute(upload_sql)

        except Exception as e:
            raise ExitCodeException(
                f"Error when trying to load file to volume: {str(e)}",
                self.EXIT_CODE_VOLUME_UPLOAD_ERROR,
            )

        else:
            self.logger.info("Successfully loaded volume")

    def _copy_into(
        self, table_name: str, file_format: str, files: Optional[str] = None
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

        copy_sql = f"""COPY INTO {table_path} FROM '{self.volume_path}' 
        FILEFORMAT = {file_format}
        """
        try:
            self.cursor.execute(copy_sql)
        except Exception as e:
            raise ExitCodeException(
                f"Error in copy data from volume {self.volume_path} into {table_path}: {str(e)}",
                self.EXIT_CODE_COPY_INTO_ERROR,
            )
        else:
            self.logger.info("Successfully copied data from volume into table")

    def _remove_volume(self):
        """Helper function to remove the volume that was used for ingestion. This should be wiped after a a successful run of _copy_into()."""
        try:
            self.cursor.execute(f"REMOVE {self.volume_path}")
        except Exception as e:
            raise ExitCodeException(
                f"Error in removing volume {self.volume_path}: {str(e)}",
                self.EXIT_CODE_REMOVE_VOLUME_ERROR,
            )

        else:
            self.logger.info(f"Successfully removed volume {self.volume_path}")

    def load_via_volume(self, file_path: str, table_name: str, file_format: str):
        """This function calls four separate helper functions to create the volume, load the file into that volume,
        copy the data from the volume to the destination table, remove the volume

        Args:
            file_path: The file path of the file to be loaded
            table_name: The name of the destination table
            file_format: The format of the file (choices are CSV and PARQUET at the moment)
        """
        volume_sql = self._volume_sql()
        try:
            self._create_volume(volume_sql)
            self._load_volume(
                file_path=file_path, table_name=table_name, file_type=file_format
            )
            self._copy_into(table_name=table_name, file_format=file_format)
            self._remove_volume()

        except ExitCodeException as ec:
            self.logger.error(ec.message)
            raise ExitCodeException(ec.message, ec.exit_code)
        except Exception as e:
            raise ExitCodeException(
                f"Unknown error in loading volume: {str(e)}",
                exit_code=self.EXIT_CODE_VOLUME_UPLOAD_ERROR,
            )
