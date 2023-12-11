import pandas as pd
from databricks.sql.client import Connection
from shipyard_templates import DatabricksDatabase, ExitCodeException
from databricks import sql
from databricks.sql.client import Connection  # for type hints
from typing import Optional, Dict, List, Any


class DatabricksSqlClient(DatabricksDatabase):
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
    ) -> None:
        self.server_host = server_host
        self.http_path = http_path
        self.access_token = access_token
        self.port = port
        self.catalog = catalog
        self.schema = schema
        super().__init__(server_host, http_path, access_token, port=port)

    def connect(self) -> Connection:
        return sql.connect(
            server_hostname=self.server_host,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog,
            schema=self.schema,
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

        query = f"SHOW TABLES like '{table_name}'"
        results = self.fetch(query)
        return results.shape[0] == 1
