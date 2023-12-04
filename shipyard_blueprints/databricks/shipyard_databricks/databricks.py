import polars as pl
import pandas as pd
from http import server
import databricks
from databricks.sql.client import Connection
from shipyard_templates import CloudStorage, DatabricksDatabase, ExitCodeException
from databricks_cli.sdk.api_client import ApiClient
from databricks.sdk import WorkspaceClient
from databricks import sql
from databricks.sql.client import Connection  # for type hints
from typing import Optional, Dict, List, Any


class DatabricksClient(CloudStorage):
    def __init__(self, access_token: str, instance_url: str) -> None:
        self.access_token = access_token
        self.instance_url = instance_url
        self.base_url = f"https://{self.instance_url}/api/2.0"
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        # self.headers = {
        #     'Authorization': f"Bearer {self.access_token}",
        #     'Content-Type': 'application/json'
        # }
        super().__init__(access_token=access_token, instance_url=instance_url)

    def connect(self):
        # client = ApiClient(host=self.base_url, token=self.access_token)
        client = WorkspaceClient(host=self.base_url, token=self.access_token)

        return client

    def upload(self):
        pass

    def remove(self):
        pass

    def move(self):
        pass

    def download(self):
        pass


class DatabricksSqlClient(DatabricksDatabase):
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
        data: pl.DataFrame,
        table_name: str,
        schema: Dict[Any, Any],
        insert_method: str = "replace",
    ):
        # NOTE: Need to have a create table function that creates a databricks table (if it doesn't exist) with the associated datatypes
        pass

    def fetch(self, query: str) -> pl.DataFrame:
        """

        Args:
            query:


        Returns:

        """
        try:
            query_results = self.execute_query(query)
            self.logger.info(f"Results are {query_results}")
            df = pl.DataFrame(query_results)
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
        """

        Args:
            query:

        Raises:
            ExitCodeException:
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
            self.logger.info("Successfully executed query")
            return results

    def __exit__(self):
        self.connection.close()

    def close(self):
        self.connection.close()
        self.logger.info("Closed connection")
