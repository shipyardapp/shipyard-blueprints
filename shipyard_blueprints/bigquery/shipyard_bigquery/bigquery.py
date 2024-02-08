import json
import os
from google.cloud.bigquery.table import RowIterator
import pandas as pd
from typing import Optional, Dict, Any, Union, List
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from shipyard_templates import GoogleDatabase, ShipyardLogger, ExitCodeException
from shipyard_bigquery.utils.exceptions import FetchError, InvalidSchema, QueryError
from shipyard_bigquery.utils import utils

logger = ShipyardLogger.get_logger()


class BigQueryClient(GoogleDatabase):
    EXIT_CODE_FETCH_ERROR = 101
    EXIT_CODE_QUERY_ERROR = 102

    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account)

    @property
    def json_creds(self):
        return json.loads(self.service_account)

    @property
    def credentials(self):
        return service_account.Credentials.from_service_account_info(self.json_creds)

    @property
    def email(self):
        return self.credentials.service_account_email

    def connect(self):
        conn = bigquery.Client(credentials=self.credentials)
        self.conn = conn
        return self

    def execute_query(self, query: str) -> RowIterator:
        """Executes a query and returns the results

        Args:
            query: The query to execute

        Returns: The query results as a pandas dataframe

        """
        try:
            query_job = self.conn.query(query)
            results = query_job.result()
        except Exception as e:
            raise QueryError(
                f"Error in executing query: {str(e)}", self.EXIT_CODE_FETCH_ERROR
            )
        else:
            return results

    def fetch(self, query: str) -> pd.DataFrame:
        """Returns the results of a query to a pandas dataframe

        Args:
            query: The query to fetch

        Returns: The query results as a pandas dataframe

        """
        try:
            df = self.conn.query(query).to_dataframe()
        except Exception as e:
            raise FetchError(
                f"Error in fetching query: {str(e)}", self.EXIT_CODE_FETCH_ERROR
            )
        else:
            return df

    def upload(
        self,
        file: str,
        dataset: str,
        table: str,
        upload_type: str,
        skip_header_rows: Optional[int] = None,
        schema: Optional[Union[List[List], Dict[str, str]]] = None,
        quoted_newline: bool = False,
    ):
        """Upload a file to a table in BigQuery
        Args:
            file: The file to load
            dataset: The name of the dataset in BigQuery to upload to
            table: The name of the table to write to
            upload_type: Whether to append to or replace the data. Choices are 'overwrite' and 'append'
            schema: The optional schema of the table to be loaded
            skip_header_rows: Whether to skip the header row
            quoted_newline: Whether newline characters should be quoted
        """
        dataset_ref = self.conn.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig()

        if upload_type == "overwrite":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True  # infer the schema
        if skip_header_rows:
            job_config.skip_leading_rows = skip_header_rows
        if schema:
            # TODO: add validation check for schema type
            # TODO: during validation, identify which datatype is bad
            # if not utils.validate_data_types(schema):
            #     raise InvalidSchema("Schema type provided is invalid")
            logger.debug(f"Schema is {schema}")
            job_config.autodetect = False
            job_config.schema = self._format_schema(schema)
        if quoted_newline:
            job_config.allow_quoted_newlines = True
        with open(file, "rb") as source_file:
            job = self.conn.load_table_from_file(
                source_file, table_ref, job_config=job_config
            )
        job.result()

    def _format_schema(
        self, schema: Union[List[List[str]], Dict[str, str]]
    ) -> List[bigquery.SchemaField]:
        formatted_schema = []
        if isinstance(schema, list):
            # handle the case where it is a list
            logger.debug("Inputted schema was a list, formatting appropriately")
            for item in schema:
                formatted_schema.append(bigquery.SchemaField(item[0], item[1]))
        elif isinstance(schema, Dict):
            # handle the case where it is a JSON representation
            logger.debug("Inputted schema was JSON, formatting appropriately")
            for k, v in schema.items():
                formatted_schema.append(bigquery.SchemaField(k, v))
        else:
            raise InvalidSchema(
                "The type of schema provided is unsupported. Provide a List of Lists or a JSON representation",
                self.EXIT_CODE_INVALID_SCHEMA,
            )

        logger.debug(f"Formatted schema is {formatted_schema}")
        return formatted_schema
