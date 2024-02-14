import json
from google.cloud.bigquery.table import RowIterator
import pandas as pd
from typing import Optional, Dict, Union, List
from google.cloud import bigquery
from google.oauth2 import service_account
from shipyard_templates import GoogleDatabase, ShipyardLogger, ExitCodeException
from shipyard_bigquery.utils.exceptions import (
    DownloadToGcsError,
    FetchError,
    InvalidSchema,
    QueryError,
    SchemaFormatError,
    SchemaValidationError,
    TempTableCreationError,
)
from shipyard_bigquery.utils import utils

logger = ShipyardLogger.get_logger()


class BigQueryClient(GoogleDatabase):
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
            raise QueryError(f"Error in executing query: {str(e)}")
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
            raise FetchError(f"Error in fetching query: {str(e)}")
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
        try:
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
                if not utils.validate_data_types(schema):
                    raise SchemaValidationError(
                        "Inputted schema contains an invalid data type. Run with LOG_LEVEL set to DEBUG for more information"
                    )
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
        except SchemaValidationError:
            raise
        except SchemaFormatError:
            raise
        except InvalidSchema:
            raise
        except Exception as e:
            raise ExitCodeException(
                f"An error occurred when attempting to upload to BigQuery: {str(e)}",
                self.EXIT_CODE_INVALID_UPLOAD_VALUE,
            )

    def download_to_gcs(self, query: str, bucket_name: str, path: Optional[str] = None):
        try:
            project_id, dataset_id, table_id, location = self._create_temp_table(query)
            dataset_ref = bigquery.DatasetReference(
                project=project_id, dataset_id=dataset_id
            )
            table_ref = dataset_ref.table(table_id)
            dest_uri = f"gs://{bucket_name}/{path}"
            self.conn.extract_table(table_ref, dest_uri, location=location).result()
        except TempTableCreationError as te:
            raise DownloadToGcsError(
                f"Error in executing the query and storing in a temp file: {te.message}"
            )
        except Exception as e:
            raise DownloadToGcsError(f"Error downloading file to GCS: {str(e)}")

    def _format_schema(
        self, schema: Union[List[List[str]], Dict[str, List[str]]]
    ) -> List[bigquery.SchemaField]:
        """Helper function to format the schema appropriately for BigQuery

        Args:
            schema: The representation inputted as either a list of lists (for backwards compatibility) or preferably JSON

        Returns: The formatted schema

        """
        formatted_schema = []
        try:
            for item in schema:
                if isinstance(item, list):
                    formatted_schema.append(bigquery.SchemaField(*item))
                elif isinstance(item, dict):
                    formatted_schema.append(bigquery.SchemaField.from_api_repr(item))
                else:
                    raise InvalidSchema(
                        "Format of inputted schema is incorrect, this should preferably be a JSON representation or a List of Lists. For additional information and examples, visit https://cloud.google.com/bigquery/docs/schemas#specifying_a_json_schema_file"
                    )
        except Exception as e:
            raise SchemaFormatError(
                f"Error in preparing the inputted schema to the approrpriate BigQuery format: {str(e)}"
            )
        else:
            logger.debug(f"Formatted schema is {formatted_schema}")
            return formatted_schema

    def _create_temp_table(self, query: str):
        """Helper function to execute a query and store the results in a temporary table

        Args:
            query: The query to execute

        Returns: The metadata of the temp table

        """
        try:
            data = self.conn.query(query)
            data.result()
            temp_table_ids = data._properties["configuration"]["query"][
                "destinationTable"
            ]
            location = data._properties["jobReference"]["location"]
            project_id = temp_table_ids.get("projectId")
            dataset_id = temp_table_ids.get("datasetId")
            table_id = temp_table_ids.get("tableId")
        except Exception as e:
            raise TempTableCreationError(
                f"Error in storing query results in temp table: {str(e)}"
            )
        else:
            return project_id, dataset_id, table_id, location
