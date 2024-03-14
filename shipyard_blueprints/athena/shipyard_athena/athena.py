import boto3
import sys
import time
from shipyard_templates import Database, ShipyardLogger, ExitCodeException
from shipyard_athena.errors import exceptions as errs
from typing import Dict, Optional

logger = ShipyardLogger.get_logger()


class AthenaClient(Database):
    def __init__(
        self,
        aws_access_key: str,
        aws_secret_key: str,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
    ) -> None:
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.bucket = bucket
        self._athena = None
        self._s3 = None

    @property
    def athena(self):
        if not self._athena:
            self._athena = self.create_client()
        return self._athena

    @property
    def s3(self):
        if not self._s3:
            self._s3 = self.connect_bucket()
        return self._s3

    def connect(self):
        try:
            auth_client = boto3.client(
                "sts",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
            )
            resp = auth_client.get_caller_identity()
            logger.debug(f"Contents of response: {resp}")
            if resp["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return 0
        except Exception as e:
            logger.error(
                f"Error occurred when trying to verify Access Key ID and Secret Access Key. Message from AWS: {e}"
            )
            return 1
        else:
            logger.error(
                f"Client connected but with an unknown status code. Response from AWS: {resp}"
            )
            return 1

    def create_client(self):
        """Establishes the connection to Athena

        Returns: The Athena client

        """
        try:
            client = boto3.client(
                "athena",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
            )
        except Exception as e:
            raise errs.InvalidAthenaAccess(e)
        else:
            return client

    def connect_bucket(self):
        """Extablishes the connection to S3

        Returns: The S3 client

        """
        try:
            s3_client = boto3.resource(
                "s3",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
            )
        except Exception as e:
            raise errs.InvalidS3Access(self.bucket, e)
        else:
            return s3_client

    def execute_query(
        self,
        query: str,
        database: Optional[str] = None,
        log_folder: Optional[str] = None,
    ):
        """
        Executes a query in Athena and returns the execution status.

        Args:
            query (str): The SQL query to be executed.
            database (Optional[str]): The name of the database to execute the query in. Defaults to None.
            log_folder (Optional[str]): The folder path to store query logs. Defaults to None.

        Returns:
            str: The execution state of the query.

        """
        job = self._execute_query(query, database, log_folder)
        logger.debug("Started query execution")
        job_id = job["QueryExecutionId"]
        logger.debug(f"Fetched job ID {job_id}")
        status = self._fetch_query_execution_status(job_id)
        logger.debug(f"Query status is {status}")
        while not status:
            time.sleep(5)
            logger.debug("Wating another 5 seconds to check query status")
            status = self._fetch_query_execution_status(job_id)
            logger.debug(f"Query status is {status}")

        logger.debug(f"Status JSON reads: {status}")
        return status["QueryExecution"]["Status"]["State"]

    def fetch(
        self,
        query: str,
        dest_path: str,
        database: Optional[str] = None,
        log_folder: Optional[str] = None,
    ):
        """
        Fetches the result of a query from Athena and saves it to a destination path.

        Args:
            query (str): The SQL query to execute.
            dest_path (str): The path where the query result will be saved.
            database (Optional[str]): The name of the database to execute the query in. Defaults to None.
            log_folder (Optional[str]): The folder where query logs are stored. Defaults to None.

        Returns:
            str: The response from the download operation.

        Raises:
            ExitCodeException: If an exit code exception occurs.
            errs.FetchError: If any other exception occurs during the fetch operation.
        """
        try:
            job = self._execute_query(
                query=query, database=database, log_folder=log_folder
            )
            job_id = job["QueryExecutionId"]
            status = self._fetch_query_execution_status(job_id)
            logger.debug(f"Query status is {status}")
            while not status:
                time.sleep(5)
                logger.debug("Waiting another 5 seconds to check query status")
                status = self._fetch_query_execution_status(job_id)
                logger.debug(f"Query status is {status}")

            response = self.s3.Bucket(self.bucket).download_file(
                f'{log_folder}{"/" if log_folder else ""}{job_id}.csv', dest_path
            )
            logger.debug("Download complete")
            if response:
                logger.debug(f"Contents of response: {response}")
        except ExitCodeException:
            raise
        except Exception as e:
            raise errs.FetchError(e)
        else:
            return response

    def _fetch_query_execution_status(self, job_id: str):
        """Fetches the query status

        Args:
            job_id: The ID of the associated query to fetch
        """
        result = self.athena.get_query_execution(QueryExecutionId=job_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return result
        elif state == "FAILED":
            err_msg = result["QueryExecution"]["Stats"].get("StateChangeReason")
            raise errs.QueryFailed(err_msg)
        return False

    def _execute_query(
        self,
        query: str,
        database: Optional[str] = None,
        log_folder: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Helper function to execute the given query using Amazon Athena.

        Args:
            query (str): The SQL query to execute.
            database (Optional[str]): The name of the database to execute the query in. Defaults to None.
            log_folder (Optional[str]): The folder in S3 where the query logs will be stored. Defaults to None.

        Returns:
            Dict[str, str]: A dictionary containing information about the executed query job.
        """
        context = {}
        if database:
            context = {"Database": database}

        output = f"s3://{self.bucket}"
        if log_folder:
            output += f"/{log_folder.strip('/')}"

        job = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext=context,
            ResultConfiguration={"OutputLocation": output},
        )
        return job

    def upload(self):
        pass
