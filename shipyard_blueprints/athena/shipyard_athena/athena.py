import boto3
import sys
import time
from shipyard_templates import Database, ShipyardLogger, ExitCodeException
from shipyard_athena.errors import exceptions as errs
from typing import Dict, Optional

logger = ShipyardLogger.get_logger()


class AthenaClient:
    def __init__(
        self,
        aws_access_key: str,
        aws_secret_key: str,
        bucket: str,
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
            self._athena = self.connect_athena()
        return self._athena

    @property
    def s3(self):
        if not self._s3:
            self._s3 = self.connect_bucket()
        return self._s3

    def connect_athena(self):
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
        job = self._execute_query(query, database, log_folder)
        logger.debug("Started query execution")
        job_id = job["QueryExecutionId"]
        logger.debug(f"Fetched job ID {job_id}")
        status = self._get_query_status(job_id)
        logger.debug(f"Query status is {status}")
        while not status:
            time.sleep(5)
            logger.debug("Wating another 5 seconds to check query status")
            status = self._get_query_status(job_id)
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
        """Returns the results of an athena query as a csv file

        Args:
            query:
            dest_path:
            database:
            output_location:
        """
        try:
            job = self._execute_query(
                query=query, database=database, log_folder=log_folder
            )
            job_id = job["QueryExecutionId"]
            status = self._get_query_status(job_id)
            logger.debug(f"Query status is {status}")
            while not status:
                time.sleep(5)
                logger.debug("Wating another 5 seconds to check query status")
                status = self._get_query_status(job_id)
                logger.debug(f"Query status is {status}")

            response = self.s3.Bucket(self.bucket).download_file(
                f'{log_folder}{"/" if log_folder else ""}{job_id}.csv', dest_path
            )
            logger.debug(f"Successfully downloaded query results to {dest_path}")
            logger.debug(f"Contents of response: {response}")
        except ExitCodeException:
            raise
        except Exception as e:
            raise errs.FetchError(e)

    def _get_query_status(self, job_id: str):
        """Fetches the query status

        Args:
            job_id: The ID of the associated query to fetch
        """
        result = self.athena.get_query_execution(QueryExecutionId=job_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return result
        elif state == "Failed":
            err_msg = result["QueryExecution"]["Stats"].get("StateChangeReason")
            raise errs.QueryFailed(err_msg)
        return False

    def _execute_query(
        self,
        query: str,
        database: Optional[str] = None,
        log_folder: Optional[str] = None,
    ) -> Dict[str, str]:
        context = {}
        if database:
            context = {"Database": database}

        if log_folder:
            output = f"s3://{self.bucket}/{log_folder.strip('/')}"
        else:
            output = f"s3://{self.bucket}"

        job = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext=context,
            ResultConfiguration={"OutputLocation": output},
        )
        return job
