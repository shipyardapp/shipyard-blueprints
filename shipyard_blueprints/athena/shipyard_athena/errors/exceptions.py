from shipyard_templates import ExitCodeException, Database
from typing import Dict

EXIT_CODE_ATHENA_ACCESS_ERROR = 101
EXIT_CODE_S3_BUCKET_ACCESS_ERRROR = 102
EXIT_CODE_QUERY_FAILED = 103
EXIT_CODE_QUERY_EXECUTION_ERROR = 104
EXIT_CODE_FETCH_ERROR = 105
EXIT_CODE_QUERY_CANCELLED = 106


class InvalidAthenaAccess(ExitCodeException):
    def __init__(self, err_message: Exception):
        self.message = f"Error in connecting to Athena. Verify that the user associated with the provided access key has the appropriate permissions outlined in the authorization guide. Error message from AWS reads: {err_message}"
        self.exit_code = EXIT_CODE_ATHENA_ACCESS_ERROR


class InvalidS3Access(ExitCodeException):
    def __init__(self, bucket: str, err_message: Exception):
        self.message = f"Error in connecting to the bucket {bucket}. Verify that the user associated with the provided access key has the appropriate permissions outlined in the authorization guide. Error message from AWS reads: {err_message}"
        self.exit_code = EXIT_CODE_S3_BUCKET_ACCESS_ERRROR


class QueryFailed(ExitCodeException):
    def __init__(self, err_message: Exception):
        self.message = (
            f"Query execution failed. Message from Amazon Athena: {err_message}"
        )
        self.exit_code = EXIT_CODE_QUERY_FAILED


class QueryExecutionError(ExitCodeException):
    def __init__(self, err_message: Exception):
        self.message = f"Query execution failed. Response from Athena: {err_message}"
        self.exit_code = EXIT_CODE_QUERY_EXECUTION_ERROR


class FetchError(ExitCodeException):
    def __init__(self, err_message: Exception):
        self.message = f"Error in attempting to download query results to file. Message from AWS: {err_message}"
        self.exit_code = EXIT_CODE_FETCH_ERROR


class QueryCancelled(ExitCodeException):
    def __init__(self):
        self.message = "The provided query has been cancelled, aborting fetch job now."
        self.exit_code = EXIT_CODE_QUERY_CANCELLED
