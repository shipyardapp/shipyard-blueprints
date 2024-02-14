from shipyard_templates import ExitCodeException

from shipyard_bigquery.bigquery import BigQueryClient


class InvalidSchema(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = BigQueryClient.EXIT_CODE_INVALID_SCHEMA


class FetchError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = BigQueryClient.EXIT_CODE_FETCH_ERROR


class QueryError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = BigQueryClient.EXIT_CODE_QUERY_ERROR


class DownloadToGcsError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = BigQueryClient.EXIT_CODE_DOWNLOAD_TO_GCS_ERROR


class TempTableCreationError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = BigQueryClient.EXIT_CODE_TEMP_TABLE_CREATION_ERROR


class SchemaFormatError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = BigQueryClient.EXIT_CODE_SCHEMA_FORMATTING_ERROR


class SchemaValidationError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = BigQueryClient.EXIT_CODE_SCHEMA_VALIDATION_ERROR
