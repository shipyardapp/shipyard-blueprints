from shipyard_templates import ExitCodeException, GoogleDatabase

EXIT_CODE_FETCH_ERROR = 101
EXIT_CODE_QUERY_ERROR = 102
EXIT_CODE_DOWNLOAD_TO_GCS_ERROR = 103
EXIT_CODE_TEMP_TABLE_CREATION_ERROR = 104
EXIT_CODE_SCHEMA_FORMATTING_ERROR = 105
EXIT_CODE_SCHEMA_VALIDATION_ERROR = 106


class InvalidSchema(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = GoogleDatabase.EXIT_CODE_INVALID_SCHEMA


class FetchError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_FETCH_ERROR


class QueryError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_QUERY_ERROR


class DownloadToGcsError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_DOWNLOAD_TO_GCS_ERROR


class TempTableCreationError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_TEMP_TABLE_CREATION_ERROR


class SchemaFormatError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_SCHEMA_FORMATTING_ERROR


class SchemaValidationError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_SCHEMA_VALIDATION_ERROR
