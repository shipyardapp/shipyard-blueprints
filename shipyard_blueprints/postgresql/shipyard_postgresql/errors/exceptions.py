from logging import error
from shipyard_templates import Database, ExitCodeException

EXIT_CODE_UPLOAD_ERROR = 101
EXIT_CODE_CHUNK_DOWNLOAD_ERROR = 102


class InvalidCredentials(ExitCodeException):
    def __init__(self, error_message: Exception):
        self.message = f"Error in connecting to Postgres. Message from the server reads: {error_message}"
        self.exit_code = Database.EXIT_CODE_INVALID_CREDENTIALS


class PostgresUploadError(ExitCodeException):
    def __init__(self, error_message: Exception):
        self.message = f"Error attempting to load data into Postgres. Message from the server reads: {error_message}"
        self.exit_code = EXIT_CODE_UPLOAD_ERROR


class ChunkDownloadError(ExitCodeException):
    def __init__(self, error_message: Exception):
        self.message = f"Error in downloading chunks for Postgres. Message from the server reads: {error_message}"
        self.exit_code = EXIT_CODE_CHUNK_DOWNLOAD_ERROR
