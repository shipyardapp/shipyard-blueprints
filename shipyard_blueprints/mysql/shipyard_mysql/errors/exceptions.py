from shipyard_templates import ExitCodeException, Database

EXIT_CODE_UPLOAD_ERROR = 101
EXIT_CODE_CHUNK_DOWNLOAD_ERROR = 102


class InvalidMySQLCredentials(ExitCodeException):
    def __init__(self, error_message: Exception):
        self.message = f"Could not authenticate to the provided MySQL server. Message from the server reads: {error_message}"
        self.exit_code = Database.EXIT_CODE_INVALID_CREDENTIALS


class ChunkDownloadError(ExitCodeException):
    def __init__(self, error_message: Exception):
        self.message = f"Error in downloading chunks from MySQL. Message from the server reads: {error_message}"
        self.exit_code = EXIT_CODE_CHUNK_DOWNLOAD_ERROR


class MySqlUploadError(ExitCodeException):
    def __init__(self, error_message: Exception):
        self.message = f"Error attempting to load data into MySQL. Message from the server reads: {error_message}"
        self.exit_code = EXIT_CODE_UPLOAD_ERROR
