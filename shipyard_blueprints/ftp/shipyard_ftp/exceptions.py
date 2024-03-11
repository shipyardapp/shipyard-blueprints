from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

logger = ShipyardLogger().get_logger()
EXIT_CODE_FTP_MOVE_ERROR = 100
EXIT_CODE_FTP_DELETE_ERROR = 101
EXIT_CODE_UPLOAD_ERROR = CloudStorage.EXIT_CODE_UPLOAD_ERROR
EXIT_CODE_NO_MATCHES_FOUND = CloudStorage.EXIT_CODE_FILE_MATCH_ERROR
EXIT_CODE_INCORRECT_CREDENTIALS = CloudStorage.EXIT_CODE_INVALID_CREDENTIALS
EXIT_CODE_DOWNLOAD_ERROR = CloudStorage.EXIT_CODE_DOWNLOAD_ERROR


class MoveError(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, EXIT_CODE_FTP_MOVE_ERROR)


class UploadError(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, EXIT_CODE_UPLOAD_ERROR)


class NoMatchFound(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, EXIT_CODE_NO_MATCHES_FOUND)


class InvalidCredentials(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, EXIT_CODE_INCORRECT_CREDENTIALS)


class DownloadError(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, EXIT_CODE_DOWNLOAD_ERROR)


class DeleteError(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, EXIT_CODE_FTP_DELETE_ERROR)
