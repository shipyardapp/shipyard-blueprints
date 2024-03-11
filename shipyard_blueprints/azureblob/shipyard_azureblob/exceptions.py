from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

logger = ShipyardLogger().get_logger()
EXIT_CODE_MOVE_ERROR = 100
EXIT_CODE_DELETE_ERROR = 101


class UnknownException(ExitCodeException):
    def __init__(self, exception: Exception = None):
        if exception:
            message = f"An unexpected exception occurred: {exception.__class__.__name__}: {exception}"
        else:
            message = "An unexpected exception occurred"
        super().__init__(message, CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


class InvalidCredentialsError(ExitCodeException):
    def __init__(self, exception: Exception = None):
        if exception:
            message = f"Invalid credentials: Message from Azure: {exception.__class__.__name__}: {exception}"
        else:
            message = "Invalid credentials"
        super().__init__(message, CloudStorage.EXIT_CODE_INVALID_CREDENTIALS)


class InvalidInputError(ExitCodeException):
    def __init__(self, message: str):
        super().__init__(message, CloudStorage.EXIT_CODE_INVALID_INPUT)


class MoveError(ExitCodeException):
    def __init__(self, message: str):
        super().__init__(message, EXIT_CODE_MOVE_ERROR)


class NoFilesFoundError(ExitCodeException):
    def __init__(self, message: str):
        super().__init__(message, CloudStorage.EXIT_CODE_FILE_NOT_FOUND)


class DeleteError(ExitCodeException):
    def __init__(self, message: str):
        super().__init__(message, EXIT_CODE_DELETE_ERROR)


class UploadError(ExitCodeException):
    def __init__(self, message: str):
        super().__init__(message, CloudStorage.EXIT_CODE_UPLOAD_ERROR)
