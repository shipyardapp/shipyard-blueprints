from shipyard_templates import ExitCodeException, CloudStorage

EXIT_CODE_MOVE_ERROR = 101
EXIT_CODE_REMOVE_ERR0R = 102
EXIT_CODE_DOWNLOAD_ERROR = 103


class InvalidCredentials(ExitCodeException):
    def __init__(self):
        self.message = "The provided credentials are incorrect, pleasure ensure you enter valid access key and secret"
        self.exit_code = CloudStorage.EXIT_CODE_INVALID_CREDENTIALS


class UploadError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = CloudStorage.EXIT_CODE_UPLOAD_ERROR


class MoveError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_MOVE_ERROR


class RemoveError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_REMOVE_ERR0R


class DownloadError(ExitCodeException):
    def __init__(self, message: str):
        self.message = message
        self.exit_code = EXIT_CODE_MOVE_ERROR
