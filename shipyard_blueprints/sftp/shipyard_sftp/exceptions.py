from shipyard_templates import CloudStorage
from shipyard_templates import ExitCodeException

EXIT_CODE_DELETE_ERROR = 100


class InvalidCredentialsError(ExitCodeException):
    def __init__(self, raised_from: Exception = None, *args, **kwargs):
        message = (
            "INVALID CREDENTIALS: Check your credentials and try again. If you continue to experience "
            "issues please contact your SFTP Admin."
        )
        if raised_from:
            message += f"Message from server...\n{raised_from.__class__.__name__}: {raised_from}"

        super().__init__(message, CloudStorage.EXIT_CODE_INVALID_CREDENTIALS)


class UnknownException(ExitCodeException):
    def __init__(self, raised_from: Exception = None, *args, **kwargs):
        message = "UNKNOWN: An unexpected error occurred."
        if raised_from:
            message += (
                f"Message from server...{raised_from.__class__.__name__}: {raised_from}"
            )

        super().__init__(message, CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


class UploadError(ExitCodeException):
    def __init__(self, raised_from: Exception = None, *args, **kwargs):
        message = "UPLOAD FAILED: An unexpected error occurred. Please try again."
        if raised_from:
            message += f"Message from server...\n{raised_from.__class__.__name__}: {raised_from}"

        super().__init__(message, CloudStorage.EXIT_CODE_UPLOAD_ERROR)


class FileMatchException(ExitCodeException):
    def __init__(self, raised_from: Exception = None, *args, **kwargs):
        message = (
            "FILE MATCH ISSUE: An unexpected error occurred when attempting to retrieve matching files from the "
            "server. Please try again."
        )
        if raised_from:
            message += (
                f"Message from server...\n{raised_from.__class__}: {raised_from}."
            )
        super().__init__(message, CloudStorage.EXIT_CODE_FILE_MATCH_ERROR)


class DownloadException(ExitCodeException):
    def __init__(self, raised_from: Exception = None, *args, **kwargs):
        message = (
            "DOWNLOAD ISSUE: An unexpected error occurred when attempting to download files from the "
            "server. Please try again."
        )
        if raised_from:
            message += (
                f"Message from server...\n{raised_from.__class__}: {raised_from}."
            )
        super().__init__(message, CloudStorage.EXIT_CODE_DOWNLOAD_ERROR)


class FileNotFound(ExitCodeException):
    def __init__(self, raised_from: Exception = None, *args, **kwargs):
        message = "FILE NOT FOUND: File not found in the server. Confirm the file exists and try again."
        if raised_from:
            message += (
                f"Message from server...\n{raised_from.__class__}: {raised_from}."
            )
        super().__init__(message, CloudStorage.EXIT_CODE_FILE_NOT_FOUND)


class DeleteException(ExitCodeException):
    def __init__(self, raised_from: Exception = None, *args, **kwargs):
        message = (
            "DELETION ISSUE: An unexpected error occurred when attempting to delete files from the server. "
            "Please try again."
        )
        if raised_from:
            message += (
                f"Message from server...\n{raised_from.__class__}: {raised_from}."
            )
        super().__init__(message, EXIT_CODE_DELETE_ERROR)
