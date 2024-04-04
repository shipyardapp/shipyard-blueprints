from typing import Union
from shipyard_templates import ExitCodeException, DataVisualization

EXIT_CODE_SDK_INITIALIZATION_ERROR = 101
EXIT_CODE_DASHBOARD_DOWNLOAD_ERROR = 102
EXIT_CODE_LOOK_DOWNLOAD_ERROR = 103

EXIT_CODE_LOOK_QUERY_ERROR = 203
EXIT_CODE_LOOK_ERROR = 204
EXIT_CODE_LOOK_DASHBOARD_ERROR = 205
EXIT_CODE_SLUG_NOT_FOUND = 206
EXIT_CODE_INVALID_DASHBOARD_ID = 206
EXIT_CODE_INVALID_CREDENTIALS = 207
EXIT_CODE_INVALID_LOOK_ID = 208
EXIT_CODE_INVALID_SLUG = 209


class SdkInitializationError(ExitCodeException):
    def __init__(self, message: Union[str, Exception]):
        self.message = f"Error initializing Looker SDK: {message}"
        self.exit_code = DataVisualization.EXIT_CODE_INVALID_CREDENTIALS


class DashboardDownloadError(ExitCodeException):
    def __init__(self, message: Union[str, Exception]):
        self.message = f"Error downloading dashboard: {message}"
        self.exit_code = EXIT_CODE_DASHBOARD_DOWNLOAD_ERROR


class LookDownloadError(ExitCodeException):
    def __init__(self, message: Union[str, Exception]):
        self.message = f"Error downloading look: {message}"
        self.exit_code = EXIT_CODE_LOOK_DOWNLOAD_ERROR


class InvalidLookID(ExitCodeException):
    def __init__(self, look_id: int):
        self.message = (
            f"The look id {look_id} does not exist, please provide a valid look id"
        )
        self.exit_code = EXIT_CODE_INVALID_LOOK_ID


class SQLCreationError(ExitCodeException):
    def __init__(self, message: Union[str, Exception]):
        self.message = f"Error running create SQL query: {message}"
        self.exit_code = EXIT_CODE_LOOK_QUERY_ERROR
