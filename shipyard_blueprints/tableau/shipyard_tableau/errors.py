from shipyard_templates import ExitCodeException, DataVisualization
from typing import Union, Optional


EXIT_CODE_TABLEAU_UNPWD_AUTH_ERROR = 100
EXIT_CODE_TABLEAU_PAT_AUTH_ERROR = 101
EXIT_CODE_TABLEAU_JWT_AUTH_ERROR = 102
EXIT_CODE_TABLEAU_CANNOT_FETCH_WORKBOOK = 103
EXIT_CODE_TABLEAU_CANNOT_FETCH_DATASOURCE = 104
EXIT_CODE_TABLEAU_WORKBOOK_REFRESH_ERROR = 105
EXIT_CODE_TABLEAU_DATASOURCE_REFRESH_ERROR = 106
EXIT_CODE_TABLEAU_VIEW_DOWNLOAD_ERROR = 107
EXIT_CODE_JOB_STATUS_ERROR = 108

EXIT_CODE_FINAL_STATUS_CANCELLED = 210
EXIT_CODE_FINAL_STATUS_ERRORED = 211
EXIT_CODE_STATUS_INCOMPLETE = 212


class TableauUNPWDAuthError(ExitCodeException):
    def __init__(self, err_message: Union[Exception, str]):
        self.message = f"Error when trying to authenticate with Tableau using username and password: {err_message}"
        self.exit_code = EXIT_CODE_TABLEAU_UNPWD_AUTH_ERROR


class TableauPATAuthError(ExitCodeException):
    def __init__(self, err_message: Union[Exception, str]):
        self.message = f"Error when trying to authenticate with Tableau using personal access token: {err_message}"
        self.exit_code = EXIT_CODE_TABLEAU_PAT_AUTH_ERROR


class TableauJWTAuthError(ExitCodeException):
    def __init__(self, err_message: Union[Exception, str]):
        self.message = (
            f"Error when trying to authenticate with Tableau using JWT: {err_message}"
        )
        self.exit_code = EXIT_CODE_TABLEAU_JWT_AUTH_ERROR


# Method exceptions for tableau
class InvalidWorkbookRequest(ExitCodeException):
    def __init__(
        self, err_message: Union[Exception, str], exit_code: Optional[int] = None
    ):
        exit_code = exit_code if exit_code else EXIT_CODE_TABLEAU_CANNOT_FETCH_WORKBOOK
        self.message = f"Unable to fetch Workbook information from Tableau. Invalid workbook request: {err_message}"
        self.exit_code = exit_code


class InvalidDatasourceRequest(ExitCodeException):
    def __init__(
        self, err_message: Union[Exception, str], exit_code: Optional[int] = None
    ):
        exit_code = (
            exit_code if exit_code else EXIT_CODE_TABLEAU_CANNOT_FETCH_DATASOURCE
        )
        self.message = f"Unable to fetch Datasource information from Tableau. Invalid datasource request: {err_message}"
        self.exit_code = exit_code


class InvalidViewRequest(ExitCodeException):
    def __init__(
        self, err_message: Union[Exception, str], exit_code: Optional[int] = None
    ):
        exit_code = exit_code if exit_code else EXIT_CODE_TABLEAU_VIEW_DOWNLOAD_ERROR
        self.message = f"Unable to fetch View information from Tableau. Invalid view request: {err_message}"
        self.exit_code = exit_code
