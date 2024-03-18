from logging import error
from shipyard_templates import ExitCodeException, Etl

EXIT_CODE_TRIGGER_JOB_ERROR = 101
EXIT_CODE_RUN_STATUS_ERROR = 102
EXIT_CODE_AUTH_ERROR = 103


class TriggerJobError(ExitCodeException):
    def __init__(self, error_message: Exception):
        self.error_message = f"Error in attempting to trigger job. Response from the API: {error_message}"
        self.exit_code = EXIT_CODE_TRIGGER_JOB_ERROR


class GetRunStatusError(ExitCodeException):
    def __init__(self, error_message: Exception, run_counter: int):
        self.error_message = f"Error in attempting to fetch the status for run {run_counter}. Response from the API: {error_message}"
        self.exit_code = EXIT_CODE_RUN_STATUS_ERROR


class AuthenticationError(ExitCodeException):
    def __init__(self, err_message: Exception):
        self.error_message = f"Error in attmepting to authenticate to the Coalesce API. Make sure you provide a valid token. Error message reads: {err_message}"
        self.exit_code = EXIT_CODE_AUTH_ERROR


class UnknownRunStatus(ExitCodeException):
    def __init__(self, status: str):
        self.error_message = f"Unknown run status reported from Coalesce: {status}"
        self.exit_code = Etl.EXIT_CODE_UNKNOWN_STATUS
