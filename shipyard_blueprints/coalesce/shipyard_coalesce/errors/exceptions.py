from shipyard_templates import ExitCodeException, Etl

EXIT_CODE_TRIGGER_JOB_ERROR = 101
EXIT_CODE_RUN_STATUS_ERROR = 102
EXIT_CODE_AUTH_ERROR = 103
EXIT_CODE_INVALID_REGION = 104


class TriggerJobError(ExitCodeException):
    def __init__(self, message: Exception):
        self.message = (
            f"Error in attempting to trigger job. Response from the API: {message}"
        )
        self.exit_code = EXIT_CODE_TRIGGER_JOB_ERROR


class GetRunStatusError(ExitCodeException):
    def __init__(self, message: Exception, run_counter: int):
        self.message = (
            f"Error in attempting to fetch the status for run {run_counter}. "
            f"Response from the API: {message}"
        )
        self.exit_code = EXIT_CODE_RUN_STATUS_ERROR


class AuthenticationError(ExitCodeException):
    def __init__(self, message: Exception):
        self.message = (
            f"Error in attempting to authenticate to the Coalesce API. Make sure you provide a valid "
            f"token. Error message reads: {message}"
        )
        self.exit_code = EXIT_CODE_AUTH_ERROR


class UnknownRunStatus(ExitCodeException):
    def __init__(self, status: str):
        self.message = f"Unknown run status reported from Coalesce: {status}"
        self.exit_code = Etl.EXIT_CODE_UNKNOWN_STATUS


class InvalidRegion(ExitCodeException):
    def __init__(self, region: str):
        self.message = f"Invalid region provided: {region}. Region must be one of: gcp-us-central-1, gcp-eu-west-3, gcp-austrailia-southeast-1, aws-us-east-1, aws-us-west-2, az-us-west-2, or az-us-east-2"
        self.exit_code = EXIT_CODE_INVALID_REGION
