from shipyard_templates import ExitCodeException

## error codes
EXIT_CODE_BAD_REQUEST = 106
EXIT_CODE_UNKNOWN_ERROR = 249

## run status codes
## these are provided here: https://learn.hex.tech/docs/develop-logic/hex-api/api-reference#operation/GetRunStatus
EXIT_CODE_PENDING = 101
EXIT_CODE_RUNNING = 102
EXIT_CODE_ERRORED = 103
EXIT_CODE_COMPLETED = 0  ## success
EXIT_CODE_KILLED = 104
EXIT_CODE_UNABLE_TO_ALLOCATE_KERNEL = 105


class RunProjectError(ExitCodeException):
    def __init__(self, project_id: str, err_msg: Exception):
        self.message = f"Error in triggering run for project {project_id}. Response from the API is: {err_msg}"
        self.exit_code = EXIT_CODE_BAD_REQUEST


class GetRunStatusError(ExitCodeException):
    def __init__(self, project_id: str, run_id: str, err_msg: Exception):
        self.message = f"Error in fetching the run id {run_id} for project {project_id}. Response from the api reads: {err_msg}"
        self.exit_code = EXIT_CODE_BAD_REQUEST
