from shipyard_templates import ExitCodeException

EXIT_CODE_MISSING_PROJECT_ID = 101
EXIT_CODE_INVALID_FILE_TYPE = 102
EXIT_CODE_UNAUTHORIZED_ACCESS = 103
EXIT_CODE_LIST_FLEET_RUNS_ERROR = 104
EXIT_CODE_LIST_VOYAGES_ERROR = 105
EXIT_CODE_VOYAGE_EXPORT_ERROR = 106
EXIT_CODE_TRIGGER_FLEET_ERROR = 107
EXIT_CODE_UNKNOWN_ERROR = 249


class MissingProjectID(ExitCodeException):
    def __init__(self):
        self.message = "Project ID is required in order to perform this operation"
        self.exit_code = EXIT_CODE_MISSING_PROJECT_ID


class InvalidFileType(ExitCodeException):
    def __init__(self):
        self.message = "Invalid file type, valid choices are csv and json"
        self.exit_code = 102


class UnauthorizedAccess(ExitCodeException):
    def __init__(self):
        self.message = "Unauthorized access, ensure that your API key has the correct access for the fleet and project provided"
        self.exit_code = EXIT_CODE_UNAUTHORIZED_ACCESS
