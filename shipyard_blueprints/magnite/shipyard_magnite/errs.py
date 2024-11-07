from shipyard_templates import ExitCodeException, errors

EXIT_CODE_INVALID_ARGS = 200
EXIT_CODE_UPDATE_ERROR = 201
EXIT_CODE_FILE_NOT_FOUND = 202


class UpdateError(ExitCodeException):
    def __init__(self, message, exit_code):
        super().__init__(message, exit_code)
        self.message = f"Error in updating item: {message}"
        self.exit_code = exit_code
