from .exit_code_exception import ExitCodeException

EXIT_CODE_INVALID_CREDENTIAL = 180
EXIT_CODE_BAD_API_REQUEST = 181


class InvalidCredentialError(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, exit_code=EXIT_CODE_INVALID_CREDENTIAL)


class BadRequestError(ExitCodeException):
    def __init__(self, message):
        super().__init__(message, exit_code=EXIT_CODE_BAD_API_REQUEST)
