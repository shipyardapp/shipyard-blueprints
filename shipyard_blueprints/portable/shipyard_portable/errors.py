from shipyard_templates import ExitCodeException, Etl


class UnauthorizedError(ExitCodeException):
    def __init__(self, message: str):
        self.message = f"Failed to connect to Portable API. Ensure that the API key provided is correct. Message from Portable API: {message}"
        self.exit_code = Etl.EXIT_CODE_INVALID_CREDENTIALS


class BadRequestError(ExitCodeException):
    def __init__(self, message: str):
        self.message = (
            f"The request is malformed. Message from the Portable API: {message}"
        )
        self.exit_code = Etl.EXIT_CODE_BAD_REQUEST
