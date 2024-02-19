from shipyard_templates import ShipyardLogger, Messaging, ExitCodeException

EXIT_CODE_INVALID_INPUT = Messaging.EXIT_CODE_INVALID_INPUT
EXIT_CODE_FILE_NOT_FOUND = Messaging.EXIT_CODE_FILE_NOT_FOUND
EXIT_CODE_INVALID_CREDENTIALS = Messaging.EXIT_CODE_INVALID_CREDENTIALS
EXIT_CODE_MSG_OBJECT_CREATION_FAILED = 100
EXIT_CODE_CONDITION_NOT_MET = 101
EXIT_CODE_INVALID_METHOD = 102
EXIT_CODE_TIMEOUT = 103

logger = ShipyardLogger().get_logger()


class InvalidInputError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_INVALID_INPUT)


class MessageObjectCreationError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_MSG_OBJECT_CREATION_FAILED)


class InvalidFileInputError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_FILE_NOT_FOUND)


class ConditionNotMetError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_CONDITION_NOT_MET)


class InvalidSendMethodError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_INVALID_METHOD)


class InvalidCredentialsError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_INVALID_CREDENTIALS)


class ServerTimeoutError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_TIMEOUT)


class UnhandledExceptionError(ExitCodeException):
    def __init__(self, message: str) -> None:
        super().__init__(message, EXIT_CODE_INVALID_INPUT)


def handle_exceptions(method):
    def exception_handler(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as e:
            if isinstance(e, TimeoutError):
                raise ServerTimeoutError(
                    "Connection to the server timed out. Please check port and host arguments"
                )
            elif isinstance(e, InvalidCredentialsError):
                raise InvalidCredentialsError(
                    f"Invalid credentials provided. Please check your username and "
                    f"password. Message from server: {str(e)}"
                )

            else:
                raise UnhandledExceptionError(
                    f"An unexpected error occurred: {type(e).__name__} {str(e)}"
                )

    return exception_handler
