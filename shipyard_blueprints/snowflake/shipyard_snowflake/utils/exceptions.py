from shipyard_templates import ExitCodeException


# These are custom exceptions specific to shipyard-snowflake
class SnowflakeToPandasError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class PandasToSnowflakeError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class SchemaInferenceError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class PutError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class CopyIntoError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class QueryExecutionError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class DownloadError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class CreateTableError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code
