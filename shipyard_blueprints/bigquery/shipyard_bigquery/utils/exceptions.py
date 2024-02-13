from shipyard_templates import ExitCodeException


class DatasetNotFound(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class InvalidSchema(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class SchemaMismatch(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class FetchError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class QueryError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class DownloadToGcsError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class TempTableCreationError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class SchemaFormatError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code


class SchemaValidationError(ExitCodeException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message, exit_code)
        self.message = message
        self.exit_code = exit_code
