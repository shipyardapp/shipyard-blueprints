from shipyard_templates import ExitCodeException, Database


class SqlServerConnectionError(ExitCodeException):
    def __init__(self, error_msg: Exception):
        self.message = (
            f"Error in connecting to SQL Server. Message from server is {error_msg}"
        )
        self.exit_code = Database.EXIT_CODE_INVALID_CREDENTIALS
