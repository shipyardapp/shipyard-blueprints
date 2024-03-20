from shipyard_templates import ExitCodeException, Database


class SqlServerConnectionError(ExitCodeException):
    def __init__(self, error_msg: Exception):
        self.message = (
            f"Error in connecting to SQL Server. Message from server is {error_msg}"
        )
        self.exit_code = Database.EXIT_CODE_INVALID_CREDENTIALS


# class UploadError(ExitCodeException):
#     def __init__(self, table: str, error_msg: Exception):
#         self.message = (
#             f"Error in loading data to {table}. Message from the server is: {error_msg}"
#         )
#         self.exit_code = EXIT_CODE_UPLOAD_ERROR
#
#
# class FetchError(ExitCodeException):
#     def __init__(self, error_msg: Exception):
#         self.message = f"Error in downloading data from SQL Server. Message from the server is: {error_msg}"
#         self.exit_code = EXIT_CODE_UPLOAD_ERROR
#
#
# class QueryError(ExitCodeException):
#     def __init__(self, error_msg: Exception):
#         self.message = (
#             f"Error in executing query. Message from the server is: {error_msg}"
#         )
#         self.exit_code = EXIT_CODE_UPLOAD_ERROR
