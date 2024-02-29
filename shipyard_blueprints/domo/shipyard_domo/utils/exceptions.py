from shipyard_templates import ExitCodeException

EXIT_CODE_COLUMN_MISMATCH = 219
EXIT_CODE_INVALID_DATA_TYPE = 218
EXIT_CODE_FILE_NOT_FOUND = 214
EXIT_CODE_DATASET_NOT_FOUND = 215

EXIT_CODE_DUPLICATE_DATASET = 216
EXIT_CODE_NON_API_DATASET = 217
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_SCHEMA_UPDATE_ERROR = 202


class ColumnMismatch(ExitCodeException):
    def __init__(self):
        self.message = "Error: The number data types does not equal the number of columns. Please number of domo data types provided matches the number of columns"
        self.exit_code = EXIT_CODE_COLUMN_MISMATCH


class InvalidDatatype(ExitCodeException):
    def __init__(self, datatype: str):
        self.message = f"Error: {datatype} is not a valid domo data type. Please ensure one of STRING, DECIMAL, LONG, DOUBLE, DATE, DATETIME is selected"
        self.exit_code = EXIT_CODE_COLUMN_MISMATCH


class InvalidClientIdAndSecret(ExitCodeException):
    def __init__(self):
        self.message = "The provided client ID and secret key are incorrect."
        self.exit_code = EXIT_CODE_INVALID_CREDENTIALS


class SchemaUpdateError(ExitCodeException):
    def __init__(self, dataset_id: str, error_msg: str):
        self.message = f"Error in updating the schema for dataset: {dataset_id}. Message from the server: {error_msg}"
        self.exit_code = EXIT_CODE_SCHEMA_UPDATE_ERROR
