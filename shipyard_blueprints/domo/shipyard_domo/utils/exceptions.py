from shipyard_templates import ExitCodeException

EXIT_CODE_FINAL_STATUS_SUCCESS = 0

EXIT_CODE_COLUMN_MISMATCH = 219
EXIT_CODE_INVALID_DATA_TYPE = 218
EXIT_CODE_FILE_NOT_FOUND = 214
EXIT_CODE_DATASET_NOT_FOUND = 215

EXIT_CODE_DUPLICATE_DATASET = 216
EXIT_CODE_NON_API_DATASET = 217
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_SCHEMA_UPDATE_ERROR = 202
EXIT_CODE_REFRESH_ERROR = 206
EXIT_CODE_INCORRECT_CARD_TYPE = 205
EXIT_CODE_EXECUTION_ID_NOT_FOUND = 204

EXIT_CODE_CARD_EXPORT_ERROR = 101
EXIT_CODE_CARD_FETCH_ERROR = 102
EXIT_CODE_UPLOAD_STREAM_ERROR = 103

EXIT_CODE_UNKNOWN_ERROR = 249

# sync statuses
EXIT_CODE_FINAL_STATUS_INVALID = 210
EXIT_CODE_FINAL_STATUS_CANCELLED = 211
EXIT_CODE_STATUS_INCOMPLETE = 212
EXIT_CODE_UNKNOWN_STATUS = 213


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


class DatasetNotFound(ExitCodeException):
    def __init__(self, dataset_id: str, error_msg: str):
        self.message = f"The dataset {dataset_id} could not be found. Message from the API: {error_msg}"
        self.exit_code = EXIT_CODE_DATASET_NOT_FOUND


class RefreshError(ExitCodeException):
    def __init__(self, error_msg: str):
        self.message = f"Error in refreshing dataset. Message from the API: {error_msg}"
        self.exit_code = EXIT_CODE_REFRESH_ERROR


class ExecutionDetailsNotFound(ExitCodeException):
    def __init__(self, dataset_id: str):
        self.message = f"Execution details for stream associated with dataset ID {dataset_id} not found"
        self.exit_code = EXIT_CODE_EXECUTION_ID_NOT_FOUND


class CardExportError(ExitCodeException):
    def __init__(self, card_id: str, error_msg: str):
        self.message = (
            f"Error in exporting card {card_id}. Message from the API: {error_msg}"
        )
        self.exit_code = EXIT_CODE_CARD_EXPORT_ERROR


class CardFetchError(ExitCodeException):
    def __init__(self, card_id: str, error_msg: str):
        self.message = f"Error in fetching data for card {card_id}. Message from the API: {error_msg}"
        self.exit_code = EXIT_CODE_CARD_FETCH_ERROR


class UploadStreamError(ExitCodeException):
    def __init__(self, dataset_id: str, error_msg: str):
        self.message = f"Error in uploading stream to dataset {dataset_id}. Message from the API: {error_msg}"
        self.exit_code = EXIT_CODE_UPLOAD_STREAM_ERROR
