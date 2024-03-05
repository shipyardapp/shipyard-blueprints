from shipyard_templates import ExitCodeException
from shipyard_templates.database import DatabricksDatabase

EXIT_CODE_VOLUME_CREATION = 101
EXIT_CODE_VOLUME_SQL = 102
EXIT_CODE_VOLUME_UPLOAD_ERROR = 103
EXIT_CODE_COPY_INTO_ERROR = 104
EXIT_CODE_REMOVE_VOLUME_ERROR = 105
EXIT_CODE_SCHEMA_CREATION_ERROR = 106
EXIT_CODE_TABLE_DNE = 107
EXIT_CODE_APPEND_ERROR = 108
EXIT_CODE_FILE_NOT_FOUND = DatabricksDatabase.EXIT_CODE_FILE_NOT_FOUND
EXIT_CODE_INVALID_QUERY = DatabricksDatabase.EXIT_CODE_INVALID_QUERY


class VolumeCreationError(ExitCodeException):
    def __init__(self, volume: str, error_msg: str):
        self.message = (
            f"Error in creating volume {volume}. Message from Databricks: {error_msg}"
        )
        self.exit_code = EXIT_CODE_VOLUME_CREATION


class VolumeSqlError(ExitCodeException):
    def __init__(
        self,
    ):
        self.message = "Schema must be provided if the catalog is provided"
        self.exit_code = EXIT_CODE_VOLUME_SQL


class VolumeUploadError(ExitCodeException):
    def __init__(self, volume: str, error_msg: str):
        self.message = f"Error when trying to load file to volume {volume}. Message from Databricks: {error_msg}"
        self.exit_code = EXIT_CODE_VOLUME_UPLOAD_ERROR


class CopyIntoError(ExitCodeException):
    def __init__(self, volume_path: str, table_path: str, error_msg: str):
        self.message = f"Error in copying data from volume {volume_path} into {table_path}. Message from Databricks: {error_msg}"
        self.exit_code = EXIT_CODE_COPY_INTO_ERROR


class RemoveVolumeError(ExitCodeException):
    def __init__(self, volume_path: str, error_msg: str):
        self.message = f"Error in removing volume {volume_path}. Message from Databricks: {error_msg}"
        self.exit_code = EXIT_CODE_REMOVE_VOLUME_ERROR


class SchemaCreationError(ExitCodeException):
    def __init__(self):
        self.message = "Error in creating schema"
        self.exit_code = EXIT_CODE_SCHEMA_CREATION_ERROR


class TableDNE(ExitCodeException):
    def __init__(self, table_name: str):
        self.message = "Table does not exist. If loading a table for the first time, please select the `replace` option"
        self.exit_code = EXIT_CODE_TABLE_DNE
