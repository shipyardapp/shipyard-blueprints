from shipyard_templates import ExitCodeException

EXIT_CODE_TEMP_TABLE_CREATION_ERROR = 101
EXIT_CODE_TEMP_TABLE_INSERT_ERROR = 102
EXIT_CODE_TEMP_TABLE_DROP_ERROR = 103


class TempTableCreationFailed(ExitCodeException):
    def __init__(self, temp_table_name: str):
        self.message = f"Failed to create temporary table {temp_table_name}"
        self.exit_code = EXIT_CODE_TEMP_TABLE_CREATION_ERROR


class TempTableInsertFailed(ExitCodeException):
    def __init__(self, src_table: str, target_table: str):
        self.message = f"Failed to insert data from {src_table} into {target_table}"
        self.exit_code = EXIT_CODE_TEMP_TABLE_INSERT_ERROR


class TempTableDropFailed(ExitCodeException):
    def __init__(self, table_name: str):
        self.message = f"Failed to drop table {table_name}"
        self.exit_code = EXIT_CODE_TEMP_TABLE_DROP_ERROR
