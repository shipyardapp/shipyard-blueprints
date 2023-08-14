from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class Database(ABC):
    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_INVALID_ACCOUNT = 201  # snowflake specific
    EXIT_CODE_INVALID_WAREHOUSE = 202
    EXIT_CODE_INVALID_DATABASE = 203
    EXIT_CODE_INVALID_SCHEMA = 204  # snowflake specific
    EXIT_CODE_INVALID_QUERY = 205
    EXIT_CODE_NO_RESULTS = 206
    EXIT_CODE_FILE_NOT_FOUND = 205
    EXIT_CODE_INVALID_UPLOAD_VALUE = 206
    EXIT_CODE_INVALID_UPLOAD_COLUMNS = 207
    EXIT_CODE_INVALID_ARGUMENTS = 208
    EXIT_CODE_INVALID_DATA_TYPES = 209

    def __init__(self, user: str, pwd: str, **kwargs) -> None:
        self.logger = ShipyardLogger().logger

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute_query(self, **kwargs):
        pass

    @abstractmethod
    def fetch(self, **kwargs):
        pass

    @abstractmethod
    def upload(self, **kwargs):
        pass


class GoogleDatabase:
    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_INVALID_ACCOUNT = 201  # snowflake specific
    EXIT_CODE_INVALID_WAREHOUSE = 202
    EXIT_CODE_INVALID_DATABASE = 203
    EXIT_CODE_INVALID_SCHEMA = 204  # snowflake specific
    EXIT_CODE_INVALID_QUERY = 205
    EXIT_CODE_NO_RESULTS = 206
    EXIT_CODE_FILE_NOT_FOUND = 205
    EXIT_CODE_INVALID_UPLOAD_VALUE = 206
    EXIT_CODE_INVALID_UPLOAD_COLUMNS = 207
    EXIT_CODE_INVALID_ARGUMENTS = 208
    EXIT_CODE_INVALID_DATA_TYPES = 209

    def __init__(self, service_account: str, **kwargs) -> None:
        self.service_account = service_account
        self.logger = ShipyardLogger().logger

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute_query(self, **kwargs):
        pass

    @abstractmethod
    def fetch(self, **kwargs):
        pass

    @abstractmethod
    def upload(self, **kwargs):
        pass
