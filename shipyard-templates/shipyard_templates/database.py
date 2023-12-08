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


class DatabricksDatabase:
    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_INVALID_WAREHOUSE = 202
    EXIT_CODE_INVALID_DATABASE = 203
    EXIT_CODE_INVALID_SCHEMA = 204
    EXIT_CODE_INVALID_QUERY = 205
    EXIT_CODE_NO_RESULTS = 206
    EXIT_CODE_FILE_NOT_FOUND = 205
    EXIT_CODE_INVALID_UPLOAD_VALUE = 206
    EXIT_CODE_INVALID_UPLOAD_COLUMNS = 207
    EXIT_CODE_INVALID_ARGUMENTS = 208
    EXIT_CODE_INVALID_DATA_TYPES = 209
    EXIT_CODE_INSERT_STATEMENT_ERROR = 210
    EXIT_CODE_CREATE_STATEMENT_ERROR = 211
    EXIT_CODE_UNKNOWN_ERROR = 249

    def __init__(
        self, server_host: str, http_path: str, access_token: str, **kwargs
    ) -> None:
        self.server_host = server_host
        self.http_path = http_path
        self.access_token = access_token
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
