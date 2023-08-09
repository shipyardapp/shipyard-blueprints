from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger
from .exit_code_exception import ExitCodeException


class Crm(ABC):
    # Class level exit codes

    # GENERIC EXIT CODES
    EXIT_CODE_INVALID_CREDENTIALS = 201
    EXIT_CODE_BAD_REQUEST = 202
    EXIT_CODE_UNKNOWN_ERROR = 203
    EXIT_CODE_RATE_LIMIT = 204
    EXIT_CODE_SERVICE_UNAVAILABLE = 205
    # API TIMEOUT
    TIMEOUT = 30

    def __init__(self, access_token: str, **kwargs) -> None:
        self.logger = ShipyardLogger().logger
        self.access_token = access_token

    @abstractmethod
    def connect(self, **kwargs):
        pass
