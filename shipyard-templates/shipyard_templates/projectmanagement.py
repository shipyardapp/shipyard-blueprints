from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class ProjectManagement(ABC):
    # Class level exit codes
    # general exit codes
    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_BAD_REQUEST = 201
    EXIT_CODE_UNKNOWN_ERROR = 299

    # API TIMEOUT
    EXIT_CODE_API_TIMEOUT = 30
    def __init__(self, access_token: str, **kwargs) -> None:
        self.logger = ShipyardLogger().logger
        self.access_token = access_token

    @abstractmethod
    def create_ticket(self, **kwargs):
        pass

    @abstractmethod
    def get_ticket(self, **kwargs):
        pass

    @abstractmethod
    def update_ticket(self, **kwargs):
        pass

    @abstractmethod
    def connect(self, **kwargs):
        pass
