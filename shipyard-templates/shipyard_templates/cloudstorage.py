from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class CloudStorage(ABC):
    EXIT_CODE_INVALID_CREDENTIALS = 202

    def __init__(self, **kwargs) -> None:
        self.logger = ShipyardLogger().logger

    @abstractmethod
    def download(self):
        pass

    @abstractmethod
    def move(self):
        pass

    @abstractmethod
    def remove(self):
        pass

    @abstractmethod
    def upload(self):
        pass

    @abstractmethod
    def connect(self):
        pass
