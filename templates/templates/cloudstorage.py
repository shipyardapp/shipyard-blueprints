
from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class CloudStorage(ABC):
    EXIT_CODE_INVALID_CREDENTIALS = 202

    def __init__(self, **kwargs) -> None:
        self.logger = ShipyardLogger().logger

    @abstractmethod
    def download_files(self):
        pass

    @abstractmethod
    def move_or_rename_files(self):
        pass

    @abstractmethod
    def remove_files(self):
        pass

    @abstractmethod
    def upload_files(self):
        pass

    @abstractmethod
    def connect(self):
        pass
