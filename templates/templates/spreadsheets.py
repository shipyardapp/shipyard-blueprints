from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class Spreadsheets(ABC):

    def __init__(self, **kwargs) -> None:
        self.logger = ShipyardLogger().logger
        super().__init__()

    @abstractmethod
    def connect(self):
        pass
