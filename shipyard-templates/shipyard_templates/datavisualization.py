from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class DataVisualization(ABC):
    def __init__(self, **kwargs) -> None:
        self.logger = ShipyardLogger().logger

    @abstractmethod
    def connect(self, **kwargs):
        pass
