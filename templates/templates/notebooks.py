from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class Notebooks(ABC):
    def __init__(self) -> None:
        self.logger = ShipyardLogger().logger

    @abstractmethod
    def connect():
        pass
