from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class Spreadsheets(ABC):
    EXIT_CODE_INVALID_TOKEN = 200 
    EXIT_CODE_INVALID_DATABASE_ID = 201
    EXIT_CODE_UPLOAD_ERROR = 202    
    EXIT_CODE_DOWNLOAD_ERROR = 203
    EXIT_CODE_UNKNWON_ERROR = 299
    def __init__(self, **kwargs) -> None:
        self.logger = ShipyardLogger().logger
        super().__init__()

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod 
    def upload(self):
        pass

    @abstractmethod 
    def download(self):
        pass

