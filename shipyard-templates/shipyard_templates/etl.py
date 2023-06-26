from abc import ABC, abstractmethod
from .shipyard_logger import ShipyardLogger


class Etl(ABC):
    # Class level exit codes
    # general exit codes
    EXIT_CODE_FINAL_STATUS_COMPLETED = 0
    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_BAD_REQUEST = 201
    # trigger sync exit codes
    EXIT_CODE_SYNC_REFRESH_ERROR = 202
    EXIT_CODE_SYNC_ALREADY_RUNNING = 203
    EXIT_CODE_SYNC_INVALID_SOURCE_ID = 204
    EXIT_CODE_SYNC_INVALID_POKE_INTERVAL = 205
    EXIT_CODE_SYNC_CHECK_ERROR = 220
    # verify status exit codes
    EXIT_CODE_FINAL_STATUS_INCOMPLETE = 210
    EXIT_CODE_FINAL_STATUS_ERRORED = 211
    EXIT_CODE_FINAL_STATUS_INVALID = 212
    EXIT_CODE_FINAL_STATUS_CANCELLED = 213
    EXIT_CODE_FINAL_STATUS_PENDING = 214
    EXIT_CODE_UNKNOWN_STATUS = 215
    EXIT_CODE_UNKNOWN_ERROR = 299

    # API TIMEOUT
    TIMEOUT = 30

    def __init__(self, access_token: str, **kwargs) -> None:
        self.logger = ShipyardLogger().logger
        self.access_token = access_token

    @abstractmethod
    def trigger_sync(self, **kwargs):
        pass

    @abstractmethod
    def determine_sync_status(self, **kwargs):
        pass
