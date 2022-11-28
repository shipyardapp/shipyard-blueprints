import logging
from abc import ABC, abstractmethod


class EtlMeta(type):
    def __new__(cls, name, bases, body):
        # ensure that execute_sync is a method in the derived class
        if 'trigger_sync' not in body:
            logging.error("trigger_sync is a required method for this class")
            raise TypeError()

        if 'determine_sync_status' not in body:
            logging.error(
                "verify_sync_status is a required method for this class")
            raise TypeError()

        return super().__new__(cls, name, bases, body)


# class Etl(metaclass=EtlMeta):
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
    EXIT_CODE_SYNC_CHECK_ERROR = 220
    # verify status exit codes
    EXIT_CODE_FINAL_STATUS_INCOMPLETE = 210
    EXIT_CODE_FINAL_STATUS_ERRORED = 211
    EXIT_CODE_FINAL_STATUS_INVALID = 212
    EXIT_CODE_UNKNOWN_STATUS = 213
    EXIT_CODE_UNKNOWN_ERROR = 299

    # API TIMEOUT
    TIMEOUT = 30

    def __init__(self, vendor: str, access_token: str) -> None:
        logging.basicConfig(level = logging.NOTSET,format= '%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

        
        _logger = logging.getLogger(f"{vendor} log")

        stream = logging.StreamHandler()
        # stream.setLevel(logging.WARNING)
        # stream.setLevel(logging.ERROR)
        # stream.setLevel(logging.INFO)
        # stream_format = logging.Formatter(
        #     fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        # stream = stream.setFormatter(stream_format)
        _logger.addHandler(stream)
        self.logger = _logger
        self.vendor = vendor
        self.access_token = access_token

    # @abstractmethod
    # def trigger_sync(self):
    #     pass

    # @abstractmethod
    # def determine_sync_status(self):
    #     pass
