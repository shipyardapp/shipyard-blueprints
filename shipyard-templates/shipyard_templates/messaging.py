from abc import ABC, abstractmethod


class Messaging(ABC):
    @abstractmethod
    def connect(self):
        pass

    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_INVALID_INPUT = 201
    EXIT_CODE_RATE_LIMIT = 202
    EXIT_CODE_BAD_REQUEST = 203
    EXIT_CODE_FILE_NOT_FOUND = 204
    EXIT_CODE_UNKNOWN_ERROR = 249
