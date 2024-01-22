from abc import ABC, abstractmethod


class Messaging(ABC):
    @abstractmethod
    def connect(self):
        pass
