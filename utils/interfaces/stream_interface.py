from abc import ABC, abstractmethod

class Stream(ABC):
    @abstractmethod
    def make_query(self):
        pass

    @abstractmethod
    def start_stream(self):
        pass