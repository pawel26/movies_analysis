from abc import ABC, abstractmethod


class BaseExtractSourceService(ABC):

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def fetch_data(self, title):
        pass
