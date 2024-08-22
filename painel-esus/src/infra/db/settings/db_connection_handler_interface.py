from abc import ABC
from abc import abstractmethod


class DBConnectionHandlerInterface(ABC):

    @abstractmethod
    def _create_database_engine(self): pass

    @abstractmethod
    def get_engine(self): pass

    @abstractmethod
    def __enter__(self): pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb): pass

    @abstractmethod
    def get_connection_str(self): pass
