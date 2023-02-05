from dataclasses import dataclass
from abc import abstractmethod

from transport.BaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass(unsafe_hash=True)
class TransportOperator(BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass | DB         | HTTP request
    ----------|------------|-------------
    id: int   | id         | perevId
    name: str | perev_name | perevName
    """
    id: int
    # name: str

    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'TransportOperator':
        pass
