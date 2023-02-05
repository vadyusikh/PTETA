from dataclasses import dataclass
from abc import abstractmethod


@dataclass(unsafe_hash=True)
class TransportOperator:
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
