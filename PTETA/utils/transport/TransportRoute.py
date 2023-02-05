from dataclasses import dataclass
from abc import abstractmethod

from transport.BaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass(unsafe_hash=True)
class TransportRoute(BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass   | DB           | HTTP request
    ------------|--------------|------------
    id: int     | id           | routeId
    name: str   | route_name   | routeName
    colour: str | route_colour | routeColour
    """
    id: int
    # name: str
    # colour: str

    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'TransportRoute':
        pass
