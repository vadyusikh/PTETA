from dataclasses import dataclass
from abc import abstractmethod


@dataclass(unsafe_hash=True)
class TransportRoute:
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
