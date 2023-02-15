from dataclasses import dataclass

from PTETA.utils.transport.TransportRoute import TransportRoute
from PTETA.utils.transport.chernivtsi.ChernivtsiBaseDBAccessDataclass import ChernivtsiBaseDBAccessDataclass
from PTETA.utils.transport.functions import cast_if_possible


@dataclass()
class ChernivtsiTransportRoute(TransportRoute, ChernivtsiBaseDBAccessDataclass):
    """
    Column name relations
    dataclass   | DB           | HTTP request
    ------------|--------------|------------
    id: int     | route_id     | routeId
    name: str   | route_name   | routeName
    colour: str | route_colour | routeColour
    """
    id: int
    name: str
    colour: str

    def __init__(self, route_name: str, route_colour: str, id: int = None, **kwargs):
        self.id = cast_if_possible(id, int)
        self.name = cast_if_possible(route_name, str, "UNKNOWN")
        self.colour = cast_if_possible(route_colour, str, "None")

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'ChernivtsiTransportRoute':
        if 'route_name' not in response_row.keys():
            response_row['route_name'] = response_row['routeName']
        if 'route_colour' not in response_row.keys():
            response_row['route_colour'] = response_row['routeColour']

        return ChernivtsiTransportRoute(id=response_row['routeId'], **response_row)

    def __eq__(self, other: 'ChernivtsiTransportRoute') -> bool:
        return isinstance(other, self.__class__) \
            and self.name == other.name \
            and self.colour == other.colour

    def __hash__(self):
        return hash((self.name, self.colour))

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.route"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "route_name", "route_colour"'

    @classmethod
    def __where_columns__(cls) -> str:
        return 'id, "route_name", "route_colour"'

    @classmethod
    def __where_expression__(cls, route: 'ChernivtsiTransportRoute') -> str:
        return f""""id" = {route.id} AND "route_name" = '{route.name}'"""

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"id", "route_name", "route_colour"'

    @classmethod
    def __insert_expression__(cls, route: 'ChernivtsiTransportRoute') -> str:
        return f"('{route.id}', '{route.name}', '{route.colour}')"
