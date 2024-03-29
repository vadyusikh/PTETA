from dataclasses import dataclass

from transport.TransportRoute import TransportRoute
from transport.chernivtsi.ChernivtsiBaseDBAccessDataclass import ChernivtsiBaseDBAccessDataclass


@dataclass(unsafe_hash=True)
class ChernivtsiTransportRoute(TransportRoute, ChernivtsiBaseDBAccessDataclass):
    """
    Column name relations
    dataclass   | DB           | HTTP request
    ------------|--------------|------------
    id: int     | id           | routeId
    name: str   | route_name   | routeName
    colour: str | route_colour | routeColour
    """
    name: str
    colour: str

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'ChernivtsiTransportRoute':
        return ChernivtsiTransportRoute(
            response_row['routeId'] if response_row['routeName'] else -1,
            response_row['routeName'] if response_row['routeName'] else "UNKNOWN",
            response_row['routeColour'] if response_row['routeColour'] else "None"
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.route"

    @classmethod
    def __select_columns__(cls) -> str:
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
