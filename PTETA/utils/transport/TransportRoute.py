from dataclasses import dataclass
from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List

from PTETA.utils.transport.BaseDBAccessDataclass import BaseDBAccessDataclass


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
    name: str
    colour: str

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'TransportRoute':
        return TransportRoute(
            response_row['routeId'], response_row['routeName'], response_row['routeColour']
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.route"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "route_name", "route_colour"'

    @classmethod
    def __where_expression__(cls, route: 'TransportRoute') -> str:
        return f""""id" = {route.id} AND "route_name" = '{route.name}'"""

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"id", "route_name", "route_colour"'

    @classmethod
    def __insert_expression__(cls, route: 'TransportRoute') -> str:
        return f"('{route.id}', '{route.name}', '{route.colour}')"
