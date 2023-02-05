from dataclasses import dataclass

from transport.TransportRoute import TransportRoute
from transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass(unsafe_hash=True)
class KharkivTransportRoute(TransportRoute, BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass   | DB           | HTTP request
    ------------|--------------|------------
    id: int     | id           |
    name: str   | route_name   |
    """
    name: str

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportRoute':
        return KharkivTransportRoute(
            response_row['route_name'] if response_row['route_name'] else "UNKNOWN",
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.route"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "route_name"'

    @classmethod
    def __where_expression__(cls, route: 'KharkivTransportRoute') -> str:
        return f""""id" = {route.id} AND "route_name" = '{route.name}'"""

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"id", "route_name"'

    @classmethod
    def __insert_expression__(cls, route: 'KharkivTransportRoute') -> str:
        return f"('{route.name}'')"
