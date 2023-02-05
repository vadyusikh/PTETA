from dataclasses import dataclass

from PTETA.utils.transport.TransportRoute import TransportRoute
from PTETA.utils.transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass


class KharkivTransportRoute(TransportRoute, BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass   | DB           | HTTP request
    ------------|--------------|------------
    id: int     | id           |
    name: str   | route_name   |
    """
    name: str
    type: int

    def __init__(self, id: int, name: str, type: int):
        self.id = None if id is None else int(id)
        self.name = str(name)
        self.type = -1 if type is None else int(type)

    def __eq__(self, other: 'KharkivTransportRoute') -> bool:
        return isinstance(other, self.__class__) \
               and self.name == other.name \
               and self.type == other.type

    def __hash__(self):
        return hash((self.name, self.type))

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportRoute':
        return KharkivTransportRoute(
            id=None,
            name=response_row['route_name'] if response_row['route_name'] else "UNKNOWN",
            type=response_row['route_type'] if response_row['route_name'] else -1,
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.route"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "name", "type"'

    @classmethod
    def __where_columns__(cls) -> str:
        return 'id, "name", "type"'

    @classmethod
    def __where_expression__(cls, route: 'KharkivTransportRoute') -> str:
        return f""" "name" = '{route.name}'""" \
               f""" AND "type" = '{route.type}'"""

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"name", "type"'

    @classmethod
    def __insert_expression__(cls, route: 'KharkivTransportRoute') -> str:
        return f"('{route.name}', '{route.type}')"
