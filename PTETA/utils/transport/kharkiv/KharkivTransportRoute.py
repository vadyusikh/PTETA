from dataclasses import dataclass

from PTETA.utils.transport.TransportRoute import TransportRoute
from PTETA.utils.transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass
class KharkivTransportRoute(TransportRoute, BaseDBAccessDataclass):
    """
    Column name relations
    dataclass   | DB           | pandas col
    ------------|--------------|------------
    id: int     | route.id     |
    name: str   | route.name   | route_name
    type: int   | route.type   | route_type
    """
    id: int
    name: str
    type: int

    def __init__(self, name: str, type: int, id: int = None, **kwargs):
        self.id = int(id) if not (id is None) else None
        self.name = str(name) if not ((name is None) or (name == '')) else "UNKNOWN"
        self.type = int(type) if not (type is None) else -1

    def __eq__(self, other: 'KharkivTransportRoute') -> bool:
        return isinstance(other, self.__class__) \
            and self.name == other.name \
            and self.type == other.type

    def __hash__(self):
        return hash((self.name, self.type))

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportRoute':
        return KharkivTransportRoute(
            name=response_row["route_name"], type=response_row["route_type"]
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
