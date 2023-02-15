from dataclasses import dataclass

from PTETA.utils.transport.TransportOperator import TransportOperator
from PTETA.utils.transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass
from PTETA.utils.transport.functions import cast_if_possible


@dataclass(unsafe_hash=True)
class KharkivTransportOperator(TransportOperator, BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass | DB            | HTTP request
    ----------|---------------|-------------
    id: int   | operator_id   |
    name: str | operator_name |
    """
    id: int
    name: str

    def __init__(self, id: int, name: str, **kwargs):
        self.id = cast_if_possible(id, int, -1)
        self.name = cast_if_possible(name, str, "UNKNOWN")

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportOperator':
        if 'operator_name' not in response_row.keys():
            return KharkivTransportOperator(id=-1, name="UNKNOWN")
        return KharkivTransportOperator(
            id=response_row['operator_id'], name=response_row['operator_name']
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.owner"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "name"'

    @classmethod
    def __where_columns__(cls) -> str:
        return cls.__select_columns__()

    @classmethod
    def __where_expression__(cls, operator: 'KharkivTransportOperator') -> str:
        return f'"id" = {operator.id} AND "name" = \'{operator.name}\''

    @classmethod
    def __insert_columns__(cls) -> str:
        return 'id, "name"'

    @classmethod
    def __insert_expression__(cls, operator: 'KharkivTransportOperator') -> str:
        return f"({operator.id}, '{operator.name}')"
