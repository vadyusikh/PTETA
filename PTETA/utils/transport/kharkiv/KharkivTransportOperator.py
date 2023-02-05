from dataclasses import dataclass

from transport.TransportOperator import TransportOperator
from transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass(unsafe_hash=True)
class KharkivTransportOperator(TransportOperator, BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass | DB         | HTTP request
    ----------|------------|-------------
    id: int   | id         | perevId
    name: str | perev_name | perevName
    """
    name: str

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportOperator':
        if 'operator_name' not in response_row.keys():
            return KharkivTransportOperator(id=-1, name="UNKNOWN")
        return KharkivTransportOperator(
            id=response_row['operator_id'] if response_row['operator_name'] else -1,
            name=response_row['operator_name'] if response_row['operator_name'] else "UNKNOWN"
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.owner"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "owner_name"'

    @classmethod
    def __where_expression__(cls, operator: 'KharkivTransportOperator') -> str:
        return f'"id" = {operator.id} AND "owner_name" = \'{operator.name}\''

    @classmethod
    def __insert_columns__(cls) -> str:
        return 'id, "owner_name"'

    @classmethod
    def __insert_expression__(cls, operator: 'KharkivTransportOperator') -> str:
        return f"({operator.id}, '{operator.name}')"
