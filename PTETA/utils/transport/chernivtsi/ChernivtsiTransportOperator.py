from dataclasses import dataclass

from transport.TransportOperator import TransportOperator
from transport.chernivtsi.ChernivtsiBaseDBAccessDataclass import ChernivtsiBaseDBAccessDataclass


@dataclass(unsafe_hash=True)
class ChernivtsiTransportOperator(TransportOperator, ChernivtsiBaseDBAccessDataclass):
    """
    Column name ralations
    dataclass | DB         | HTTP request
    ----------|------------|-------------
    id: int   | id         | perevId
    name: str | perev_name | perevName
    """
    name: str

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'ChernivtsiTransportOperator':
        return ChernivtsiTransportOperator(
            response_row['perevId'] if response_row['perevName'] else -1,
            response_row['perevName'] if response_row['perevName'] else "UNKNOWN"
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.owner"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "perev_name"'

    @classmethod
    def __where_expression__(cls, operator: 'ChernivtsiTransportOperator') -> str:
        return f'"id" = {operator.id} AND "perev_name" = \'{operator.name}\''

    @classmethod
    def __insert_columns__(cls) -> str:
        return 'id, "perev_name"'

    @classmethod
    def __insert_expression__(cls, operator: 'ChernivtsiTransportOperator') -> str:
        return f"({operator.id}, '{operator.name}')"
