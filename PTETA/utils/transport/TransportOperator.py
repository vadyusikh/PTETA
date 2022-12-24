from dataclasses import dataclass
from PTETA.utils.transport.BaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass(unsafe_hash=True)
class TransportOperator(BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass | DB         | HTTP request
    ----------|------------|-------------
    id: int   | id         | perevId
    name: str | perev_name | perevName
    """
    id: int
    name: str

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'TransportOperator':
        return TransportOperator(
            response_row['perevId'], response_row['perevName']
        )

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.owner"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "perev_name"'

    @classmethod
    def __where_expression__(cls, operator: 'TransportOperator') -> str:
        return f'"id" = {operator.id} AND "perev_name" = \'{operator.name}\''

    @classmethod
    def __insert_columns__(cls) -> str:
        return 'id, "perev_name"'

    @classmethod
    def __insert_expression__(cls, operator: 'TransportOperator') -> str:
        return f"({operator.id}, '{operator.name}')"
