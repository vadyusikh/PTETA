from dataclasses import dataclass

from PTETA.utils.transport.TransportOperator import TransportOperator
from PTETA.utils.transport.chernivtsi.ChernivtsiBaseDBAccessDataclass import ChernivtsiBaseDBAccessDataclass
from PTETA.utils.transport.functions import cast_if_possible


@dataclass
class ChernivtsiTransportOperator(TransportOperator, ChernivtsiBaseDBAccessDataclass):
    """
    Column name relations
    dataclass | DB (owner) | HTTP request
    ----------|------------|-------------
    id: int   | id         | perevId
    name: str | perev_name | perevName
    """
    id: int
    name: str

    def __init__(self, perev_name: str, id: int = -1, **kwargs):
        self.id = cast_if_possible(id, int, -1)
        self.name = cast_if_possible(perev_name, str, "UNKNOWN")

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'ChernivtsiTransportOperator':
        return ChernivtsiTransportOperator(
            perev_name=response_row['perevName'],
            id=response_row['perevId']
        )

    def __eq__(self, other: 'ChernivtsiTransportOperator') -> bool:
        return isinstance(other, self.__class__) \
            and self.id == other.id \
            and self.name == other.name

    def __hash__(self):
        return hash((self.id, self.name))

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.owner"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "perev_name"'

    @classmethod
    def __where_columns__(cls) -> str:
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
