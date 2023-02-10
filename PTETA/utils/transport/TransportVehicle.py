from abc import abstractmethod
from dataclasses import dataclass
from psycopg2.extensions import connection as Connection


@dataclass
class TransportVehicle:
    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'TransportVehicle':
        pass

    @abstractmethod
    def __eq__(self, other: 'TransportVehicle') -> bool:
        pass

    @abstractmethod
    def __hash__(self):
        pass

    @abstractmethod
    def update_id_from_table(self, connection: Connection) -> None:
        pass
