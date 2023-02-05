from abc import abstractmethod
from dataclasses import dataclass
from psycopg2.extensions import connection as Connection


@dataclass
class TransportVehicle:
    """
    Column name ralations
    dataclass       | DB          | HTTP request
    ----------------|-------------|------------
    id: int         | id(PK)      |  -
    imei: int       | imei        |
    name: str       | name        |
    bus_number: str | bus_number  |
    remark: str     | remark      |
    perev_id: int   | perev_id(FK)|

    """
    id: int
    imei: str
    name: str

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
