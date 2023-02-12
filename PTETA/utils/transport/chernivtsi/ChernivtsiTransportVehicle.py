from dataclasses import dataclass
from psycopg2.extensions import connection as Connection

from PTETA.utils.transport.TransportVehicle import TransportVehicle
from PTETA.utils.transport.chernivtsi.ChernivtsiBaseDBAccessDataclass import ChernivtsiBaseDBAccessDataclass


@dataclass
class ChernivtsiTransportVehicle(TransportVehicle, ChernivtsiBaseDBAccessDataclass):
    """
    Column name ralations
    dataclass       | DB          | HTTP request
    ----------------|-------------|------------
    id: int         | id(PK)      |  -
    imei: int       | imei        | imei
    name: str       | name        | name
    bus_number: str | bus_number  | busNumber
    remark: str     | remark      | remark
    perev_id: int   | perev_id(FK)| perevId

    """
    id: int
    imei: str
    name: str
    bus_number: str
    remark: str
    perev_id: int

    def __init__(self, imei: str, name: str, bus_number: str, remark: str, perev_id: int, id: int = None,
                 **kwargs):
        self.id = int(id) if not (id is None) else None
        self.imei = str(imei)
        self.name = str(name) if not (name is None) else "NULL"
        self.bus_number = str(bus_number) if bus_number else "UNKNOWN"
        self.remark = str(remark) if remark else ''
        self.perev_id = int(perev_id) if not (perev_id is None) else -1

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'ChernivtsiTransportVehicle':
        if 'bus_number' not in response_row.keys():
            response_row['bus_number'] = response_row['busNumber']
        if 'perev_id' not in response_row.keys():
            response_row['perev_id'] = response_row['perevId']

        return ChernivtsiTransportVehicle(**response_row)

    def __eq__(self, other: 'ChernivtsiTransportVehicle') -> bool:
        return isinstance(other, self.__class__) \
            and self.imei == other.imei \
            and self.name == other.name \
            and self.bus_number == other.bus_number \
            and self.remark == other.remark \
            and self.perev_id == other.perev_id

    def __hash__(self):
        return hash((self.imei, self.name, self.bus_number, self.remark, self.perev_id))

    def update_id_from_table(self, connection: Connection) -> None:
        if not self.is_in_table(connection):
            return

        with connection.cursor() as cursor:
            sql = f"SELECT id FROM {self.__table_name__()} " + \
                  f"WHERE {self.__where_expression__(self)};"

            cursor.execute(sql)
            self.id = cursor.fetchone()[0]

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.vehicle"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "imei", "name", "bus_number", "remark", "perev_id"'

    @classmethod
    def __where_columns__(cls) -> str:
        return 'id, "imei", "name", "bus_number", "remark", "perev_id"'

    @classmethod
    def __where_expression__(cls, vehicle: 'ChernivtsiTransportVehicle') -> str:
        return f'"imei" = \'{vehicle.imei}\' ' + \
            f'AND "name" = \'{vehicle.name}\' ' + \
            f'AND "bus_number" = \'{vehicle.bus_number if vehicle.bus_number else "NULL"}\' ' + \
            f'AND "remark" = \'{vehicle.remark}\' ' + \
            f'AND "perev_id" = {vehicle.perev_id if vehicle.perev_id else "NULL"} '

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"imei", "name", "bus_number", "remark", "perev_id"'

    @classmethod
    def __insert_expression__(cls, vehicle: 'ChernivtsiTransportVehicle') -> str:
        return f"('{vehicle.imei}', '{vehicle.name}', " \
               f"'{vehicle.bus_number if vehicle.bus_number else 'NULL'}', " + \
            f"'{vehicle.remark}'," \
            f"{vehicle.perev_id if vehicle.perev_id else 'NULL'})"
