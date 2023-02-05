from dataclasses import dataclass
from psycopg2.extensions import connection as Connection

from PTETA.utils.transport.TransportVehicle import TransportVehicle
from PTETA.utils.transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass
class KharkivTransportVehicle(TransportVehicle, BaseDBAccessDataclass):
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
    owner_id: int

    def __init__(self, id: int, imei: str, name: str, owner_id: int):
        self.id = None if id is None else int(id)
        self.imei = str(imei)
        self.name = str(name)
        self.owner_id = -1 if id is None else int(owner_id)

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportVehicle':
        vehicle_name = None
        if "vehicle_name" in response_row.keys():
            vehicle_name = response_row["vehicle_name"]

        owner_id = -1
        if 'owner_id' in response_row.keys() and response_row['owner_id'] is not None:
            owner_id = response_row['owner_id']

        return KharkivTransportVehicle(
            id=None,
            imei=response_row['imei'],
            name=vehicle_name,
            owner_id=owner_id
        )

    def __eq__(self, other: 'KharkivTransportVehicle') -> bool:
        return isinstance(other, self.__class__) \
               and self.imei == other.imei \
               and self.name == other.name \
               and self.owner_id == other.owner_id

    def __hash__(self):
        return hash((self.imei, -1 if self.name else self.name, self.owner_id))

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
        return 'id, "imei", "name", "owner_id"'

    @classmethod
    def __where_columns__(cls) -> str:
        return cls.__select_columns__()

    @classmethod
    def __where_expression__(cls, vehicle: 'KharkivTransportVehicle') -> str:
        return f'"imei" = \'{vehicle.imei}\' ' + \
               f'AND "name" = \'{vehicle.name}\' ' + \
               f'AND "owner_id" = {vehicle.owner_id if vehicle.owner_id else "NULL"} '

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"imei", "name", "owner_id"'

    @classmethod
    def __insert_expression__(cls, vehicle: 'KharkivTransportVehicle') -> str:
        return f"('{vehicle.imei}', '{vehicle.name}', " \
               f"{vehicle.owner_id if vehicle.owner_id else 'NULL'})"
