from dataclasses import dataclass
from psycopg2.extensions import connection as Connection

from PTETA.utils.transport.BaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass
class TransportVehicle(BaseDBAccessDataclass):
    """
    Column name ralations
    dataclass      | DB        | HTTP request
    ---------------|-----------|------------
    id: int        | id(PK)      |  -
    imei: int      | imei        | imei
    name: str      | name        | name
    busNumber: str | busNumber   | busNumber
    remark: str    | remark      | remark
    perevId: int   | perevId(FK) | perevId
    routeId: int   | routeId(FK) | routeId
    """
    id: int
    imei: str
    name: str
    busNumber: str
    remark: str
    perevId: int
    routeId: int

    def __eq__(self, other: 'TransportVehicle') -> bool:
        return isinstance(other, self.__class__) \
               and self.imei == other.imei \
               and self.name == other.name \
               and self.busNumber == other.busNumber \
               and self.remark == other.remark \
               and self.perevId == other.perevId \
               and self.routeId == other.routeId

    def __hash__(self):
        return hash((self.imei, self.name, self.busNumber,
                     self.remark, self.perevId, self.routeId))

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
        return "pteta.vehicle"

    @classmethod
    def __select_columns__(cls) -> str:
        return 'id, "imei", "name", "busNumber", "remark", "perevId", "routeId"'

    @classmethod
    def __where_expression__(cls, vehicle: 'TransportVehicle') -> str:
        return f'"imei" = \'{vehicle.imei}\' ' + \
               f'AND "name" = \'{vehicle.name}\' ' + \
               f'AND "busNumber" = \'{vehicle.busNumber}\' ' + \
               f'AND "remark" = \'{vehicle.remark}\' ' + \
               f'AND "perevId" = {vehicle.perevId} ' + \
               f'AND "routeId" = {vehicle.routeId} '

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"imei", "name", "busNumber", "remark", "perevId", "routeId"'

    @classmethod
    def __insert_expression__(cls, vehicle: 'TransportVehicle') -> str:
        return f"('{vehicle.imei}', '{vehicle.name}', '{vehicle.busNumber}', " + \
               f"'{vehicle.remark}', {vehicle.perevId},  {vehicle.routeId})"
