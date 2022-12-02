from dataclasses import dataclass
from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List


@dataclass(unsafe_hash=True)
class TransportVehicle:
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

    def is_in_table(self, connection: Connection) -> bool:
        with connection.cursor() as cursor:
            sql = f"""SELECT EXISTS(SELECT * FROM pteta.vehicle """ + \
                  f"""WHERE "imei" = '{self.imei}' """ + \
                  f"""AND "name" = '{self.name}' """ + \
                  f"""AND "busNumber" = '{self.busNumber}' """ + \
                  f"""AND "remark" = '{self.remark}' """ + \
                  f"""AND "perevId" = {self.perevId} """ + \
                  f"""AND "routeId" = {self.routeId});"""
            try:
                cursor.execute(sql)
                return cursor.fetchone()[0]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    def insert_in_table(self, connection: Connection) -> None:
        with connection.cursor() as cursor:
            sql = f"""INSERT INTO pteta.vehicle("imei", "name", "busNumber", "remark", "perevId", "routeId")""" + \
                  f"""VALUES ('{self.imei}', '{self.name}', '{self.busNumber}', """ + \
                  f"""'{self.remark}', {self.perevId},  {self.routeId});"""

            cursor.execute(sql)
            connection.commit()

    @classmethod
    def get_table(cls, connection: Connection) -> List['TransportVehicle']:
        with connection.cursor() as cursor:
            sql = f"""SELECT id, "imei", "name", "busNumber", "remark", "perevId", "routeId" FROM pteta.vehicle;"""
            try:
                cursor.execute(sql)
                return [TransportVehicle(*rec) for rec in cursor.fetchall()]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err
