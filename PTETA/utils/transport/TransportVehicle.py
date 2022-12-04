from dataclasses import dataclass
from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List


@dataclass
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

    def is_in_table(self, connection: Connection) -> bool:
        with connection.cursor() as cursor:
            sql = f"""SELECT EXISTS(SELECT * FROM {self.__get_table_name__()} """ + \
                  f"""WHERE {self.__get_where_expression__(self)});"""
            try:
                cursor.execute(sql)
                return cursor.fetchone()[0]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    @classmethod
    def are_in_table(
            cls,
            connection: Connection,
            vehicle_list: List['TransportVehicle']
    ) -> List[bool]:
        """140 faster single obj method for same number of objects"""
        if not vehicle_list:
            return []

        sql = f'SELECT {cls.__get_select_columns__()} ' \
              f'FROM {cls.__get_table_name__()} ' + \
              f""" WHERE ({cls.__get_where_expression__(vehicle_list[0])}) """ + \
              " ".join([f"""OR ({cls.__get_where_expression__(obj)}) """
                        for obj in vehicle_list[1:]]) + ";"

        with connection.cursor() as cursor:
            try:
                cursor.execute(sql)
                response_set = set([TransportVehicle(*row) for row in cursor.fetchall()])
                return [vehicle in response_set for vehicle in vehicle_list]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    def insert_in_table(self, connection: Connection) -> None:
        with connection.cursor() as cursor:
            sql = f"""INSERT INTO {self.__get_table_name__()}""" + \
                f"""({self.__get_insert_columns__()}) VALUES""" + \
                f""" {self.__get_insert_expression__(self)};"""

            cursor.execute(sql)
            connection.commit()

    @classmethod
    def insert_many_in_table(
            cls,
            connection: Connection,
            route_list: List['TransportVehicle']
    ) -> None:
        if not route_list:
            return

        sql = f"INSERT INTO {cls.__get_table_name__()}" + \
            f"({cls.__get_insert_columns__()}) VALUES" + \
            ", ".join([f"{cls.__get_insert_expression__(obj)}"
                       for obj in route_list]) + ";"

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                connection.commit()
        except InFailedSqlTransaction as err:
            connection.rollback()
            raise err

    def update_id_from_table(self, connection: Connection) -> None:
        if not self.is_in_table(connection):
            return

        with connection.cursor() as cursor:
            sql = f"""SELECT id FROM {self.__get_table_name__()} """ + \
                  f"""WHERE {self.__get_where_expression__(self)};"""

            cursor.execute(sql)
            self.id = cursor.fetchone()[0]

    @classmethod
    def get_table(cls, connection: Connection) -> List['TransportVehicle']:
        with connection.cursor() as cursor:
            sql = f"""SELECT {cls.__get_select_columns__()} """ +\
                  f""" FROM {cls.__get_table_name__()};"""
            try:
                cursor.execute(sql)
                return [TransportVehicle(*rec) for rec in cursor.fetchall()]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    @classmethod
    def __get_table_name__(cls) -> str:
        return "pteta.vehicle"

    @classmethod
    def __get_select_columns__(cls) -> str:
        return 'id, "imei", "name", "busNumber", "remark", "perevId", "routeId"'

    @classmethod
    def __get_where_expression__(cls, vehicle: 'TransportVehicle') -> str:
        return f""""imei" = '{vehicle.imei}' """ + \
               f"""AND "name" = '{vehicle.name}' """ + \
               f"""AND "busNumber" = '{vehicle.busNumber}' """ + \
               f"""AND "remark" = '{vehicle.remark}' """ + \
               f"""AND "perevId" = {vehicle.perevId} """ + \
               f"""AND "routeId" = {vehicle.routeId} """

    @classmethod
    def __get_insert_columns__(cls) -> str:
        return '"imei", "name", "busNumber", "remark", "perevId", "routeId"'

    @classmethod
    def __get_insert_expression__(cls, vehicle: 'TransportVehicle') -> str:
        return f"""('{vehicle.imei}', '{vehicle.name}', '{vehicle.busNumber}', """ + \
               f"""'{vehicle.remark}', {vehicle.perevId},  {vehicle.routeId})"""
