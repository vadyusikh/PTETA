from dataclasses import dataclass
from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List


@dataclass(unsafe_hash=True)
class TransportRoute:
    """
    Column name ralations
    dataclass   | DB          | HTTP request
    ------------|-------------|------------
    id: int     | id          | routeId
    name: str   | routeName   | routeName
    colour: str | routeColour | routeColour
    """
    id: int
    name: str
    colour: str

    def is_in_table(self, connection: Connection) -> bool:
        with connection.cursor() as cursor:
            sql = f"""SELECT EXISTS(SELECT * FROM pteta.route """ + \
                  f"""WHERE "id" = {self.id} AND "routeName" = '{self.name}' );"""
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
            route_list: List['TransportRoute']
    ) -> List[bool]:
        """30 faster single obj method for same number of objects"""
        if not route_list:
            return []

        sql = 'SELECT "id", "routeName", "routeColour" FROM pteta.route ' + \
              f""" WHERE ("id" = {route_list[0].id} """ + \
              f""" AND "routeName" = '{route_list[0].name}') """ + \
              " ".join([f"""OR ("id" = {obj.id} AND "routeName" = '{obj.name}') """
                        for obj in route_list[1:]]) + ";"

        with connection.cursor() as cursor:
            try:
                cursor.execute(sql)
                response_set = set([TransportRoute(*row) for row in cursor.fetchall()])
                return [route in response_set for route in route_list]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    def insert_in_table(self, connection: Connection) -> None:
        try:
            with connection.cursor() as cursor:
                sql = f"""INSERT INTO pteta.route("id", "routeName", "routeColour")""" + \
                      f"""VALUES ({self.id}, '{self.name}', '{self.colour}');"""
                cursor.execute(sql)
                connection.commit()
        except InFailedSqlTransaction as err:
            connection.rollback()
            raise err

    @classmethod
    def insert_many_in_table(
            cls,
            connection: Connection,
            route_list: List['TransportRoute']
    ) -> None:
        if not route_list:
            return

        sql = f"""INSERT INTO pteta.route("id", "routeName", "routeColour") VALUES """ + \
              ", ".join([f"""({obj.id}, '{obj.name}', '{obj.colour}')"""
                         for obj in route_list]) + ";"

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                connection.commit()
        except InFailedSqlTransaction as err:
            connection.rollback()
            raise err

    @classmethod
    def get_table(cls, connection: Connection) -> List['TransportRoute']:
        with connection.cursor() as cursor:
            sql = f"""SELECT "id", "routeName", "routeColour" FROM pteta.route;"""
            try:
                cursor.execute(sql)
                return [TransportRoute(rec[0], rec[1], rec[2]) for rec in cursor.fetchall()]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err
