from dataclasses import dataclass
from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List


@dataclass(unsafe_hash=True)
class TransportOperator:
    """
    Column name ralations dataclass | DB | HTTP request
    id: int   | id        | perevId
    name: str | perevName | perevName
    """
    id: int
    name: str

    def is_in_table(self, connection: Connection) -> bool:
        with connection.cursor() as cursor:
            sql = f"""SELECT EXISTS(SELECT * FROM pteta.owner """ + \
                  f"""WHERE "id" = {self.id} AND "perevName" = '{self.name}' );"""
            try:
                cursor.execute(sql)
                return cursor.fetchone()[0]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    def insert_in_table(self, connection: Connection) -> None:
        with connection.cursor() as cursor:
            sql = f"""INSERT INTO pteta.owner("id", "perevName")""" + \
                  f"""VALUES ({self.id}, '{self.name}');"""
            cursor.execute(sql)
            connection.commit()

    @classmethod
    def get_table(cls, connection: Connection) -> List['TransportOperator']:
        with connection.cursor() as cursor:
            sql = f"""SELECT ID, "perevName" FROM pteta.owner;"""
            try:
                cursor.execute(sql)
                return [TransportOperator(rec[0], rec[1]) for rec in cursor.fetchall()]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err
