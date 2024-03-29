from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List

from abc import ABC, abstractmethod


class BaseDBAccessDataclass(ABC):
    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'BaseDBAccessDataclass':
        pass

    def is_in_table(self, connection: Connection) -> bool:
        return self.are_in_table(connection, [self])[0]

    @classmethod
    def are_in_table(
            cls,
            connection: Connection,
            obj_list: List['BaseDBAccessDataclass']
        ) -> List[bool]:
        """140 faster single obj method for same number of objects"""
        if not obj_list:
            return []

        sql = f"SELECT {cls.__where_columns__()} " \
              f"FROM {cls.__table_name__()} " + f" WHERE " + \
              " OR".join([f"({cls.__where_expression__(obj)}) "
                          for obj in obj_list]) + ";"

        with connection.cursor() as cursor:
            try:
                cursor.execute(sql)
                response_set = set(cls.obj_from_request(cursor))
                return [obj in response_set for obj in obj_list]
            except Exception as err:
                connection.rollback()
                print(f"Error raised while select {cls} '{obj_list}'")
                raise err

    def insert_in_table(self, connection: Connection) -> None:
        self.insert_many_in_table(connection, [self])

    @classmethod
    def obj_from_request(cls, cursor):
        columns = [desc[0] for desc in cursor.description]
        list_of_dict = [dict(zip(columns, row)) for row in cursor.fetchall()]
        try:
            return [cls(**row) for row in list_of_dict]
        except TypeError as e:
            print(f"Exception raised on class\n\t'{cls}'\n on data\n\t{list_of_dict}")
            raise e

    @classmethod
    def insert_many_in_table(
            cls,
            connection: Connection,
        obj_list: List['BaseDBAccessDataclass']
        ) -> None:

        if not obj_list:
            return

        sql = f"INSERT INTO {cls.__table_name__()}" + \
              f"({cls.__insert_columns__()}) VALUES" + \
              ", ".join([f"{cls.__insert_expression__(obj)}"
                         for obj in obj_list]) + \
              "ON CONFLICT DO NOTHING;"

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                connection.commit()
        except InFailedSqlTransaction as err:
            connection.rollback()
            print(f"Error raised while select {cls} '{obj_list}'")
            raise err
            
    @classmethod
    def get_table(
            cls,
            connection: Connection,
            additional_condition: str = ''
    ) -> List['BaseDBAccessDataclass']:

        with connection.cursor() as cursor:
            sql = f"SELECT {cls.__select_columns__()} " + \
                  f" FROM {cls.__table_name__()} {additional_condition};"
            try:
                cursor.execute(sql)
                return cls.obj_from_request(cursor)
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    @classmethod
    @abstractmethod
    def __schema_name__(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def __table_name__(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def __select_columns__(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def __where_columns__(cls, obj: 'BaseDBAccessDataclass') -> str:
        pass

    @classmethod
    @abstractmethod
    def __where_expression__(cls, obj: 'BaseDBAccessDataclass') -> str:
        pass

    @classmethod
    @abstractmethod
    def __insert_columns__(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def __insert_expression__(cls, obj: 'BaseDBAccessDataclass') -> str:
        pass
