from dataclasses import dataclass
from datetime import datetime

from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List


@dataclass
class TransportAVLData:
    """
    Column lng relations

    dataclass                   | DB                    | HTTP request
    ----------------------------|-----------------------|------------
    lat: float                  | lat                   | lat
    lng: float                  | lng                   | lng
    speed: float                | speed                 | speed
    orientation : float         | orientation           | orientation
    gpstime: datetime           | gpstime               | gpstime
    inDepo : bool               | inDepo                | inDepo
    vehicleId: int              | vehicleId(FK)         | vehicleId
    response_datetime: datetime | response_datetime(FK) | response_datetime
    """
    lat: float
    lng: float
    speed: float
    orientation: float
    gpstime: datetime
    inDepo: bool
    vehicleId: int
    response_datetime: datetime

    def __eq__(self, other: 'TransportAVLData') -> bool:
        return isinstance(other, self.__class__) \
               and self.lat == other.lat \
               and self.lng == other.lng \
               and self.speed == other.speed \
               and self.orientation == other.orientation \
               and self.gpstime == other.gpstime \
               and self.inDepo == other.inDepo \
               and self.vehicleId == other.vehicleId

    def __hash__(self):
        return hash((self.lat, self.lng, self.speed, self.orientation,
                     self.gpstime, self.inDepo, self.vehicleId))

    def is_in_table(self, connection: Connection) -> bool:
        with connection.cursor() as cursor:
            sql = f"""SELECT EXISTS(SELECT * FROM pteta.gpsdata """ + \
                  f"""WHERE "lat" = '{self.lat}' """ + \
                  f"""AND "lng" = '{self.lng}' """ + \
                  f"""AND "speed" = '{self.speed}' """ + \
                  f"""AND "orientation" = '{self.orientation}' """ + \
                  f"""AND "gpstime" = '{self.gpstime}' """ + \
                  f"""AND "inDepo" = {self.inDepo} """ + \
                  f"""AND "vehicleId" = {self.vehicleId});"""
            try:
                cursor.execute(sql)
                return cursor.fetchone()[0]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err

    def insert_in_table(self, connection: Connection) -> None:
        with connection.cursor() as cursor:
            sql = f"""INSERT INTO pteta.gpsdata("lat", "lng", "speed", "orientation", """ + \
                  f""" "gpstime", "inDepo", "vehicleId", "response_datetime")""" + \
                  f"""VALUES ('{self.lat}', '{self.lng}', '{self.speed}', '{self.orientation}', """ + \
                  f""" '{self.gpstime}',  '{self.inDepo}', '{self.vehicleId}', '{self.response_datetime}');"""

            cursor.execute(sql)
            connection.commit()

    @classmethod
    def get_table(cls, connection: Connection) -> List['TransportAVLData']:
        with connection.cursor() as cursor:
            sql = f"""SELECT "lat", "lng", "speed", "orientation", """ + \
                  f""" "gpstime", "inDepo", "vehicleId", "response_datetime" FROM pteta.gpsdata;"""
            try:
                cursor.execute(sql)
                return [TransportAVLData(*rec) for rec in cursor.fetchall()]
            except InFailedSqlTransaction as err:
                connection.rollback()
                raise err
