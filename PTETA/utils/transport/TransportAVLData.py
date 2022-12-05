from dataclasses import dataclass
from datetime import datetime

from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction
from typing import List

from PTETA.utils.transport.BaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass
class TransportAVLData(BaseDBAccessDataclass):
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

    @classmethod
    def __table_name__(cls) -> str:
        return "pteta.gpsdata"

    @classmethod
    def __select_columns__(cls) -> str:
        return '"lat", "lng", "speed", "orientation", "gpstime", ' \
               '"inDepo", "vehicleId", "response_datetime"'

    @classmethod
    def __where_expression__(cls, avl_data: 'TransportAVLData') -> str:
        return f' "lat" = \'{avl_data.lat}\' ' + \
                  f'AND "lng" = \'{avl_data.lng}\' ' + \
                  f'AND "speed" = \'{avl_data.speed}\' ' + \
                  f'AND "orientation" = \'{avl_data.orientation}\' ' + \
                  f'AND "gpstime" = \'{avl_data.gpstime}\' ' + \
                  f'AND "inDepo" = {avl_data.inDepo} ' + \
                  f'AND "vehicleId" = {avl_data.vehicleId}'

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"lat", "lng", "speed", "orientation", "gpstime", ' \
               '"inDepo", "vehicleId", "response_datetime"'

    @classmethod
    def __insert_expression__(cls, avl_data: 'TransportAVLData') -> str:
        return f"('{avl_data.lat}', '{avl_data.lng}', '{avl_data.speed}', " \
               f"'{avl_data.orientation}', '{avl_data.gpstime}', '{avl_data.inDepo}', " \
               f"'{avl_data.vehicleId}', '{avl_data.response_datetime}')"