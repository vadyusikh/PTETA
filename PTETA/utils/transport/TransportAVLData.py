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
    routeId: int                | routeId(FK)           | routeId
    response_datetime: datetime | response_datetime(FK) | response_datetime
    """
    lat: float
    lng: float
    speed: float
    orientation: float
    gpstime: datetime
    inDepo: bool
    vehicleId: int
    routeId: int
    response_datetime: datetime

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'TransportAVLData':
        return TransportAVLData(
            response_row['lat'], response_row['lng'], response_row['speed'],
            response_row['orientation'], response_row['gpstime'], response_row['inDepo'],
            None, None, response_row['response_datetime']
        )

    def __eq__(self, other: 'TransportAVLData') -> bool:
        return isinstance(other, self.__class__) \
               and self.lat == other.lat \
               and self.lng == other.lng \
               and self.speed == other.speed \
               and self.orientation == other.orientation \
               and self.gpstime == other.gpstime \
               and self.inDepo == other.inDepo \
               and self.vehicleId == other.vehicleId \
               and self.routeId == other.routeId

    def __hash__(self):
        return hash((self.lat, self.lng, self.speed, self.orientation,
                     self.gpstime, self.inDepo, self.vehicleId, self.routeId))

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__}.gpsdata"

    @classmethod
    def __select_columns__(cls) -> str:
        return '"lat", "lng", "speed", "orientation", "gpstime", ' \
               '"inDepo", "vehicleId", "routeId", "response_datetime"'

    @classmethod
    def __where_expression__(cls, avl_data: 'TransportAVLData') -> str:
        return f' "lat" = \'{avl_data.lat}\' ' + \
                  f'AND "lng" = \'{avl_data.lng}\' ' + \
                  f'AND "speed" = \'{avl_data.speed}\' ' + \
                  f'AND "orientation" = \'{avl_data.orientation}\' ' + \
                  f'AND "gpstime" = \'{avl_data.gpstime}\' ' + \
                  f'AND "inDepo" = {avl_data.inDepo} ' + \
                  f'AND "vehicleId" = {avl_data.vehicleId} ' + \
                  f'AND "routeId" = {avl_data.routeId}'

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"lat", "lng", "speed", "orientation", "gpstime", ' \
               '"inDepo", "vehicleId", "routeId", "response_datetime"'

    @classmethod
    def __insert_expression__(cls, avl_data: 'TransportAVLData') -> str:
        return f"('{avl_data.lat}', '{avl_data.lng}', '{avl_data.speed}', " \
               f"'{avl_data.orientation}', '{avl_data.gpstime}', '{avl_data.inDepo}', " \
               f"'{avl_data.vehicleId}', '{avl_data.routeId}', '{avl_data.response_datetime}')"
