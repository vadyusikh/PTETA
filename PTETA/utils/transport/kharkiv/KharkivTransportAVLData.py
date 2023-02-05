from dataclasses import dataclass
from datetime import datetime

from transport.TransportAVLData import TransportAVLData
from transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass


@dataclass
class KharkivTransportAVLData(TransportAVLData, BaseDBAccessDataclass):
    """
    Column lng relations

    dataclass                   | DB                    | HTTP request
    ----------------------------|-----------------------|------------
    lat: float                  | lat                   | lat
    lng: float                  | lng                   | lng
    speed: float                | speed                 | speed
    orientation : float         | orientation           | orientation
    gpstime: datetime           | gpstime               | gpstime
    in_depo : bool              | in_depo               | in_depo
    vehicle_id: int             | vehicle_id(FK)        | vehicle_id
    route_id: int               | route_id(FK)          | route_id
    response_datetime: datetime | response_datetime(FK) | response_datetime
    """
    gps_datetime_origin: int
    dd: int

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportAVLData':
        return KharkivTransportAVLData(
            lat=response_row['lat'], lng=response_row['lng'],
            speed=response_row['speed'] if response_row['speed'] else -1,
            orientation=response_row['orientation'] if response_row['orientation'] else -1,
            gpstime=response_row['gpstime'],
            gps_datetime_origin=response_row['gps_datetime_origin'],
            dd=response_row['dd'],
            vehicle_id=None, route_id=None,
            response_datetime=response_row['response_datetime']
        )

    def __eq__(self, other: 'KharkivTransportAVLData') -> bool:
        return isinstance(other, self.__class__) \
               and self.lat == other.lat \
               and self.lng == other.lng \
               and self.speed == other.speed \
               and self.orientation == other.orientation \
               and self.gpstime == other.gpstime \
               and self.gps_datetime_origin == other.gps_datetime_origin \
               and self.dd == other.dd \
               and self.vehicle_id == other.vehicle_id \
               and self.route_id == other.route_id

    def __hash__(self):
        return hash((self.lat, self.lng, self.speed, self.orientation,
                     self.gpstime, self.gps_datetime_origin, self.dd, self.vehicle_id, self.route_id))

    @classmethod
    def __table_name__(cls) -> str:
        return f"{cls.__schema_name__()}.gpsdata"

    @classmethod
    def __select_columns__(cls) -> str:
        return '"lat", "lng", "speed", "orientation", "gpstime", "gps_datetime_origin",' \
               '"dd", "vehicle_id", "route_id", "response_datetime"'

    @classmethod
    def __where_expression__(cls, avl_data: 'KharkivTransportAVLData') -> str:
        return f' "lat" = \'{avl_data.lat}\' ' + \
                  f'AND "lng" = \'{avl_data.lng}\' ' + \
                  f'AND "speed" = \'{avl_data.speed}\' ' + \
                  f'AND "orientation" = \'{avl_data.orientation}\' ' + \
                  f'AND "gpstime" = \'{avl_data.gpstime}\' ' + \
                  f'AND "gps_datetime_origin" = {avl_data.gps_datetime_origin} ' + \
                  f'AND "dd" = {avl_data.dd} ' + \
                  f'AND "vehicle_id" = {avl_data.vehicle_id} ' + \
                  f'AND "route_id" = {avl_data.route_id}'

    @classmethod
    def __insert_columns__(cls) -> str:
        return '"lat", "lng", "speed", "orientation", "gpstime", "gps_datetime_origin",' \
               '"dd", "vehicle_id", "route_id", "response_datetime"'

    @classmethod
    def __insert_expression__(cls, avl_data: 'KharkivTransportAVLData') -> str:
        return f"('{avl_data.lat}', '{avl_data.lng}', '{avl_data.speed}', " \
               f"'{avl_data.orientation}', '{avl_data.gpstime}', '{avl_data.gps_datetime_origin}', " \
               f"'{avl_data.dd}','{avl_data.vehicle_id}', '{avl_data.route_id}', " \
               f""" { f"'{avl_data.response_datetime}'" if avl_data.response_datetime else 'NULL'})"""
