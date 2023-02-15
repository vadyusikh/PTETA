from dataclasses import dataclass
from datetime import datetime

from PTETA.utils.transport.TransportAVLData import TransportAVLData
from PTETA.utils.transport.kharkiv.KharkivBaseDBAccessDataclass import BaseDBAccessDataclass
from PTETA.utils.transport.functions import cast_if_possible


@dataclass
class KharkivTransportAVLData(TransportAVLData, BaseDBAccessDataclass):
    """
    Column relations
    dataclass                   | DB                    | HTTP request
    ----------------------------|-----------------------|------------
    lat: float                  | lat                   |
    lng: float                  | lng                   |
    speed: float                | speed                 |
    orientation: float          | orientation           |
    gpstime: datetime           | gpstime               |
    gps_datetime_origin: int    | gps_datetime_origin   |
    dd: int                     | dd                    |
    vehicle_id: int             | vehicle_id(FK)        |
    route_id: int               | route_id(FK)          |
    response_datetime: datetime | response_datetime(FK) |
    """
    lat: float
    lng: float
    speed: float
    orientation: float
    gpstime: datetime
    gps_datetime_origin: int
    dd: int
    vehicle_id: int
    route_id: int
    response_datetime: datetime

    def __init__(self, lat: float, lng: float, speed: float, orientation: float,
                 gpstime: str, gps_datetime_origin: str, dd: int,
                 vehicle_id: int, response_datetime: str, route_id: int=None, **kwargs):
        self.lat = float(lat)
        self.lng = float(lng)
        self.speed = cast_if_possible(speed, float, "NULL")
        self.orientation = cast_if_possible(orientation, float, "NULL")
        self.gpstime = str(gpstime)
        self.gps_datetime_origin = int(gps_datetime_origin)
        self.dd = cast_if_possible(dd, int, "NULL")
        self.vehicle_id = cast_if_possible(vehicle_id, int)
        self.route_id = cast_if_possible(route_id, int)
        self.response_datetime = str(response_datetime)

    @classmethod
    def from_response_row(cls, response_row: dict) -> 'KharkivTransportAVLData':
        return KharkivTransportAVLData(**response_row)

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
    def __where_columns__(cls) -> str:
        return cls.__select_columns__()

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
