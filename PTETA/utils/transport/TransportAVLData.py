from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime

from transport.BaseDBAccessDataclass import BaseDBAccessDataclass


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
    vehicle_id: int             | vehicle_id(FK)        | vehicle_id
    route_id: int               | route_id(FK)          | route_id
    response_datetime: datetime | response_datetime(FK) | response_datetime
    """
    lat: float
    lng: float
    speed: float
    orientation: float
    gpstime: datetime
    vehicle_id: int
    route_id: int
    response_datetime: datetime

    def __init__(self):
        self.route_id = None

    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'TransportAVLData':
        pass

    @abstractmethod
    def __eq__(self, other: 'TransportAVLData') -> bool:
        pass

    @abstractmethod
    def __hash__(self):
        pass
