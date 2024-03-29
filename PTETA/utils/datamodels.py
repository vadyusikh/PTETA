from dataclasses import dataclass

import numpy as np


@dataclass
class BusStop:
    def __init__(self, data_dict: dict) -> None:
        self.id = data_dict['i']
        self.name = data_dict['n']
        self.direction = data_dict['d']
        self.lat = data_dict['x']
        self.lng = data_dict['y']

    id: int
    name: str
    direction: int
    lat: float
    lng: float


@dataclass
class RouteProjection:
    forward: np.ndarray
    backward: np.ndarray
