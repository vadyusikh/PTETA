import numpy as np
from tqdm import tqdm

from PTETA.utils.TrackENU import TrackENU, get_track_enu
from PTETA.utils.datamodels import BusStop, RouteProjection


class RouteENU:
    def __init__(self, route_info: dict, center: list):
        self.row_route_info = route_info
        self.center = np.array(center)

        self.stops_forward = [BusStop(stp) for stp in route_info['stops']['forward']]
        self.stops_backward = [BusStop(stp) for stp in route_info['stops']['backward']]

        self.track_forward = get_track_enu(route_info['scheme']['forward'], center)
        self.track_backward = get_track_enu(route_info['scheme']['backward'], center)

    def get_projection_on_track(self, points: np.array, is_geod:bool = True) -> RouteProjection:
        return RouteProjection(
            forward=self.track_forward.get_projection_data(points, is_geod),
            backward=self.track_backward.get_projection_data(points, is_geod)
        )
