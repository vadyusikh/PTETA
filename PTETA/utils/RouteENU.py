import numpy as np
from tqdm import tqdm

from TrackENU import get_track_enu
from datamodels import BusStop


class RouteENU:
    def __init__(self, route_info: dict, center: list):
        self.row_route_info = route_info
        self.center = np.array(center)

        self.stops_forward = [BusStop(stp) for stp in route_info['stops']['forward']]
        self.stops_backward = [BusStop(stp) for stp in route_info['stops']['backward']]

        self.track_forward = get_track_enu(route_info['scheme']['backward'])
        self.track_backward = get_track_enu(route_info['scheme']['backward'])


    def process_trace(self, trace: np.ndarray):
        dists = [None] * len(trace)

        trace_enu = self.track_forward.convert_to_enu(trace)

        for i, coord in tqdm(enumerate(trace)):
            dists[i] = r.get_proj_on_track(r.convert_to_enu(coord))

        end = time.perf_counter()
        print(f"{end - st}")
