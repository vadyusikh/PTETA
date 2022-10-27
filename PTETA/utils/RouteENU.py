import pymap3d
import numpy as np


class RouteENU:
    def __init__(self, route_geod: np.array, center: np.array):
        self.route_enu = None
        self.route_geod = route_geod
        self.center = center
        self.ellipsoid = pymap3d.ellipsoid.Ellipsoid('wgs84')

        self.process_route()

    def __len__(self) -> int:
        return len(self.route_geod)

    def convert(self, coord_geod: np.array) -> np.array:
        e, n, u = pymap3d.geodetic2enu(
            lat=coord_geod[0], lon=coord_geod[1], h=0,
            lat0=self.center[0], lon0=self.center[1], h0=0,
            ell=self.ellipsoid, deg=True)

        return np.array(e, n)

    def process_route(self):
        self.route_enu = [self.convert(point) for point in self.route_geod]
        self.route_enu = np.array(self.route_enu)