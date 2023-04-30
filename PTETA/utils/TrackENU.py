import numpy as np
import pymap3d


def get_track_enu(coord_str: str, center: np.array):
    coords = [[float(line) for line in point.split(',')]
              for point in coord_str.split(' ')]
    return TrackENU(np.array(coords), center)


def optimize_track(points: np.array, min_dist: float = 10) -> np.array:
    opt_track = [points[0]]
    i = 0

    while i < len(points[:-1, :]):
        c1 = points[i, :]
        j = 0
        dist = np.linalg.norm(points[i + j, :] - c1)
        while j < len(points[i + 1:, :]) and dist < min_dist:
            j += 1
            dist = np.linalg.norm(points[i + j, :] - c1)

        if np.linalg.norm(points[i + max(1, j - 1), :] - c1) > 1e-2:
            opt_track += [points[i + max(1, j - 1), :]]
        i += max(1, j - 1)

    return np.array(opt_track)


class TrackENU:
    def __init__(self, track_geod: np.array, center: np.array):
        self.track_enu = None
        mask = np.append([True], np.linalg.norm(np.diff(track_geod, axis=0), axis=1) > 1e-8)
        self.track_geod = track_geod[mask]
        self.center = center
        self.ellipsoid = pymap3d.ellipsoid.Ellipsoid('wgs84')

        self.process_track()

        self.unit_vec = np.diff(self.track_enu, axis=0, n=1)
        self.segment_length = np.linalg.norm(self.unit_vec, axis=1)
        self.unit_vec /= self.segment_length[:, None]
        self.cumulative_length = np.append(0, np.cumsum(self.segment_length))

    def process_track(self, min_dist: float = 50) -> None:
        self.track_enu = self.convert_to_enu(self.track_geod)
        self.track_enu = optimize_track(np.array(self.track_enu), min_dist)
        self.track_geod = self.convert_to_geod(self.track_enu)

    def __len__(self) -> int:
        return len(self.track_geod)

    @classmethod
    def check_coordinates(cls, coords: np.array) -> np.array:
        if coords.ndim > 2:
            raise ValueError(
                f"Wrong input dimension - got {coords.ndim}, should be 1 or 2."
                f"It's shape is {coords.shape}"
            )

        if coords.ndim == 1:
            return np.expand_dims(coords, axis=0)

        return coords

    def convert_to_enu(self, coord_geod: np.array) -> np.array:
        coord_geod = self.check_coordinates(coord_geod)

        e, n, _ = pymap3d.geodetic2enu(
            lat=coord_geod[:, 0],
            lon=coord_geod[:, 1],
            h=np.zeros_like(coord_geod[:, 0]),
            lat0=self.center[0],
            lon0=self.center[1],
            h0=0,
            ell=self.ellipsoid, deg=True)

        return np.dstack([e, n])[0]

    def convert_to_geod(self, coord_enu: np.array) -> np.array:
        coord_enu = self.check_coordinates(coord_enu)

        lat, lon, _ = pymap3d.enu2geodetic(
            e=coord_enu[:, 0],
            n=coord_enu[:, 1],
            u=np.zeros_like(coord_enu[:, 0]),
            lat0=self.center[0],
            lon0=self.center[1],
            h0=0, ell=self.ellipsoid, deg=True
        )

        return np.dstack([lat, lon])[0]

    def proj_on_segment_cos_value(self, seg_num: int, point: np.array) -> float:
        vec2point = point - self.track_enu[seg_num]
        return vec2point.dot(self.unit_vec[seg_num])

    def is_on_segment(self, seg_num: int, point: np.array) -> bool:
        proj_cos_value = self.proj_on_segment_cos_value(seg_num, point)
        return 0 < proj_cos_value <= self.segment_length[seg_num]

    def get_proj_on_seg_data(self, seg_num: int, point: np.array) -> dict:
        """
        https://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line#Vector_formulation
        """
        if not self.is_on_segment(seg_num, point):
            return None

        point2start = point - self.track_enu[seg_num]
        proj_vec = - point2start.dot(self.unit_vec[seg_num]) * self.unit_vec[seg_num]
        tang_vec = point2start + proj_vec

        return {"proj_vec": proj_vec,
                "proj_length": np.linalg.norm(proj_vec),
                "tang_vec": tang_vec,
                "distance_to_line": np.linalg.norm(tang_vec),
                "proj_point": self.track_enu[seg_num] - proj_vec,
                "progress": self.cumulative_length[seg_num] + np.linalg.norm(proj_vec)
                }

    def get_proj_on_track(self, point_enu: np.array) -> dict:
        result = dict()

        seg_proj = list()
        for seg_n, _ in enumerate(self.unit_vec):
            res = self.get_proj_on_seg_data(seg_n, point_enu)
            if res is not None:
                res['segment_number'] = seg_n
                seg_proj.append(res)

        if seg_proj:
            result["segment_projection"] = sorted(seg_proj, key=lambda x: x['distance_to_line'])[0]
        else:
            result["segment_projection"] = None

        dists2points = np.linalg.norm(self.track_enu - point_enu, axis=1)

        result["optimal_point_dist"] = {"point_number": dists2points.argmin(),
                                        "dist_to_point": dists2points[dists2points.argmin()],
                                        "progress": self.cumulative_length[dists2points.argmin()]
                                        }
        result["track_bounds_dist"] = {'start': dists2points[0],
                                       'end': dists2points[-1]}

        return result

    def get_projection_data(self, points: np.array, is_geod: bool = True) -> np.ndarray:
        points = self.check_coordinates(points)
        if is_geod:
            points = self.convert_to_enu(points)

        dists_data = np.zeros_like(points[:, :2])

        for i, point in enumerate(points):
            dist = self.get_proj_on_track(point)

            point_dist = dist["optimal_point_dist"]
            seg_dist = dist["segment_projection"]
            if (seg_dist is None) or (point_dist["dist_to_point"] < seg_dist["distance_to_line"]):
                dists_data[i, :] = [point_dist["dist_to_point"],
                                    point_dist["progress"]
                                    ]
            else:
                dists_data[i, :] = [seg_dist["distance_to_line"],
                                    seg_dist["progress"]
                                    ]

        return dists_data


if __name__ == "__main__":
    from os.path import dirname

    print(dirname(__file__))