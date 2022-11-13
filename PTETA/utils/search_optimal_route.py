from pathlib import Path
import json
import requests
from tqdm import tqdm

from PTETA.configs.config import CHERNIVTSI_CENTER
from PTETA.utils.RouteENU import RouteENU
from PTETA.configs.key_configs import EWAY_CHERNIVTSI_REQUEST_PARAMS as REQ_PARAMS
from typing import List, Union


def get_routes_list(routes_dir: Path) -> List[Path]:
    dir_list = [f_path for f_path in routes_dir.iterdir()]
    dir_list = [f_path for f_path in dir_list
                if f_path.is_file()
                and f_path.name.endswith('.json')
                and f_path.stat().st_size != 0]
    return dir_list


def load_eway_route(
        route_number: Union[str, int],
        city: str = "chernivtsi",
        language: str = 'en',
        cookies: dict = REQ_PARAMS['cookies'],
        headers: dict = REQ_PARAMS['headers'],

) -> dict:
    route_url = f'https://www.eway.in.ua/ajax/{language}/{city}/routeInfo/{route_number}'
    response = requests.get(route_url, cookies=cookies,headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise ConnectionError("Request not processed!")


def save_route(
        route_number: Union[str, int],
        destination_file_path: Path = None,
        destination_dir: Path = None,
        **kwargs
) -> None:
    route_json = load_eway_route(route_number=route_number, **kwargs)

    if destination_file_path is None and destination_dir is None:
        raise ValueError(f"destination_file_path and destination_dir not provided! Provide at least one!")
    elif destination_file_path is None :
        res_general = route_json['general']
        file_name = f"{res_general['tn'].lower()}_{res_general['rn'].upper()}.json"
        destination_file_path = destination_dir / file_name

    with open(str(destination_file_path), "w") as out_file:
        json.dump(route_json, out_file)


BUS_ROUTES_DIR = Path('../../data/routes/chernivtsi/eway/bus')
TROLBUS_ROUTES_DIR = Path('../../data/routes/chernivtsi/eway/trolbus')
ROUTES_DIR = Path('../../data/routes/chernivtsi/eway/test2')


if __name__ == "__main__":
    print("Hello! I'm empty, please fill me!")

    ### TEST
    # ROUTES_DIR.mkdir(parents=True, exist_ok=True)
    # for r_num in tqdm(range(30)):
    #     try:
    #         save_route(route_number=r_num, destination_dir=ROUTES_DIR)
    #     except Exception as e:
    #         print(e)
