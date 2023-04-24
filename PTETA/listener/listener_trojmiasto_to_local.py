import datetime as dt
import json
import pathlib
import shutil
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from tqdm import tqdm

from PTETA.configs.config import FILENAME_DATETIME_FORMAT, DATETIME_PATTERN
from PTETA.listener.utils.functions import \
    dict_special_comparator, load_all_responses

REQUEST_URI = 'https://mapa.ztm.gda.pl/gpsPositions?v=2'

TROJMIASTO_FOLDER_PATH = pathlib.Path("../../data/local/trojmiasto")
TROJMIASTO_REQUESTS_PATH = TROJMIASTO_FOLDER_PATH / "jsons"
TROJMIASTO_TABLES_PATH = TROJMIASTO_FOLDER_PATH / "tables"

REQ_TIME_DELTA = 5.0
PROCESS_FREQUENCY = 30

START_DATE = (datetime.now().replace(hour=3, minute=55) - timedelta(days=1)).strftime(DATETIME_PATTERN)
END_DATE = (datetime.now() + dt.timedelta(days=30)).strftime(DATETIME_PATTERN)

latest_avl = dict()


def get_response_folder(datetime_):
    return TROJMIASTO_REQUESTS_PATH / f"trans_data_{datetime_.strftime('%d_%b_%Y').upper()}"


def request_data():
    dt_now = datetime.now()

    try:
        request = requests.get(REQUEST_URI)
        if (request is None) or (request.text is None):
            return
        response = json.loads(request.text)

        global latest_avl
        optimized_data_list = list()

        for avl in response['vehicles']:
            if avl['vehicleId'] in latest_avl.keys():
                is_equal = dict_special_comparator(
                    latest_avl[avl['vehicleId']], avl,
                    ["generated", 'lastUpdate', 'response_datetime']
                )
                if not is_equal:
                    latest_avl[avl['vehicleId']] = avl
                    optimized_data_list += [avl]
            else:
                optimized_data_list += [avl]
                latest_avl[avl['vehicleId']] = avl

        response['vehicles'] = optimized_data_list

        path = get_response_folder(dt_now)
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)

        f_name = pathlib.Path(f"trans_{dt_now.astimezone(timezone.utc).strftime(FILENAME_DATETIME_FORMAT)}.json")

        if (path / f_name).is_file():
            print(f"{dt_now.strftime(DATETIME_PATTERN)} : '{f_name}' file is already exists!")
            return

        with open(str(path / f_name), 'w', encoding='utf8') as out_f:
            json.dump(response, out_f, ensure_ascii=False)

    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
        print(f"{dt_now.strftime(DATETIME_PATTERN)} : error while trying to GET data\n"
              f"\t{err}\n")


def process_single_response(response):
    file_name, result = response[0], response[1]['vehicles']
    response_dt = datetime.strptime(f"{file_name[-29:-5]}", f"{FILENAME_DATETIME_FORMAT}").astimezone(timezone.utc)
    response_dt_str = response_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    for i, _ in enumerate(result):
        result[i]['lastUpdate'] = response[1]['lastUpdate']
        result[i]['response_datetime'] = response_dt_str
    return result


def clear_data(responses: list):
    optimized_data_list = list()
    latest_data = dict()

    for resp in tqdm(responses, desc="Data clearing"):
        for avl in resp:
            if avl['vehicleId'] in latest_data.keys():
                is_equal = dict_special_comparator(
                    latest_data[avl['vehicleId']], avl,
                    ['lastUpdate', 'response_datetime']
                )

                if not is_equal:
                    latest_data[avl['vehicleId']] = avl
                    optimized_data_list += [avl]
            else:
                optimized_data_list += [avl]
                latest_data[avl['vehicleId']] = avl

    return optimized_data_list


def accumulate_responses_from_folder(folder_path):
    responses = load_all_responses(folder_path)
    cleared_data = clear_data(list(map(process_single_response, responses)))
    return pd.DataFrame(cleared_data)


def after_process_data():
    trojmiasto_folders = [p for p in TROJMIASTO_REQUESTS_PATH.iterdir()
                          if "trans_data_" in p.name]
    trojmiasto_folders = sorted(trojmiasto_folders,
                                key=lambda p: datetime.strptime(p.name[11:], '%d_%b_%Y'))

    for folder_path in tqdm(trojmiasto_folders[:-1], desc="Folders processing"):
        print(f"Processing '{folder_path}'")
        df = accumulate_responses_from_folder(folder_path)
        df.to_parquet(TROJMIASTO_TABLES_PATH / (folder_path.name + '_origin.parquet'))

        print(f"Start delete {folder_path}")
        shutil.rmtree(folder_path)
        print(f"{folder_path} is deleted")


def main():
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 8})
    scheduler.add_job(
        request_data, 'interval',
        start_date=START_DATE,
        seconds=REQ_TIME_DELTA,
        end_date=END_DATE,
        id='listener')

    scheduler.add_job(
        after_process_data, 'interval',
        start_date=START_DATE,
        days=1,
        end_date=END_DATE,
        id='after_process_data')

    scheduler.start()

    # This code will be executed after the sceduler has started
    try:
        print('Scheduler started!')
        display_files_num = True
        prev_update = datetime.now().replace(minute=0, second=0, microsecond=0)
        while 1:
            dt_now = datetime.now()
            path = get_response_folder(dt_now)

            try:  # in cases folder don't exist yet
                files_list = list(pathlib.Path(path).iterdir())
            except:
                files_list = []

            if len(files_list) % 1_000 > 10:
                display_files_num = True
            elif display_files_num:
                print(f"{dt_now.strftime(DATETIME_PATTERN)}"
                      f"\t Total files num is {len(files_list)}")
                display_files_num = False
                prev_update = dt_now

            if (dt_now - prev_update).seconds > 30 * 60:
                print(f"{dt_now.strftime(DATETIME_PATTERN)} : "
                      f"(last update {(dt_now - prev_update).seconds} sec ago)"
                      f"\t Total files num is {len(files_list)}")
                prev_update = dt_now

            time.sleep(3)
    except KeyboardInterrupt:
        if scheduler.state:
            scheduler.shutdown()


if __name__ == "__main__":
    main()
