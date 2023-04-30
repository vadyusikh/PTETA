import datetime as dt
import json
import pathlib
import shutil
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from tqdm.auto import tqdm

from PTETA.configs.config import FILENAME_DATETIME_FORMAT, DATETIME_PATTERN
from PTETA.listener.utils.functions import \
    dict_special_comparator, \
    load_all_responses, \
    display_folder_status

REQUEST_URI = 'http://www.trans-gps.cv.ua/map/tracker/?selectedRoutesStr='

CHERNIVTSI_FOLDER_PATH = pathlib.Path(f"../../data/local/chernivtsi")
CHERNIVTSI_REQUESTS_PATH = CHERNIVTSI_FOLDER_PATH / "jsons"
CHERNIVTSI_TABLES_PATH = CHERNIVTSI_FOLDER_PATH / "tables"

REQ_TIME_DELTA = 5

START_DATE = (datetime.now().replace(hour=3, minute=25) - timedelta(days=1)).strftime(DATETIME_PATTERN)
END_DATE = (datetime.now() + dt.timedelta(days=30)).strftime(DATETIME_PATTERN)

response_prev = dict()


def get_response_folder(datetime_=None):
    if datetime_ is None:
        datetime_ = datetime.now()
    return CHERNIVTSI_REQUESTS_PATH / f"trans_data_{datetime_.strftime('%d_%b_%Y').upper()}"


def request_data():
    dt_now = datetime.now()
    dt_now_str = dt_now.strftime(DATETIME_PATTERN)
    dt_now_file_str = dt_now.astimezone(timezone.utc).strftime(FILENAME_DATETIME_FORMAT)

    try:
        request = requests.get(REQUEST_URI)
        if (request is None) or (request.text is None):
            return
        response_cur = json.loads(request.text)

        global response_prev

        keys_prev = set(response_prev.keys())
        keys_cur = set(response_cur.keys())

        optimized_data_list = list()
        for imei in keys_prev.intersection(keys_cur):
            if not dict_special_comparator(response_prev[imei], response_cur[imei]):
                optimized_data_list += [response_cur[imei]]

        for imei in keys_cur.difference(keys_prev):
            optimized_data_list += [response_cur[imei]]

        for resp in optimized_data_list:
            resp['response_datetime'] = dt_now_str

        if optimized_data_list:
            path = get_response_folder(dt_now)
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)

            f_name = pathlib.Path(f"trans_{dt_now_file_str}.json")

            result = {d['imei']: d for d in optimized_data_list}
            if (path / f_name).is_file():
                print(f"{dt_now_file_str} : '{f_name}' file is already exists!")
                return

            with open(str(path / f_name), 'w', encoding='utf8') as out_f:
                json.dump(result, out_f, ensure_ascii=False)

            response_prev = response_cur
    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
        print(f"{dt_now_file_str} : error while trying to GET data\n"
              f"\t{err}\n")


def get_df(jsons_content):
    data = list()
    for r in jsons_content:
        if r[1] is not None:
            data += [val for _, val in r[1].items()]

    df_ = pd.DataFrame(data)
    useful_columns = list(set(df_.columns) ^ {'response_datetime'})
    duplicates_num = df_.duplicated(useful_columns).value_counts().get(True, 0)
    print(f"Total duplicates are {duplicates_num} of {len(df_)}")
    return df_.drop_duplicates(useful_columns)


def folder_to_df(folder):
    df_ = get_df(load_all_responses(folder))
    df_file_path = CHERNIVTSI_TABLES_PATH / f"data_for_{str(folder.name)[11:]}.parquet"
    df_.to_parquet(df_file_path)


def after_process_data():
    print(f"Data after processing started at {datetime.now()}")
    folders_list = [p for p in CHERNIVTSI_REQUESTS_PATH.iterdir() if 'trans_data_' in str(p)]
    folders_list = sorted(folders_list, key=lambda x: datetime.strptime(str(x.name)[11:], '%d_%b_%Y'))[:-1]
    print(f"\tFolders to process {folders_list}")
    pbar = tqdm(folders_list, leave=False)
    for folder in pbar:
        pbar.set_postfix_str(str(folder))
        print(f"\tFolder conversion to df started at {datetime.now()}")
        folder_to_df(folder)
        print(f"\tFolder conversion to df finished at {datetime.now()}")
        time.sleep(1)
        print(f"Start delete {folder}")
        shutil.rmtree(folder)
        print(f"{folder} is deleted")
    print(f"Data after processing finished at {datetime.now()}")


def main():
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 8})
    scheduler.add_job(
        request_data, 'interval',
        seconds=REQ_TIME_DELTA,
        end_date=END_DATE,
        id='listener')

    scheduler.add_job(
        after_process_data, 'interval',
        start_date=START_DATE,
        days=1,
        end_date=END_DATE,
        id='after_process_data'
    )

    scheduler.add_job(
        display_folder_status, 'interval',
        kwargs={"get_folder_fn": get_response_folder},
        start_date=START_DATE,
        minutes=20,
        end_date=END_DATE,
        id='display_folder_status'
    )

    scheduler.start()

    # This code will be executed after the sceduler has started
    try:
        print('Scheduler started!')
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        if scheduler.state:
            scheduler.shutdown()


if __name__ == "__main__":
    main()
