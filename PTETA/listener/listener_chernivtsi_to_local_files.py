from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import datetime as dt
import json
import requests
import os
import pandas as pd
import pathlib
import shutil
import time
from tqdm.auto import tqdm
import zipfile

# chernivtsi
from apscheduler.schedulers.background import BackgroundScheduler

BASE_DATA_PATH = pathlib.Path(f"../../data/local/chernivtsi")

REQUEST_URI = 'http://www.trans-gps.cv.ua/map/tracker/?selectedRoutesStr='
DATETIME_PATTERN = '%Y-%m-%d %H:%M:%S'
DATETIME_FILE_PATTERN = '%Y-%m-%d %H;%M;%S'

REQ_TIME_DELTA = 1.1
PROCESS_FREQUENCY = 30

START_DATE = datetime.now().strftime(DATETIME_PATTERN)
END_DATE = (datetime.now() + dt.timedelta(days=30)).strftime(DATETIME_PATTERN)

response_prev = dict()


def get_response_folder(datetime_):
    return BASE_DATA_PATH / f"jsons/trans_data_{datetime_.strftime('%d_%b_%Y').upper()}"


def request_data():
    dt_now = datetime.now()
    dt_now_str = dt_now.strftime(DATETIME_PATTERN)
    dt_now_file_str = dt_now.strftime(DATETIME_FILE_PATTERN)

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
            if response_prev[imei] != response_cur[imei]:
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


def load_responses(f_path):
    try:
        with open(f_path, 'r', encoding='utf-8') as file:
            return (json.load(file), None)
    except Exception as e:
        print(f"Unable to open : {f_path}")
        print(f"Exception is '{e}'")
        return (None, file)


def get_df(jsons_content):
    data = list()
    for r in jsons_content:
        if r[1] is None:
            data += [val for _, val in r[0].items()]

    df_ = pd.DataFrame(data)
    duplicates_num = df_.duplicated().value_counts().get(True, 0)
    print(f"Total duplicates are {duplicates_num} of {len(df_)}")
    return df_.drop_duplicates()


def folder_to_df(folder):
    f_path_list = sorted(list(folder.iterdir()))

    with ThreadPoolExecutor(max_workers=40) as executor:
        results = list(
            tqdm(
                executor.map(load_responses, f_path_list),
                total=len(f_path_list), miniters=int(len(f_path_list) // 10),
                desc=f"Read {str(folder)} to write df", ascii=True
            )
        )

    df_ = get_df(results)
    df_file_path = BASE_DATA_PATH/f"tables/data_for_{str(folder.name)[11:]}.parquet"
    df_.to_parquet(df_file_path, encoding='utf-8', index=False, header=True)


def afterprocess_data():
    print(f"Data aterprocessing started at {datetime.now()}")
    request_folders = BASE_DATA_PATH / f"jsons"
    folders_list = [p for p in request_folders.iterdir() if 'trans_data_' in str(p)]
    folders_list = sorted(folders_list, key=lambda x: datetime.strptime(str(x.name)[11:], '%d_%b_%Y'))[:-1]
    print(f"\tFolders to process {folders_list}")
    pbar = tqdm(folders_list, leave=False)
    for folder in pbar:
        pbar.set_postfix_str(str(folder))
        print(f"\tFolder conversion to df started at {datetime.now()}")
        folder_to_df(folder)
        print(f"\tFolder conversion to df fibished at {datetime.now()}")
        time.sleep(1)
        print(f"Start delete {folder}")
        shutil.rmtree(folder)
        print(f"{folder} is deleted")
    print(f"Data aterprocessing finished at {datetime.now()}")


def main():
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 8})
    scheduler.add_job(
        request_data, 'interval',
        seconds=REQ_TIME_DELTA,
        end_date=END_DATE,
        id='listener')

    scheduler.add_job(
        afterprocess_data, 'interval',
        days=1,
        end_date=END_DATE,
        id='afterprocess_data'
    )

    print(datetime.now())
    while datetime.now().microsecond > 5_000:
        time.sleep(0.00002)
    print(datetime.now())
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
                print(f"{dt_now.strftime('%Y-%m-%d %H:%M:%S')}"
                      f"\t Total files num is {len(files_list)}")
                display_files_num = False
                prev_update = dt_now

            if (dt_now - prev_update).seconds > 30 * 60:
                print(f"{dt_now.strftime('%Y-%m-%d %H:%M:%S')} : "
                      f"(last update {(dt_now - prev_update).seconds} sec ago)"
                      f"\t Total files num is {len(files_list)}")
                prev_update = dt_now

            time.sleep(3)
    except KeyboardInterrupt:
        if scheduler.state:
            scheduler.shutdown()


if __name__ == "__main__":
    main()
