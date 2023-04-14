from dateutil.tz import tzlocal
from datetime import datetime
import datetime as dt
import json
import requests
import pathlib
import time

from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from tqdm import tqdm

REQUEST_URI = 'https://mapa.ztm.gda.pl/gpsPositions?v=2'
DATETIME_PATTERN = '%Y-%m-%d %H:%M:%S'

REQ_TIME_DELTA = 5.0
PROCESS_FREQUENCY = 30

START_DATE = datetime.now().strftime(DATETIME_PATTERN)
END_DATE = (datetime.now() + dt.timedelta(days=30)).strftime(DATETIME_PATTERN)
END_DATE_2 = (datetime.now() + dt.timedelta(days=30, seconds=2 * PROCESS_FREQUENCY)).strftime(DATETIME_PATTERN)

response_prev = dict()


def get_response_folder(datetime_):
    return f"../../data/local/trojmiasto/jsons/trans_data_{datetime_.strftime('%d_%b_%Y').upper()}"


def request_data():
    dt_now = datetime.now()

    try:
        request = requests.get(REQUEST_URI)
        if (request is None) or (request.text is None):
            return
        response = json.loads(request.text)

        path = get_response_folder(dt_now)
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)

        f_name = pathlib.Path(f"trans_{dt_now.strftime('%Y-%m-%d %H;%M;%S')}.json")

        if (path / f_name).is_file():
            print(f"{dt_now.strftime('%Y-%m-%d %H;%M;%S')} : '{f_name}' file is already exists!")
            return

        with open(str(path/f_name), 'w', encoding='utf8') as out_f:
            json.dump(response, out_f, ensure_ascii=False)

    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
        print(f"{dt_now.strftime('%Y-%m-%d %H;%M;%S')} : error while trying to GET data\n"
              f"\t{err}\n")


def main():
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 8})
    scheduler.add_job(
        request_data, 'interval',
        seconds=REQ_TIME_DELTA,
        end_date=END_DATE,
        id='listener')

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

            try:    # in cases folder don't exist yet
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
