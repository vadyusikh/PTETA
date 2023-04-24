import datetime as dt
import json
import os
import time
from datetime import datetime, timezone, timedelta
from queue import Queue

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler

from PTETA.configs.config import DATETIME_PATTERN
from PTETA.listener.utils.support_classes import LockingCounter
from aws.src.TransGPSCVMonitor import TransGPSCVMonitor

REQUEST_URI = 'https://gt.kh.ua/?do=api&fn=gt&noroutes'
GPSTIME_TIMEZONE = timezone(timedelta(hours=2))

REQUEST_FREQUENCY = 5
PROCESS_FREQUENCY = 60 * 10

START_DATE = (datetime.now() - dt.timedelta(minutes=9)).strftime(DATETIME_PATTERN)
END_DATE = (datetime.now() + dt.timedelta(days=30)).strftime(DATETIME_PATTERN)
END_DATE_2 = (datetime.now() + dt.timedelta(days=30, seconds=2 * PROCESS_FREQUENCY)).strftime(DATETIME_PATTERN)
COLUMNS = [
    'imei', 'lat', 'lng', 'speed', 'gps_datetime_origin', 'orientation',
    'route_name', 'route_type', 'vehicle_id', 'dd', 'gpstime', 'response_datetime'
]


def request_data(process_queue: Queue, request_counter: LockingCounter) -> None:
    dt_now = datetime.now()
    dt_tz_str = dt.datetime.now(dt.timezone.utc).strftime(f"{DATETIME_PATTERN}%z")

    try:
        request = requests.get(REQUEST_URI)
        if (request is None) or (request.text is None):
            return
        response = json.loads(request.text)
        response['response_datetime'] = dt_tz_str

        try:
            process_queue.put(response, timeout=5)
        except process_queue.Full as e:
            print(f"{dt_now.strftime(DATETIME_PATTERN)}\n"
                  f"\tQueue is full, its' size is {process_queue.qsize()}"
                  f"\t{e}\n")
            raise e
        request_counter.increment()

    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
        print(f"{dt_now.strftime(DATETIME_PATTERN)} : error while trying to GET data\n"
              f"\t{err}\n")


def clear_data(in_df, verbose: bool = False):
    unique_data = []

    in_df["gpstime_ns"] = pd.to_datetime(in_df["gpstime"])
    imei_list = in_df['imei'].value_counts().index

    for imei in imei_list:
        row_data = in_df[in_df['imei'] == imei].values.tolist()
        row_data = sorted(row_data, key=lambda x: x[-1])
        result = [row_data[0][:-1]]
        for row0, row1 in zip(row_data[:-1], row_data[1:]):
            if row0[1:9] != row1[1:9]:
                result += [row1[:-1]]
        unique_data += result

    del in_df['gpstime_ns']

    df_unique = pd.DataFrame(unique_data, columns=in_df.columns)
    if verbose:
        print(f"Clear data\n\t{len(in_df)} / {len(df_unique)} [avg {len(in_df) / len(df_unique):.02f}]")
    return df_unique


def process_data(
        request_queue: Queue,
        last_avl_df_queue: Queue,
        monitor: TransGPSCVMonitor,
        records_counter: LockingCounter,
        verbose: bool = False
) -> None:
    if verbose:
        print(f"process_data::{datetime.now().strftime(DATETIME_PATTERN)} : "
              f"Queue len is {request_queue.qsize()}")
    avl_data_list = []
    while not request_queue.empty():
        response = request_queue.get()
        if 'rows' not in response.keys():
            return
        avl_data_list += [
            row + [response['timestamp'], response['response_datetime']]
            for row in response['rows']
        ]

    if not avl_data_list:
        return

    df = pd.DataFrame(avl_data_list, columns=COLUMNS)

    if last_avl_df_queue.empty():
        pass
    else:
        last_avl_df = last_avl_df_queue.get()
        while not last_avl_df_queue.empty():
            last_avl_df_queue.get()

        df = pd.concat([df, last_avl_df])

    df_unique = clear_data(df)

    if verbose:
        print(f"\tprocess_data:: unique {len(df_unique)} of {df}")

    last_avl_list = list()
    for state, frame in df_unique.groupby('imei'):
        item = frame.sort_values(
            by="gpstime", ascending=False,
            key=lambda x: x.astype('datetime64[ns]')
        ).iloc[0]
        last_avl_list.append(item)

    last_avl_df = pd.DataFrame(last_avl_list, columns=df_unique.columns)
    last_avl_df_queue.put(last_avl_df)
    if verbose:
        print(f"\tprocess_data:: New last awl {len(last_avl_df)} was {len(last_avl_list)}")

    if len(df_unique) > 0:
        monitor.write_to_db(df_unique.to_dict('records'))
        records_counter.increment(len(df_unique))

    if verbose:
        print(f"\tprocess_data:: to write {len(df_unique)} of unique {len(df)}")


def verbose_update(
        records_counter: LockingCounter,
        request_counter: LockingCounter
) -> None:
    dt_now = datetime.now()

    if request_counter.get_count() % 1_000 > 50:
        request_counter.set_is_shown(True)
    elif request_counter.get_is_shown():
        print(f"{dt_now.strftime(DATETIME_PATTERN)}: "
              f"Done {records_counter.get_count():6}/{request_counter.get_count():6}(records/request)\n")
        request_counter.set_is_shown(False)
        request_counter.set_last_update(dt_now)

    if (dt_now.hour == 0) and (request_counter.get_count() > 5_000):
        request_counter.set_count(0)
        records_counter.set_count(0)

    if (dt_now - request_counter.get_last_update()).seconds > 30 * 60:
        print(f"{dt_now.strftime(DATETIME_PATTERN)} : "
              f"Done {records_counter.get_count():6}/{request_counter.get_count():6}(records/request)"
              f" \t(Last log was {(dt_now - request_counter.get_last_update()).seconds} sec ago)\n")
        request_counter.set_last_update(dt_now)


def main():
    request_queue = Queue(maxsize=100_000)
    last_avl_df_queue = Queue(maxsize=1_000)
    records_counter = LockingCounter()
    request_counter = LockingCounter()
    request_counter.set_is_shown(True)

    connection_config = dict({
        'host': os.environ['RDS_HOSTNAME'],
        'database': "pteta_db",
        'user': "postgres",
        'password': os.environ['RDS_PTETA_DB_PASSWORD']
    })

    monitor = TransGPSCVMonitor(connection_config=connection_config, data_model="kharkiv")

    scheduler = BackgroundScheduler(job_defaults={'max_instances': 8})
    scheduler.add_job(
        request_data,
        args=(request_queue, request_counter),
        trigger='interval',
        seconds=REQUEST_FREQUENCY,
        end_date=END_DATE,
        id='listener')

    scheduler.add_job(
        process_data,
        args=(request_queue, last_avl_df_queue, monitor, records_counter),
        trigger='interval',
        seconds=PROCESS_FREQUENCY,
        start_date=START_DATE,
        end_date=END_DATE_2,
        id='process_data'
    )

    scheduler.add_job(
        verbose_update,
        args=(records_counter, request_counter),
        trigger='interval',
        seconds=10,
        start_date=START_DATE,
        end_date=END_DATE_2,
        id='verbose_update'
    )

    scheduler.start()

    # This code will be executed after the sceduler has started
    try:
        print('Scheduler started!')
        while True:
            print(f"{datetime.now().strftime(DATETIME_PATTERN)} : Queue len is {request_queue.qsize()}")
            time.sleep(600)

    except KeyboardInterrupt:
        if scheduler.state:
            scheduler.shutdown()


if __name__ == "__main__":
    main()
