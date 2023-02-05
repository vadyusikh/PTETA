import os
from datetime import datetime
import datetime as dt
from queue import Queue
import json
import requests
import pandas as pd
import time

from apscheduler.schedulers.background import BackgroundScheduler
from aws.src.TransGPSCVMonitor import TransGPSCVMonitor
from PTETA.listener.utils.support_classes import LockingCounter

REQUEST_URI = 'http://www.trans-gps.cv.ua/map/tracker/?selectedRoutesStr='
DATETIME_PATTERN = '%Y-%m-%d %H:%M:%S'

REQUEST_FREQUENCY = 1.05
PROCESS_FREQUENCY = 30

START_DATE = datetime.now().strftime(DATETIME_PATTERN)
END_DATE = (datetime.now() + dt.timedelta(days=10)).strftime(DATETIME_PATTERN)
END_DATE_2 = (datetime.now() + dt.timedelta(days=10, seconds=2 * PROCESS_FREQUENCY)).strftime(DATETIME_PATTERN)
COLUMNS_TO_UNIQUE = [
    'imei', 'name', 'lat', 'lng', 'speed', 'orientation', 'gpstime',
    'routeId', 'inDepo', 'busNumber', 'perevId', 'perevName', 'remark', 'online'
]


def request_data(process_queue: Queue, request_counter: LockingCounter) -> None:
    dt_now = datetime.now()
    dt_tz_str = dt.datetime.now(dt.timezone.utc).strftime(f"{DATETIME_PATTERN} %z")

    try:
        request = requests.get(REQUEST_URI)
        if (request is None) or (request.text is None):
            return
        response = json.loads(request.text)
        for imei in response:
            response[imei]['response_datetime'] = dt_tz_str
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
        avl_data_list += [response[key] for key in response]

    if not avl_data_list:
        return

    df = pd.DataFrame(avl_data_list)
    total = len(df)
    df.drop_duplicates(subset=COLUMNS_TO_UNIQUE, inplace=True)

    if verbose:
        print(f"\tprocess_data:: unique {len(df)} of {total}")

    if last_avl_df_queue.empty():
        last_avl_df = pd.DataFrame(columns=df.columns)
    else:
        last_avl_df = last_avl_df_queue.get()
        while not last_avl_df_queue.empty():
            last_avl_df_queue.get()

    df_to_write = df.merge(last_avl_df, on=COLUMNS_TO_UNIQUE, how='left', indicator=True)
    df_to_write = df[(df_to_write['_merge'] == 'left_only').tolist()]

    if len(df_to_write) > 0:
        monitor.write_to_db(df_to_write.to_dict('records'))
        records_counter.increment(len(df_to_write))

    if verbose:
        print(f"\tprocess_data:: to write {len(df_to_write)} of unique {len(df)}")

    last_avl_df = pd.concat([df, last_avl_df])
    last_avl_list = list()
    for state, frame in last_avl_df.groupby('imei'):
        item = frame.sort_values(
            by="gpstime", ascending=False, key=lambda x: x.astype('datetime64[ns]')
        ).iloc[0]
        last_avl_list.append(item)

    last_avl_df = pd.DataFrame(last_avl_list)
    last_avl_df_queue.put(last_avl_df)
    if verbose:
        print(f"\tprocess_data:: New last awl {len(last_avl_df)} was {len(last_avl_list)}")


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

    monitor = TransGPSCVMonitor(connection_config=connection_config, data_model="chernivtsi")

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
