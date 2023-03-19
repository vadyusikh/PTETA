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

REQUEST_URI = 'https://gt.kh.ua/?do=api&fn=gt&noroutes'
DATETIME_PATTERN = '%Y-%m-%d %H:%M:%S'

REQ_TIME_DELTA = 5.0
PROCESS_FREQUENCY = 30

START_DATE = datetime.now().strftime(DATETIME_PATTERN)
END_DATE = (datetime.now() + dt.timedelta(days=30)).strftime(DATETIME_PATTERN)
END_DATE_2 = (datetime.now() + dt.timedelta(days=30, seconds=2 * PROCESS_FREQUENCY)).strftime(DATETIME_PATTERN)

response_prev = dict()


def get_response_folder(datetime_):
    return f"../../data/local/jsons/kharkiv/trans_data_{datetime_.strftime('%d_%b_%Y').upper()}"


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


def load_responses(resp_path):
    resp_tm = datetime.strptime(resp_path.name[6:-5], '%Y-%m-%d %H;%M;%S')
    resp_tm_str = resp_tm.replace(tzinfo=tzlocal()).isoformat()

    with open(resp_path) as f:
        resp = json.load(f)
        if ("rows" in resp) and (resp["rows"]):
            return [row + [resp['timestamp'], resp_tm_str]
                    for row in resp['rows']]
        else:
            return []


def accumulate_responses_from_folder(folder_path):
    file_path_list = list(folder_path.iterdir())
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(tqdm(executor.map(load_responses, file_path_list),
                            total=len(file_path_list), mininterval=10, leave=False,
                            desc="Accumulate responses from folder")
                       )
        resp_list = []
        for resp_ in results:
            resp_list += resp_
        print(f"Accumulate responses from folder\n" \
              f"\t{len(resp_list)} / {len(resp_list)} [avg={len(resp_list) / len(resp_list):.2f}]")

    columns = ['imei', 'lat', 'lng', 'speed', 'gps_datetime_origin', 'orientation', 'route_name',
               'route_type', 'vehicle_id', 'dd', 'gpstime', 'response_datetime']

    return pd.DataFrame(resp_list, columns=columns)


def clear_data(in_df):
    unique_data = []

    imei_list = in_df['imei'].value_counts().index
    imei_tqdm = tqdm(
        imei_list, total=len(imei_list), mininterval=10, leave=False, desc="Clear data"
    )

    for imei in imei_tqdm:
        row_data = in_df[in_df['imei'] == imei].values.tolist()
        result = [row_data[0]]
        for row0, row1 in zip(row_data[:-1], row_data[1:]):
            if row0[1:9] != row1[1:9]:
                result += [row1]
        unique_data += result

    df_unique = pd.DataFrame(unique_data, columns=in_df.columns)
    print(f"Clear data\n\t{len(in_df)} / {len(df_unique)} [avg {len(in_df) / len(df_unique):.02f}]")
    return df_unique


def after_process_data():
    kharkiv_folder_path = pathlib.Path("../../data/local/jsons/kharkiv")
    kharkiv_folder_list = [p for p in kharkiv_folder_path.iterdir() if "trans_data_" in p.name]
    kharkiv_folders_list = sorted(kharkiv_folder_list,
                                  key=lambda p: datetime.strptime(p.name[11:], '%d_%b_%Y'))

    for folder_path in tqdm(kharkiv_folders_list[:-1]):
        df = accumulate_responses_from_folder(folder_path)
        df.to_parquet(folder_path.parent / 'archive/origin' / (folder_path.name + '_origin.parquet'))

        df_u = clear_data(df)
        df_u.to_parquet(folder_path.parent / 'archive/optimized' / (folder_path.name + '_optimized.parquet'))


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
            path =  get_response_folder(dt_now)

            try:# for cases folder don't exist yet
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
