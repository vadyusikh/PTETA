from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import requests
import os
import pandas as pd
from pathlib import Path
import shutil
import time
from tqdm.auto import tqdm
import zipfile

# from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

REQUEST_URI = 'http://www.trans-gps.cv.ua/map/tracker/?selectedRoutesStr='
LOCAL_DATA_DIR_PATH = Path('../../data/local')
END_DATE = '2022-10-09 23:59:00'
START_DATE = '2022-10-03 00:03:00'
response_prev = dict()


def save_data(optimized_data_list, dt_now, data_dir_path=''):
    if optimized_data_list:
        path = f"trans_data_{dt_now.strftime('%d_%b_%Y').upper()}"
        Path(data_dir_path, path).mkdir(parents=True, exist_ok=True)

        f_name = Path(f"trans_{dt_now.strftime('%Y-%m-%d %H;%M;%S')}.json")

        result = {d['imei']: d for d in optimized_data_list}
        if (path / f_name).is_file():
            print(f"{dt_now.strftime('%Y-%m-%d %H;%M;%S')} : '{f_name}' file is already exists!")
            return

        with open(str(path / f_name), 'w', encoding='utf8') as out_f:
            json.dump(result, out_f, ensure_ascii=False)


def request_data():
    dt_now = datetime.now()

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

        response_prev = response_cur
        save_data(optimized_data_list, dt_now, dir_path=Path(LOCAL_DATA_DIR_PATH, "jsons"))

    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
        print(f"{dt_now.strftime('%Y-%m-%d %H;%M;%S')} : error while trying to GET data\n"
              f"\t{err}\n")


def read_json_TPE(f_path):
    try:
        with open(f_path, 'r', encoding='utf-8') as file:
            return json.load(file), None
    except Exception as e:
        print(f"Unable to open : {f_path}")
        print(f"Exception is '{e}'")
        return None, file


def get_df(jsons_content):
    data = list()
    for r in jsons_content:
        if r[1] is None:
            data += [val for _, val in r[0].items()]

    df = pd.DataFrame(data)
    duplicates_num = df.duplicated().value_counts().get(True, 0)
    print(f"Total duplicates are {duplicates_num} of {len(df)}")
    return df.drop_duplicates()


def folder_to_df(src_folder, dst_dir=Path(LOCAL_DATA_DIR_PATH, "tables")):
    fpaths_list = sorted(list(src_folder.iterdir()))

    with ThreadPoolExecutor(max_workers=40) as executor:
        fpath_list = fpaths_list
        results = list(
            tqdm(
                executor.map(read_json_TPE, fpath_list),
                total=len(fpath_list), miniters=int(len(fpath_list) // 10),
                desc=f"Read {str(src_folder)} to write df", ascii=True
            )
        )

    df = get_df(results)
    df_file_name = f"data_for_{str(src_folder)[11:]}.feather"
    df.to_feather(Path(dst_dir,df_file_name), encoding='utf-8', index=False, header=True)


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in tqdm(files, position=0, leave=True, desc=f"Zip {path}"):
            ziph.write(os.path.join(root, file),
                       os.path.relpath(os.path.join(root, file),
                                       os.path.join(path, '..')))


def write_zip_archive(folder):
    zip_fname = f"archives/{folder}.zip"
    with zipfile.ZipFile(zip_fname, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipdir(f"{folder}", zipf)


def afterprocess_data():
    print(f"Data aterprocessing started at {datetime.now()}")
    folders_list = [p for p in pathlib.Path().iterdir() if 'trans_data_' in str(p)]
    folders_list = sorted(folders_list, key=lambda x: datetime.strptime(str(x)[11:], '%d_%b_%Y'))[:-1]
    print(f"\tFolders to process {folders_list}")
    pbar = tqdm(folders_list, leave=False)
    for folder in pbar:
        pbar.set_postfix_str(str(folder))
        print(f"\tFolder conversion to df started at {datetime.now()}")
        folder_to_df(folder)
        print(f"\tFolder conversion to df fibished at {datetime.now()}")
        print(f"\tArchiving of folder started at {datetime.now()}")
        write_zip_archive(folder)
        print(f"\tArchiving of folder finished at {datetime.now()}")
        time.sleep(1)
        print(f"Start delete {folder}")
        shutil.rmtree(folder)
        print(f"{folder} is deleted")
    print(f"Data aterprocessing finished at {datetime.now()}")


def main():
    scheduler = BackgroundScheduler(job_defaults={'max_instances': 8})
    scheduler.add_job(
        request_data, 'interval',
        seconds=1.02,
        end_date=END_DATE,
        id='listener')

    scheduler.add_job(
        afterprocess_data, 'interval',
        days=1,
        start_date=START_DATE,
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
            path = f"trans_data_{dt_now.strftime('%d_%b_%Y').upper()}"
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
