from datetime import datetime
import json
import requests
import pathlib
import time

from apscheduler.schedulers.background import BackgroundScheduler

REQUEST_URI = 'https://gt.kh.ua/?do=api&fn=gt&noroutes'
END_DATE = '2023-02-27 23:59:00'
START_DATE = '2022-10-03 00:03:00'
REQ_TIME_DELTA = 5.0

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
    # print("\n".join([str(p) for p in pathlib.Path("../../data/local/jsons").iterdir()]) )
