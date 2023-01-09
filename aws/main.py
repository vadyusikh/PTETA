from datetime import datetime
import numpy as np
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import os

from src.RequestProcessor import RequestProcessor

CONNECTION_CONFIG = dict({
        'host': os.environ['RDS_HOSTNAME'],
        'database': "pteta_db",
        'user': "postgres",
        'password': os.environ['RDS_PTETA_DB_PASSWORD']
    })

BATCH_SIZE = 1_000
# BATCH_SIZE = 10


    def main():
        monitor = RequestProcessor(CONNECTION_CONFIG)

        file_path_list = list(Path("D:/projects/pet_project/tables").iterdir())
        file_path_list = sorted(file_path_list,
                                key=lambda p: datetime.strptime(p.name[9:-4], '%d_%b_%Y'))

        # data_for_04_JAN_2023.csv
        # for df_path in tqdm(file_path_list[5:]):
        # a = 12 + 4 + 9 + 9 + 23 + 3 + 9 + 5 + 3 + 5
        print(file_path_list[-4])
        return
        for df_path in tqdm(file_path_list[:15]):

            df_sum = pd.read_csv(df_path, encoding='utf-8', low_memory=False)
            df_sum = df_sum[:]

            if "response_datetime" not in df_sum:
                df_sum['response_datetime'] = None

            if not df_sum['imei'].dtype == 'O':
                df_sum['imei'] = df_sum['imei'].astype(str, copy=True)

            if not df_sum['busNumber'].dtype == 'O':
                df_sum['busNumber'] = df_sum['busNumber'].astype(str, copy=True)

            df_cur = df_sum[:]
            # df_cur = df_sum[28*5_000:]

            batch_tqdm = tqdm(df_cur.groupby(np.arange(len(df_cur)) // BATCH_SIZE),
                              # miniters=40
                              )
            for batch_number, batch_df in batch_tqdm:
                batch_df = batch_df.where(pd.notnull(batch_df), None)
                try:
                    monitor.write_to_db(batch_df.to_dict('records'))
                except Exception as e:
                    print(f"Error raised on batch number {batch_number}!")
                    raise e


    if __name__ == "__main__":
        main()
