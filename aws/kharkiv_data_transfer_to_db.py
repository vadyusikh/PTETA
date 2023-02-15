from datetime import datetime
import numpy as np
import psycopg2.extras
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import os

from src.TransGPSCVMonitor import TransGPSCVMonitor

CONNECTION_CONFIG = dict({
        'host': os.environ['RDS_HOSTNAME'],
        'database': "pteta_db",
        'user': "postgres",
        'password': os.environ['RDS_PTETA_DB_PASSWORD']
    })

BATCH_SIZE = 5_000


def main():
    monitor = TransGPSCVMonitor(CONNECTION_CONFIG, data_model="kharkiv")

    # kharkiv_folder_path = Path("../../pet_project/kharkiv/archive/optimized")
    kharkiv_folder_path = Path("../data/local/jsons/kharkiv/archive/optimized")

    file_path_list = list(kharkiv_folder_path.iterdir())
    print([p.name for p in file_path_list])
    file_path_list = sorted(file_path_list,
                            key=lambda p: datetime.strptime(p.name[11:22], '%d_%b_%Y'))
    print([p.name for p in file_path_list])

    # return
    for df_path in tqdm(file_path_list):
        print(f"{df_path.name}")
        df_sum = pd.read_parquet(df_path)

        df_cur = df_sum[:]

        batch_tqdm = tqdm(df_cur.groupby(np.arange(len(df_cur)) // BATCH_SIZE))
        for batch_number, batch_df in batch_tqdm:
            batch_df = batch_df.where(pd.notnull(batch_df), None)
            try:
                monitor.write_to_db(batch_df.to_dict('records'))
            except Exception as e:
                print(f"Error raised on batch number {batch_number}!")
                raise e


if __name__ == "__main__":
    main()
