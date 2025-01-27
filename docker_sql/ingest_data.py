#!/usr/bin/env python
# coding: utf-8

# # Loading data from nyc dataset
# Since the dataset is no longer in csv format, I convert it from parquet format to csv.

import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name =params.table_name
    url = params.url

    parquet_file_path = 'parquet.csv'
    csv_file_path = 'output.csv'

    # Connection to PostGres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Step 1: Download the parquet file
    os.system(f"wget {url} -O {parquet_file_path}")
    df_parquet = pd.read_parquet(parquet_file_path)   

    # Step 2: Convert the Parquet file to CSV and save it locally
    df_parquet.to_csv(csv_file_path, index=False)

    print(f"Parquet file downloaded, converted to CSV, and saved as {csv_file_path}")

    #Step 3: read the CSV file and write the file into the postgres db
    df_iter = pd.read_csv(csv_file_path, iterator=True, chunksize=10000)
    df_next=next(df_iter)

    df_next.tpep_pickup_datetime = pd.to_datetime(df_next.tpep_pickup_datetime)
    df_next.tpep_dropoff_datetime = pd.to_datetime(df_next.tpep_dropoff_datetime)
    df_next.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df_next.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()
        df_next=next(df_iter)

        df_next.tpep_pickup_datetime = pd.to_datetime(df_next.tpep_pickup_datetime)
        df_next.tpep_dropoff_datetime = pd.to_datetime(df_next.tpep_dropoff_datetime)

        df_next.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))


if __name__ == '__main__':

    #parse input from command line
    parser = argparse.ArgumentParser(description='Ingest Parquet/CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='passowrd  for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table were we will write the results to')
    parser.add_argument('--url', help='URL of the Parquet/CSV file to download')

    args = parser.parse_args()

    main(args)