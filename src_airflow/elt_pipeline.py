import sys
import os
import pandas as pd
from glob import glob
from minio import Minio
import time

sys.path.append("/opt/airflow/dags/scripts/")
from helpers import load_cfg
from minio_utils import create_bucket, connect_minio

###############################################
# Parameters & Arguments
###############################################
BASE_PATH = "/opt/airflow/"

CFG_FILE = BASE_PATH + "config/datalake_airflow.yaml"
DATA_PATH = BASE_PATH + "data/"
TAXI_LOOKUP_PATH = BASE_PATH + "dags/scripts/data/taxi_lookup.csv"
YEARS = ["2022"]
###############################################


###############################################
# Extract and Load
###############################################
def extract_load_to_datalake():
    """
        Extract data file and Load to Datalake (MinIO) at bucket 'datalake'
    """
    # Load minio config
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    # Create a client with the MinIO server
    minio_client = connect_minio()
    create_bucket(datalake_cfg["bucket_name_1"])

    for year in YEARS:
        # Upload files
        all_fps = glob(os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet"))

        for fp in all_fps:
            print(f"Uploading {fp}")
            minio_client.fput_object(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name=os.path.join(datalake_cfg["folder_name"], os.path.basename(fp)),
                file_path=fp,
            )
###############################################


###############################################
# Transform
###############################################
def drop_column(df, file):
    """
        Drop columns 'store_and_fwd_flag'
    """
    if "store_and_fwd_flag" in df.columns:
        df = df.drop(columns=["store_and_fwd_flag"])
        print("Dropped column store_and_fwd_flag from file: " + file)
    else:
        print("Column store_and_fwd_flag not found in file: " + file)

    return df


def merge_taxi_zone(df, file):
    """
        Merge dataset with taxi zone lookup
    """
    df_lookup = pd.read_csv(TAXI_LOOKUP_PATH)

    def merge_and_rename(df, location_id, lat_col, long_col):
        df = df.merge(df_lookup, left_on=location_id, right_on="LocationID")
        df = df.drop(columns=["LocationID", "Borough", "service_zone", "zone"])
        df = df.rename(columns={
            "latitude" : lat_col,
            "longitude" : long_col
        })
        return df

    if "pickup_latitude" not in df.columns:
        df = merge_and_rename(df, "pulocationid", "pickup_latitude", "pickup_longitude")
        
    if "dropoff_latitude" not in df.columns:
        df = merge_and_rename(df, "dolocationid", "dropoff_latitude", "dropoff_longitude")

    df = df.drop(columns=[col for col in df.columns if "Unnamed" in col], errors='ignore').dropna()

    print("Merged data from file: " + file)

    return df


def process(df, file):
    """
    Green:
        Rename column: lpep_pickup_datetime, lpep_dropoff_datetime, ehail_fee
        Drop: trip_type
    Yellow:
        Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
    """
    
    if file.startswith("green"):
        # rename columns
        df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "ehail_fee": "fee"
            },
            inplace=True
        )

        # drop column
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)

    elif file.startswith("yellow"):
        # rename columns
        df.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "airport_fee": "fee"
            },
            inplace=True
        )

    # fix data type in columns 'payment_type', 'dolocationid', 'pulocationid', 'vendorid'
    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].astype(int)
    if "dolocationid" in df.columns:
        df["dolocationid"] = df["dolocationid"].astype(int)
    if "pulocationid" in df.columns:
        df["pulocationid"] = df["pulocationid"].astype(int)
    if "vendorid" in df.columns:
        df["vendorid"] = df["vendorid"].astype(int)

    # drop column 'fee'
    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)
                
    # Remove missing data
    df = df.dropna()
    df = df.reindex(sorted(df.columns), axis=1)
    
    print("Transformed data from file: " + file)

    return df


def transform_data():
    """
        Transform data after loading into Datalake (MinIO)
    """
    import s3fs

    # Load minio config
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    s3_fs = s3fs.S3FileSystem(
        anon=False,
        key=datalake_cfg["access_key"],
        secret=datalake_cfg["secret_key"],
        client_kwargs={'endpoint_url': 'http://minio:9000'}
    )

    # Create bucket 'processed'
    create_bucket(datalake_cfg['bucket_name_2'])

    # Transform data
    for year in YEARS:
        all_fps = glob(os.path.join(BASE_PATH, nyc_data_cfg["folder_path"], year, "*.parquet"))

        for file in all_fps:
            file_name = file.split('/')[-1]
            print(f"Reading parquet file: {file_name}")
            
            df = pd.read_parquet(file, engine='pyarrow')

            # lower case all columns
            df.columns = map(str.lower, df.columns)

            df = drop_column(df, file_name)
            df = merge_taxi_zone(df, file_name)
            df = process(df, file_name)

            # save to parquet file
            path = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}/" + file_name
            df.to_parquet(path, index=False, filesystem=s3_fs, engine='pyarrow')
            print("Finished transforming data in file: " + path)
            print("="*100)
            time.sleep(5)
###############################################
