import sys
import os
import pandas as pd
from glob import glob
from minio import Minio
import time

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

###############################################
# Parameters & Arguments
###############################################
DATA_PATH = "data/"
YEARS = ["2022"]
TAXI_LOOKUP_PATH = os.path.join(os.path.dirname(__file__), "data", "taxi_lookup.csv")
CFG_FILE =  "config/datalake.yaml"
###############################################


###############################################
# Process data
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


def transform_data(endpoint_url, access_key, secret_key):
    """
        Transform data after loading into Datalake (MinIO)
    """
    import s3fs

    # Load minio config
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    s3_fs = s3fs.S3FileSystem(
        anon=False,
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': "".join(["http://", endpoint_url])}
    )

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    # Create bucket 'processed'
    client.create_bucket(datalake_cfg['bucket_name_2'])

    # Transform data
    for year in YEARS:
        all_fps = glob(os.path.join(DATA_PATH, year, "*.parquet"))

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
###############################################


###############################################
# Process data
###############################################
if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    ENDPOINT_URL_LOCAL = datalake_cfg['endpoint']
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']

    transform_data(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)
###############################################