import os
from minio import Minio
from helpers import load_cfg

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"
###############################################

def connect_minio():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    # Create a client with the MinIO server
    client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )
    return client


def create_bucket(bucket_name):
    client = connect_minio()

    # Create bucket if not exist
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)
        print(f"Bucket {bucket_name} created successfully!")
    else:
        print(f"Bucket {bucket_name} already exists, skip creating!")


def list_parquet_files(bucket_name, prefix=""):
    client = connect_minio()

    # List all objects in the bucket with the given prefix
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    # Filter and collect Parquet file names
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        
    return parquet_files