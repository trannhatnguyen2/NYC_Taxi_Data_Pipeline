from minio import Minio
from helpers import load_cfg

CFG_FILE = "./config/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

minio_client = Minio(
    endpoint=datalake_cfg["endpoint"],
    access_key=datalake_cfg["access_key"],
    secret_key=datalake_cfg["secret_key"],
    secure=False,
)

def list_parquet_files(bucket_name, prefix=""):
    """
        Function to list all Parquet files in a bucket (MinIO)
    """
    try:
        # List all objects in the bucket with the given prefix
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        # Filter and collect Parquet file names
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        
        return parquet_files
    except Exception as err:
        print(f"Error: {err}")
        return []

# bucket_name = datalake_cfg["bucket_name"]
# parquet_files = list_parquet_files(bucket_name)

# print("Parquet files in the bucket:")
# for file_name in parquet_files:
#     print(file_name)
