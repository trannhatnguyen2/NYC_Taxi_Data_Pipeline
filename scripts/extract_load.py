import sys
import os
from glob import glob
from minio import Minio

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"
YEARS = ["2020", "2021", "2022", "2023"]
###############################################


###############################################
# Main
###############################################
def extract_load(endpoint_url, access_key, secret_key):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    client.create_bucket(datalake_cfg["bucket_name_1"])

    for year in YEARS:
        # Upload files
        all_fps = glob(os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet"))

        for fp in all_fps:
            print(f"Uploading {fp}")
            client_minio = client.create_conn()
            client_minio.fput_object(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name=os.path.join(datalake_cfg["folder_name"], os.path.basename(fp)),
                file_path=fp,
            )
###############################################


if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    ENDPOINT_URL_LOCAL = datalake_cfg['endpoint']
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']

    extract_load(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)