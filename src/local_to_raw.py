import sys
import os
from glob import glob
from minio import Minio

sys.path.append("./utils/")
from helpers import load_cfg
from minio_utils import create_bucket, connect_minio

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"
YEARS = ["2020", "2021", "2022", "2023"]
###############################################


###############################################
# Main
###############################################
def main():
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = connect_minio()
    create_bucket(datalake_cfg["bucket_name_1"])

    for year in YEARS:
        # Upload files
        all_fps = glob(os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet"))

        for fp in all_fps:
            print(f"Uploading {fp}")
            client.fput_object(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name=os.path.join(datalake_cfg["folder_name"], os.path.basename(fp)),
                file_path=fp,
            )
###############################################


if __name__ == "__main__":
    main()