import sys
import os
import warnings
import traceback
import logging
import time
from minio import Minio

sys.path.append("/opt/airflow/dags/scripts/")
from helpers import load_cfg
from minio_utils import create_bucket, connect_minio, list_parquet_files

from pyspark import SparkConf, SparkContext

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
BASE_PATH = "/opt/airflow/"

CFG_FILE = BASE_PATH + "config/datalake_airflow.yaml"
###############################################


###############################################
# PySpark
###############################################
def main_convert():
    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip

    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    jars = "jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.262.jar"

    builder = SparkSession.builder \
                    .appName("Converting to Delta Lake") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.hadoop.fs.s3a.access.key", datalake_cfg["access_key"]) \
                    .config("spark.hadoop.fs.s3a.secret.key", datalake_cfg["secret_key"]) \
                    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                    .config("spark.jars", jars)
        
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
    logging.info('Spark session successfully created!')

    # Create bucket 'delta'
    create_bucket(datalake_cfg['bucket_name_3'])

    for file in list_parquet_files(datalake_cfg['bucket_name_2'], prefix=datalake_cfg['folder_name']):
        print(file)

        path_read = f"s3a://{datalake_cfg['bucket_name_2']}/" + file
        logging.info(f"Reading parquet file: {file}")

        df = spark.read.parquet(path_read)

        print(df.count())

        # Save to bucket 'delta' 
        path_read = f"s3a://{datalake_cfg['bucket_name_3']}/" + file
        logging.info(f"Saving delta file: {file}")

        df_delta = df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(f"s3a://{datalake_cfg['bucket_name_3']}/batch")
        
        logging.info("="*50 + "COMPLETELY" + "="*50)
###############################################
