import sys
import os
import warnings
import traceback
import logging
import time
from minio import Minio

sys.path.append("/opt/airflow/dags/scripts/")
from helpers import load_cfg

from pyspark import SparkConf, SparkContext

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
BASE_PATH = "/opt/airflow/"

CFG_FILE = BASE_PATH + "config/datalake_airflow.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

# Create a client with the MinIO server
minio_client = Minio(
    endpoint=datalake_cfg["endpoint"],
    access_key=datalake_cfg["access_key"],
    secret_key=datalake_cfg["secret_key"],
    secure=False,
)
###############################################


###############################################
# Utils
###############################################
def create_bucket(bucket_name):
    """
        Create bucket if not exist
    """
    try:
        found = minio_client.bucket_exists(bucket_name=bucket_name)
        if not found:
            minio_client.make_bucket(bucket_name=bucket_name)
        else:
            print(f"Bucket {bucket_name} already exists, skip creating!")
    except Exception as err:
        print(f"Error: {err}")
        return []


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
###############################################


###############################################
# PySpark
###############################################
def main_convert():
    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip

    jars = "jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.262.jar"

    builder = SparkSession.builder \
                    .appName("Converting to Delta Lake") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.jars", jars)
        
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()

    sc = spark.sparkContext

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_access_key")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_secret_key")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    logging.info('Spark session successfully created!')

    # Create bucket 'delta'
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
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
