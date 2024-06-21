import sys
import os
import warnings
import traceback
import logging
import time
from minio import Minio

from pyspark import SparkConf, SparkContext
from helpers import load_cfg

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "config/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']
BUCKET_NAME_3 = datalake_cfg['bucket_name_3']

# Create a client with the MinIO server
minio_client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
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

    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
    sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    logging.info('Spark session successfully created!')

    # Create bucket 'delta'
    create_bucket(BUCKET_NAME_3)

    # Convert to delta
    for file in list_parquet_files(BUCKET_NAME_2, prefix=datalake_cfg['folder_name']):
        print(file)

        path_read = f"s3a://{BUCKET_NAME_2}/" + file
        logging.info(f"Reading parquet file: {file}")

        df = spark.read.parquet(path_read)

        print(df.count())

        # Save to bucket 'delta' 
        path_read = f"s3a://{BUCKET_NAME_3}/" + file
        logging.info(f"Saving delta file: {file}")

        df_delta = df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(f"s3a://{BUCKET_NAME_3}/batch")
        
        logging.info("="*50 + "COMPLETELY" + "="*50)
###############################################


###############################################
# Main
###############################################
if __name__ == "__main__":
    main_convert()
###############################################
