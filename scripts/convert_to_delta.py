import sys
import os
import warnings
import traceback
import logging
import time
from minio import Minio

from pyspark import SparkConf, SparkContext

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']
BUCKET_NAME_3 = datalake_cfg['bucket_name_3']
###############################################


###############################################
# PySpark
###############################################
def delta_convert(endpoint_url, access_key, secret_key):
    """
        Convert parquet file to delta format
    """
    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip

    jars = "jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.262.jar"

    builder = SparkSession.builder \
                    .appName("Converting to Delta Lake") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.hadoop.fs.s3a.access.key", access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
                    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                    .config("spark.jars", jars)
        
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
    logging.info('Spark session successfully created!')


    # Create bucket 'delta'
    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )
    client.create_bucket(BUCKET_NAME_3)

    # Convert to delta
    for file in client.list_parquet_files(BUCKET_NAME_2, prefix=datalake_cfg['folder_name']):
        path_read = f"s3a://{BUCKET_NAME_2}/" + file
        logging.info(f"Reading parquet file: {file}")

        df = spark.read.parquet(path_read)

        # Save to bucket 'delta' 
        path_read = f"s3a://{BUCKET_NAME_3}/" + file
        logging.info(f"Saving delta file: {file}")

        df_delta = df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(f"s3a://{BUCKET_NAME_3}/{datalake_cfg['folder_name']}")
        
        logging.info("="*50 + "COMPLETELY" + "="*50)
###############################################


###############################################
# Main
###############################################
if __name__ == "__main__":
    delta_convert(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
###############################################
