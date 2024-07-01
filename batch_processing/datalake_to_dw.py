import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
dotenv.load_dotenv(".env")

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
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
DB_STAGING_TABLE = os.getenv("DB_STAGING_TABLE")

CFG_FILE = "./config/datalake.yaml"
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME = datalake_cfg['bucket_name_2']

CFG_FILE_SPARK = "./config/spark.yaml"
cfg = load_cfg(CFG_FILE_SPARK)
spark_cfg = cfg["spark_config"]

MEMORY = spark_cfg['executor_memory']
###############################################


###############################################
# PySpark
###############################################
def create_spark_session():
    """
        Create the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    try: 
        spark = (SparkSession.builder.config("spark.executor.memory", MEMORY) \
                        .config(
                            "spark.jars", 
                            "jars/postgresql-42.4.3.jar,jars/aws-java-sdk-bundle-1.12.262.jar,jars/hadoop-aws-3.3.4.jar",
                        )
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                        .appName("Batch Processing Application")
                        .getOrCreate()
        )
        
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def load_minio_config(spark_context: SparkContext):
    """
        Establish the necessary configurations to access to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO config could not be created successfully due to exception: {e}")


def processing_dataframe(df, file_path):
    """
        Process data before loading to staging area
    """
    from pyspark.sql import functions as F 

    df2 = df.withColumn('year', F.year('pickup_datetime')) \
            .withColumn('month', F.date_format('pickup_datetime', 'MMMM')) \
            .withColumn('dow', F.date_format('pickup_datetime', 'EEEE'))

    df_final = df2.groupBy(
        'year',
        'month',
        'dow',
        F.col('vendorid').alias('vendor_id'),
        F.col('ratecodeid').alias('rate_code_id'),
        F.col('pulocationid').alias('pickup_location_id'),
        F.col('dolocationid').alias('dropoff_location_id'),
        F.col('payment_type').alias('payment_type_id'),
        'pickup_datetime',
        'dropoff_datetime',
        'pickup_latitude',
        'pickup_longitude',
        'dropoff_latitude',
        'dropoff_longitude'
        ).agg(
            F.sum('passenger_count').alias('passenger_count'),
            F.sum('trip_distance').alias('trip_distance'),
            F.sum('extra').alias('extra'),
            F.sum('mta_tax').alias('mta_tax'),
            F.sum('fare_amount').alias('fare_amount'),
            F.sum('tip_amount').alias('tip_amount'),
            F.sum('tolls_amount').alias('tolls_amount'),
            F.sum('total_amount').alias('total_amount'),
            F.sum('improvement_surcharge').alias('improvement_surcharge'),
            F.sum('congestion_surcharge').alias('congestion_surcharge'),
        )

    # add 'service_type' column
    if 'yellow' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(1))
    elif 'green' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(2))

    return df_final


def load_to_staging_table(df):
    """
        Save data after processing to Staging Area (PostgreSQL)
    """
    URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # write data to PostgreSQL
    df.write.jdbc(url=URL, table= DB_STAGING_TABLE, mode='append', properties=properties)
    # df.write.jdbc(url=URL, table= 'staging.nyc_taxi_test', mode='append', properties=properties)
###############################################


###############################################
# Main
###############################################
if __name__ == "__main__":
    start_time = time.time()

    spark = create_spark_session()
    load_minio_config(spark.sparkContext)

    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    for file in client.list_parquet_files(BUCKET_NAME, prefix='batch/'):
        path = f"s3a://{BUCKET_NAME}/" + file
        logging.info(f"Reading parquet file: {file}")

        df = spark.read.parquet(path)
        
        df_final = processing_dataframe(df, file)
        
        # load data to staging table in PostgreSQL
        load_to_staging_table(df_final)
        print("="*100)

    logging.info(f"Time to process: {time.time() - start_time}")
    logging.info("Batch processing successfully!")
###############################################
