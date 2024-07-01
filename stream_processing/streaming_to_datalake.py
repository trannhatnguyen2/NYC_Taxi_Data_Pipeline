import sys
import os
import warnings
import traceback
import logging
import dotenv
import json
from time import sleep
dotenv.load_dotenv(".env")

from pyspark import SparkConf, SparkContext

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
            
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

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
                            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.hadoop:hadoop-aws:2.8.2"
                        )
                        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                        .config("spark.hadoop.fs.s3a.path.style.access", "true")
                        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                        .appName("Streaming Processing Application")
                        .getOrCreate()
        )
        
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def create_initial_dataframe(spark_session):
    """
        Reads the streaming data and creates the initial dataframe accordingly
    """
    try: 
        df = (spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "device.iot.taxi_nyc_time_series")
            # .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load())
        logging.info("Initial dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Initial dataframe could not be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    """
        Modifies the initial dataframe, and creates the final dataframe
    """
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampNTZType, DoubleType, LongType
    from pyspark.sql.functions import col, from_json, udf

    # Load the configuration file
    with open('./stream_processing/schema_config.json', 'r') as f:
        config = json.load(f)

    # Define a mapping from type names to PySpark types
    type_mapping = {
        "IntegerType": IntegerType(),
        "StringType": StringType(),
        "TimestampNTZType": TimestampNTZType(),
        "DoubleType": DoubleType(),
        "LongType": LongType()
    }

    # Create the schema based on the configuration file
    payload_after_schema = StructType([
        StructField(field["name"], type_mapping[field["type"]], field["nullable"])
        for field in config["fields"]
    ])

    data_schema = StructType([
        StructField("payload", StructType([
            StructField("after", payload_after_schema, True)
        ]), True)
    ])


    # Explain:
    #  1. Converts the value column of df to a STRING type and names the new column 'json'
    #  2. Converts the new 'json' column into JSON format based on the schema 'data_schema' with the alias 'data'.
    #  3. Based on the Debezium JSON, only extracts the data from "payload.after.*"
    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), data_schema).alias("data")) \
                .select("data.payload.after.*")

    parsed_df = parsed_df \
        .withColumn("tpep_pickup_datetime", (col("tpep_pickup_datetime") / 1000000).cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", (col("tpep_dropoff_datetime") / 1000000).cast("timestamp"))

    parsed_df.createOrReplaceTempView("nyc_taxi_view")

    df_final = spark.sql("""
        SELECT
            * 
        FROM nyc_taxi_view
    """)

    logging.info("Final dataframe created successfully!")
    return df_final


def start_streaming(df):
    """
    Store data into Datalake (MinIO) with parquet format
    """
    logging.info("Streaming is being started...")
    stream_query = df.writeStream \
                        .format("parquet") \
                        .outputMode("append") \
                        .option("path", f"s3a://{BUCKET_NAME}/stream/") \
                        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/stream/checkpoint") \
                        .start() 
    return stream_query.awaitTermination()
###############################################


###############################################
# Main
###############################################
if __name__ == '__main__':
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)
###############################################