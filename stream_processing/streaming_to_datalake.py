import sys
import os
import warnings
import traceback
import logging
import dotenv
from time import sleep
dotenv.load_dotenv(".env")

from pyspark import SparkConf, SparkContext

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
        spark = (SparkSession.builder.config("spark.executor.memory", "4g") \
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
    from pyspark.sql.types import IntegerType, StringType, StructType, StructField, TimestampNTZType, DoubleType, LongType
    from pyspark.sql.functions import col, from_json, udf

    payload_after_schema = StructType([
            StructField("dolocationid", IntegerType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("ratecodeid", DoubleType(), True),
            StructField("vendorid", IntegerType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("tpep_dropoff_datetime", LongType(), True),
            StructField("tpep_pickup_datetime", LongType(), True),
            StructField("trip_distance", DoubleType(), True)
    ])

    data_schema = StructType([
        StructField("payload", StructType([
            StructField("after", payload_after_schema, True)
        ]), True)
    ])

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