import os
import dotenv
dotenv.load_dotenv(".env")

from pyspark.sql import SparkSession

###############################################
# Parameters & Arguments
###############################################
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
###############################################


spark = (SparkSession.builder.config("spark.executor.memory", "4g") \
                        .config(
                            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.2"
                        )
                        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                        .config("spark.hadoop.fs.s3a.path.style.access", "true")
                        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                        .appName("Read Parquet file")
                        .getOrCreate()
        )

file_name = 'part-00000-fa5ff503-a19d-4931-bf87-a3495045c243-c000.snappy.parquet'
file_path = f's3a://{BUCKET_NAME}/stream/'

df = spark.read.parquet(file_path)
df.show()
df.printSchema()