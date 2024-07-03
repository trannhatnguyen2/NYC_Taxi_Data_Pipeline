import os
from minio import Minio

class MinIOClient:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key

    def create_conn(self):
        client = Minio(
            endpoint=self.endpoint_url,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )
        return client

    def create_bucket(self, bucket_name):
        client = self.create_conn()

        # Create bucket if not exist
        found = client.bucket_exists(bucket_name=bucket_name)
        if not found:
            client.make_bucket(bucket_name=bucket_name)
            print(f"Bucket {bucket_name} created successfully!")
        else:
            print(f"Bucket {bucket_name} already exists, skip creating!")

    def list_parquet_files(self, bucket_name, prefix=""):
        client = self.create_conn()

        # List all objects in the bucket with the given prefix
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        # Filter and collect Parquet file names
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
            
        return parquet_files