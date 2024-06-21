import os
import argparse
import pandas as pd
import json
import logging
from time import sleep
from bson import json_util

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default="localhost:9092",
    help="Where the bootstrap server is",
)

args = parser.parse_args()

NUM_DEVICES = 1


def create_topic(admin, topic_name):
    """
        Create topic if not exists
    """
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topic([topic])
        logging.info(f"A new topic {topic_name} has been created!")
    except Exception:
        logging.info(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def create_streams(servers):
    """
        Create streaming data to Kafka Topic
    """

    producer = None
    admin = None
    for _ in range (10): 
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            break
        except Exception as e:
            logging.info(f"Trying to instantiate admin and producer with bosootstrap server {servers} with error {e}")
            sleep(10)
            pass

    # send data
    df = pd.read_parquet("data/2024/yellow_tripdata_2024-01.parquet")
    
    for index, row in df.iterrows():
        topic_name = "nyc_taxi_device"
        create_topic(admin, topic_name=topic_name)
        
        producer.send(
            topic_name, json.dumps(format_record(row), default=json_util.default).encode("utf-8")
        )
        print(f"Sent: {format_record(row)}")
        sleep(2)


def format_record(row):
    taxi_res = {
        'dolocationid': row['dolocationid'],
        'pulocationid': row['pulocationid'],
        'ratecodeid': row['ratecodeid'],
        'vendorid': row['vendorid'],
        'congestion_surcharge': row['congestion_surcharge'],
        'extra': row['extra'],
        'fare_amount': row['fare_amount'],
        'improvement_surcharge': row['improvement_surcharge'],
        'mta_tax': row['mta_tax'],
        'passenger_count': row['passenger_count'],
        'payment_type': row['payment_type'],
        'tip_amount': row['tip_amount'],
        'tolls_amount': row['tolls_amount'],
        'total_amount': row['total_amount'],
        'dropoff_datetime': str(row['dropoff_datetime']),
        'pickup_datetime': str(row['pickup_datetime']),
        'trip_distance': row['trip_distance']
    }
    return taxi_res


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]

    # Tear down all previous streams
    print("Tearing down all existing topics!")
    for _ in range(NUM_DEVICES):
        try:
            teardown_stream(f"nyc_taxi_device", [servers])
        except Exception as e:
            print(f"Topic nyc_taxi_device does not exist. Skipping...!")

    if mode == "setup":
        create_streams([servers])