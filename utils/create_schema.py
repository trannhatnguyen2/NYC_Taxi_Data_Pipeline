import os
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient
load_dotenv(".env")


def main():

    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    create_iot_schema = """CREATE SCHEMA IF NOT EXISTS iot;"""

    create_staging_schema = """CREATE SCHEMA IF NOT EXISTS staging;"""

    create_production_schema = """CREATE SCHEMA IF NOT EXISTS production;"""

    try:
        pc.execute_query(create_iot_schema)
        pc.execute_query(create_staging_schema)
        pc.execute_query(create_production_schema)
    except Exception as e:
        print(f"Failed to create schema with error: {e}")


if __name__ == "__main__":
    main()