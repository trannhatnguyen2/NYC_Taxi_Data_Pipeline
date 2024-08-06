import requests
import os


yearmonth = "2018-01"
#Create a  list for range yearmonth format from 2018-01 to 2024-04 
date_range = [f"{year}-{month:02d}" for year in range(2018, 2025) for month in range(1, 13)]

for yearmonth in date_range:
    url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{yearmonth}.parquet"
    query_parameters = {"downloadformat": "parquet"}
    response = requests.get(url, params=query_parameters)
    print(response.status_code)
    file_path=f"data/green_tripdata_yellow_tripdata_{yearmonth}.parquet"
    try:
        if os.path.exists(file_path):
            os.chmod(file_path, 0o666)
            print("File permissions modified successfully!")
        else:
            print("File not found:", file_path)
    except PermissionError:
        print("Permission denied: You don't have the necessary permissions to change the permissions of this file.")

    with open(file_path, mode="wb") as file:
        file.write(response.content)
        print(f"File {file_path} downloaded successfully!")


