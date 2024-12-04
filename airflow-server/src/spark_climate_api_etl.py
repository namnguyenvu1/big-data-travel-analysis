import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pandas as pd
import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()


def fetch_weather_data():

    # obtain access key
    service_account_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    spark = SparkSession.builder \
    .appName('climate_analysis') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

    # bind key
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", service_account_key)


    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 51.0501,
        "longitude": -114.0853,
        "hourly": "temperature_2m"
    }
    response = requests.get(url, params=params) # make request to meteo api
    data = response.json()

    # parse data from json and convert to dataframe
    hourly_data = data.get('hourly') 
    time = hourly_data.get('time')
    temp = hourly_data.get('temperature_2m')

    rows = [{
        'time': x, 
        'temp': y
        } for x, y in zip(time, temp)]

    spark_df = spark.createDataFrame(rows) # create df
    # spark_df.show()
    # spark_df.write.mode("append").csv("gs://climate_analysis_bucket/weather_data") # write dataframe to gcp data lake

fetch_weather_data()