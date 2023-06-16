import sys
import os

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from classes.ForecastWeatherAPI import ForecastWeatherAPI
from db.create_ref_mongodb import mongodb_connection
from db.create_ref_mariadb import mariadb_connection
import pandas as pd

client = mongodb_connection()
eng = mariadb_connection()

def add_forecast_to_mongodb(url: str, df: pd.DataFrame):
    api = ForecastWeatherAPI(url)
    data = api.get_data(df)
    client["forecast"].insert_one(data)

if __name__ == "__main__":

    url_forecast = "https://api.openweathermap.org/data/2.5/forecast"
    sql_df = pd.read_sql(
    "SELECT * FROM windfarms",
    con=eng
    )
    
    add_forecast_to_mongodb(url_forecast, sql_df)
    print("Forecast data imported.")