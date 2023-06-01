from api_weather import ForecastWeatherAPI
from create_ref_mongodb import mongodb_connection
from create_ref_mariadb import mariadb_connection
import pandas as pd

client = mongodb_connection()
eng = mariadb_connection()

def add_forecast_to_mongodb(url: str, df: pd.DataFrame):
    api = ForecastWeatherAPI(url)
    data = api.get_data(df)
    client["forecast"].insert_one(data)

def add_current_to_mongodb(url: str, df: pd.DataFrame):
    api = ForecastWeatherAPI(url)
    data = api.get_data(df)
    client["current"].insert_one(data)

if __name__ == "__main__":

    url_forecast = "https://api.openweathermap.org/data/2.5/forecast"
    url_current = "https://api.openweathermap.org/data/2.5/weather"
    sql_df = pd.read_sql(
    "SELECT * FROM windfarms",
    con=eng
    )
    
    add_forecast_to_mongodb(url_current, sql_df)
    print("Forecast data imported.")

    add_current_to_mongodb(url_current, sql_df)
    print("Current data imported.\nScript over.")