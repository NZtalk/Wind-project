import requests
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv() 

class ForecastWeatherAPI:
    """
    A class dedicated to retrieve weather data from Open Weather Map API.
    """

    def __init__(self, url):
        """
        Initialization get the API key from the .env. and the url of the API.
        """
        self.key = os.environ["API_WEATHER_KEY"]
        self.url = url
    
    def get_data(self, df: pd.DataFrame) -> dict:
        """
        Method to retrieve the data from the API.
        Args:
            df (DataFrame): df with 3 columns: windfarm_id, lat and long.
        Return:
            Extensive data from the API in a json format. 
        """
        list_responses = []
        dict = {}
        #Iterating on winfarm_id
        for index, row in df.iterrows():
            windfarm_id = row["windfarm_id"]
            lat = row["latitude"]
            lon = row["longitude"]
            response = requests.get("{}?lat={}&lon={}&appid={}"
                                    .format(self.url, lat, lon, self.key ))
            data = response.json()
            # Converting text to datetime format
            if self.url[-8:] == "forecast": 
                for elem in data["list"]:
                    elem["dt_txt"] = datetime.strptime(elem["dt_txt"],"%Y-%m-%d %H:%M:%S")
            else :
                data["dt"] = datetime.fromtimestamp(data["dt"])
            data["windfarm_id"] = windfarm_id
            list_responses.append(data)
        dict["extract_date"] = datetime.now()
        dict["data"]= list_responses
        return dict
    
