import requests
from pprint import pprint
from datetime import datetime
from decouple import config

class ForecastWeatherAPI:
    """
    A class dedicated to retrieve forecast weather data (5 day forecast, every 3 hours) from Open Weather Map API.
    """

    def __init__(self):
        """
        Initialization get the API key from the .env.
        """
        self.key = config("API_WEATHER_KEY")
    
    def get(self, coord: dict) -> dict:
        """
        Method to retrieve the data from the API.
        Args:
            coord (dict): coordinates of the requested weather forecast following the format {"lat": x, "lon": y} 
        Return:
            Extensive data from the API in a json format. 
        """
        self.coord = coord
        r = requests.get(url="https://api.openweathermap.org/data/2.5/forecast?lat={}&lon={}&appid={}"
                         .format(self.coord["lat"], self.coord["lon"], self.key ))
        self.data_api = r.json()
        return self.data_api
    
    def transform(data: dict) -> dict:
        """
        Method to select the data needed (wind stats) and add additional information (retrieve date).
        Return:
            Extensive data from the API in a json format. 
        """
        data_list = []
        data_clean = {}
        
        for dict in data["list"]:
            for key, value in dict.items():
                data_temp = {}
                if key == "wind" or key == "dt_txt":
                    data_temp[key] = value
                else:
                    continue
                data_list.append(data_temp)
        data_clean["extract_date"] = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        data_clean["weather"] = data_list
        data_clean["coordinates"] = data["city"]["coord"]
        return data_clean

# Check

coordinate = {"lat": 49.7836416, "lon": 2.7646835}
#coordinate = {"lat": "a", "lon": 2.7646835}
weather_data = ForecastWeatherAPI().get(coordinate)
pprint(ForecastWeatherAPI.transform(weather_data))