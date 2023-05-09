from pymongo import MongoClient
import api_weather

# Base de données MongoDB

def database_mongodb_creation():
    client = MongoClient(host="localhost", port=27017)
    database = client["database"]
    weather = database.create_collection(name="weather")

def add_data(client, coordinates):
    for coordinate in coordinates:
        data = extract_weather_data(coordinate)
        weather.insert_one(data)

coord = [{"LAT": 49.7836416, "LON": 2.7646835}, {"LAT": 49.7836416, "LON": 2.7646835}]


#add_data(client, coord)