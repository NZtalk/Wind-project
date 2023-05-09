# données méteorologiques extraites a l'instanté
# essaie d'initialisation sur flask , on pourra remplacer par fast api
# Il sera important de valoriser ce code par une boucle qui viendra avec le travail de Louis , executer une boucle mais sur les donéées météorologiques prévisionnel
#il sera important de revaluer la boucle pour l'adapter aux données metéorologiques previsionnel et les stocker dans une base de donnée de la collection toute les 24h et qu'elle s'actualise automatiquement 

import requests
import os
from datetime import datetime
from pymongo import MongoClient
from time import sleep
from flask import Flask, jsonify
from dotenv import load_dotenv

# initialiser l'application Flask
app = Flask(__name__)

# charger les variables d'environnement depuis le fichier .env
load_dotenv()

# configurer la clé API
API_KEY = os.getenv("API_KEY")

# configurer la base de données MongoDB
client = MongoClient(host="localhost", port=27017)
database = client["database"]
weather = database.create_collection(name="weather9")

# créer la fonction pour extraire et enregistrer les données météorologiques
def extract_and_save_weather_data(lat= 49.7836416, lon= 2.7646835):
    while True:
        data_temp = extract_weather_data(lat, lon)
        weather.insert_one(data_temp)
        print(data_temp)
        # test d'initialisation , les données sont bien extraite toute les 6 secondes a réevaluer selon le besoin on pourra egalement prendre la même logique pour le code de Louis , elle s'inscrit parfaitement dans la base de donnée nosql(MongoDB)
        sleep(6)

# créer la fonction pour extraire les données météorologiques
def extract_weather_data(lat, lon):
    r = requests.get(
    url="https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}".format(
        lat, lon, API_KEY))

    data_api = r.json()
    data_temp = {}

    data_temp["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_temp["wind_speed"] = data_api["wind"]["speed"]
    data_temp["wind_deg"] = data_api["wind"]["deg"]
    data_temp["humidity"] = data_api["main"]["humidity"]
    data_temp["temperature"] = data_api["main"]["temp"]
    data_temp["pressure"] = data_api["main"]["pressure"]
    data_temp["description"] = data_api["weather"][0]["description"]
    data_temp["icon"] = data_api["weather"][0]["icon"]

    return data_temp

# exécuter la fonction pour extraire et enregistrer les données météorologiques
extract_and_save_weather_data()
