import pymongo
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os
from dotenv import load_dotenv
from sqlalchemy import text
import mysql.connector
from mysql.connector import Error

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

# Récupérer les informations de connexion à partir des variables d'environnement
mongodb_username = os.getenv("MONGO_ROOT_USERNAME")
mongodb_password = os.getenv("MONGO_ROOT_PASSWORD")
mongodb_database = os.getenv("MONGO_DATABASE")

mariadb_user = os.getenv("MARIADB_USER")
mariadb_root_password = os.getenv("MARIADB_ROOT_PASSWORD")
mariadb_database = os.getenv("MARIADB_DATABASE")
mariadb_password = os.getenv("MARIADB_PASSWORD")

Base = declarative_base()


class ScadaDataProcessor:
    def __init__(self, mongodb_username, mongodb_password, mongodb_database, mariadb_user, mariadb_root_password,
                 mariadb_database, mariadb_password):
        # Connexion à MongoDB
        mongodb_url = f"mongodb://{mongodb_username}:{mongodb_password}@localhost:27017/{mongodb_database}"
        self.mongo_client = pymongo.MongoClient(mongodb_url)
        self.mongo_db = self.mongo_client[mongodb_database]
        self.mongo_collection = self.mongo_db['scada_data']

        # Connexion à MariaDB
        try:
            self.mariadb_connection = mysql.connector.connect(
                host='mariadb',
                port=3306,
                database=mariadb_database,
                user=mariadb_user,
                password=mariadb_password
            )
            if self.mariadb_connection.is_connected():
                db_info = self.mariadb_connection.get_server_info()
                print("Connected to MariaDB Server version:", db_info)
                self.mariadb_cursor = self.mariadb_connection.cursor()

        except Error as e:
            print("Error while connecting to MariaDB:", e)

    def process_scada_data(self):
        # Récupérer les identifiants de turbines depuis la table windturbine dans MariaDB
        turbine_ids = self.get_turbine_ids()

        # Traiter les données SCADA pour chaque turbine
        total_processed = self.process_turbines(turbine_ids)

        return total_processed

    def process_turbines(self, turbine_ids):
        total_processed = 0

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for turbine_id in turbine_ids:
                future = executor.submit(self.fetch_and_process_scada_data, turbine_id)
                futures.append(future)

            for future in as_completed(futures):
                total_processed += future.result()

        return total_processed

    def fetch_and_process_scada_data(self, turbine_id):
        url = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-scada-data"
        last_timestamp = self.get_last_timestamp(turbine_id)
        start_date = last_timestamp.strftime("%Y-%m-%d %H:%M:%S") if last_timestamp else None
        end_date = "2023-05-28 00:00:00"

        payload = {
            "windturbine_id": turbine_id,
            "start_date": start_date,
            "end_date": end_date
        }

        response = requests.post(url, json=payload)
        scada_data = response.json()

        new_data = []
        for data in scada_data:
            if not self.is_data_present(turbine_id, data['timestamp']):
                new_data.append(data)

        # Stocker les nouvelles données dans MongoDB
        self.mongo_collection.insert_many(new_data)

        # Mettre à jour le référentiel MariaDB avec les nouvelles données
        self.update_mariadb_referential(new_data)

        return len(new_data)

    def is_data_present(self, turbine_id, timestamp):
        query = f"SELECT * FROM scadadata WHERE turbine_id = {turbine_id} AND timestamp = '{timestamp}'"
        self.mariadb_cursor.execute(query)
        result = self.mariadb_cursor.fetchone()
        return result is not None

    def get_last_timestamp(self, turbine_id):
        query = f"SELECT timestamp FROM scadadata WHERE turbine_id = {turbine_id} ORDER BY timestamp DESC LIMIT 1"
        self.mariadb_cursor.execute(query)
        result = self.mariadb_cursor.fetchone()
        if result:
            return result[0]
        return None

    def update_mariadb_referential(self, scada_data):
        for data in scada_data:
            query = f"INSERT INTO scadadata (turbine_id, scada_value, timestamp) VALUES ({data['windturbine_id']}, " \
                    f"{data['scada_value']}, '{data['timestamp']}')"
            self.mariadb_cursor.execute(query)

        self.mariadb_connection.commit()

    def get_turbine_ids(self):
        query = "SELECT id FROM windturbine"
        self.mariadb_cursor.execute(query)
        turbine_ids = [row[0] for row in self.mariadb_cursor.fetchall()]
        return turbine_ids


# Création d'une instance de ScadaDataProcessor
scada_processor = ScadaDataProcessor(mongodb_username, mongodb_password, mongodb_database, mariadb_user,
                                     mariadb_root_password, mariadb_database, mariadb_password)

# Traitement des données SCADA
total_processed = scada_processor.process_scada_data()
print(f"Total processed: {total_processed}")
