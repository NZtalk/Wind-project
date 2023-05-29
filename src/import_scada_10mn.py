import pymongo
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from sqlalchemy import create_engine, Column, Integer, String, DateTime, select, update, bindparam
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
import os
from dotenv import load_dotenv
from sqlalchemy import text
import mysql.connector
from mysql.connector import Error
from classes.WindAPI import *
from datetime import datetime, timedelta
from create_ref_mongodb import mongodb_connection
from create_ref_mariadb import mariadb_connection, windturbines
import pytz
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

client = mongodb_connection()
eng = mariadb_connection()

def process_scada_data():
    # Récupérer les identifiants et last scada update de turbines depuis la table windturbine dans MariaDB
    stmt = select(windturbines.c['windturbine_id','last_scada_update'])
    turbines = []
    with Session(eng) as session:
        for row in session.execute(stmt):
            turbines.append(row)
    # Traiter les données SCADA pour chaque turbine
    return process_turbines(turbines)


def process_turbines(turbines):
    
    # Payload for API Scada multi-thread call
    payload = []
    
    d30_datetime = datetime.now() - timedelta(days=30)
    current_datetime = datetime.now()
    
    for turbine in turbines:
        last_scada_update = turbine[1]
        # Default start date to D-30
        if (last_scada_update == None):
            last_scada_update = d30_datetime
            
        payload.append({
            'windturbine_id': turbine[0],
            'start_date': last_scada_update.strftime("%Y-%m-%d %H:%M:%S"),
            'end_date': current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        })
    
    scada_api = WindAPI("https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-scada-data")
    df_scada = scada_api.multithread_get(payload)
    
    if (df_scada.empty == False):
        # Insert rows in MongoDB collection
        client["scada"].insert_many(df_scada.to_dict('records'))
        for turbine in turbines:
            windturbine_id = turbine[0]
            last_windturbine_log_date = df_scada[df_scada['wind_turbine'] == windturbine_id]['log_date'].max()
            if (isinstance(last_windturbine_log_date, str)):
                # Update Windturbine last scada update records
                last_windturbine_log_datetime = (datetime.
                                                strptime(last_windturbine_log_date[:-3], "%Y-%m-%d %H:%M:%S").
                                                astimezone(pytz.timezone('Europe/Paris')).strftime('%Y-%m-%d %H:%M:%S'))
                stmt = (update(windturbines).
                        where(windturbines.c.windturbine_id == windturbine_id).
                        values(last_scada_update=last_windturbine_log_datetime))
                with Session(eng) as session:
                    session.execute(stmt)
                    session.commit()
        
    return len(df_scada.index)


# Traitement des données SCADA
total_processed = process_scada_data()
print(f"Total data SCADA processed: {total_processed}")