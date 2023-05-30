from sqlalchemy import select, update
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from classes.WindAPI import *
from datetime import datetime, timedelta
from create_ref_mongodb import mongodb_connection
from create_ref_mariadb import mariadb_connection, windturbines
import pytz
# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

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
    
    d90_datetime = datetime.now() - timedelta(days=90)
    tz = pytz.timezone('Europe/Paris')
    current_datetime = datetime.now(tz)
    
    # Payload construction for API Request with list comprehension
    payload = [{
        'windturbine_id': t[0], 
        'start_date': d90_datetime.strftime("%Y-%m-%d %H:%M:%S") if t[1] == None else t[1].strftime("%Y-%m-%d %H:%M:%S"), 
        'end_date': current_datetime.strftime("%Y-%m-%d %H:%M:%S") 
    } for t in turbines]
    
    # SCADA API Request with payload
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
                
                last_windturbine_log_datetime = datetime.strptime(last_windturbine_log_date[:-3], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=10)
                
                stmt = (update(windturbines).
                        where(windturbines.c.windturbine_id == windturbine_id).
                        values(last_scada_update=last_windturbine_log_datetime.strftime('%Y-%m-%d %H:%M:%S')))
                with Session(eng) as session:
                    session.execute(stmt)
                    session.commit()
        
    return len(df_scada.index)


# Process SCADA DATA
total_processed = process_scada_data()
print(f"Total data SCADA processed: {total_processed}")