from sqlalchemy import select, update, bindparam
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from classes.WindAPI import *
from datetime import datetime, timedelta
from create_ref_mongodb import mongodb_connection
from create_ref_mariadb import mariadb_connection, windturbines
import pytz
import logging
from pandas import to_datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

def fetch_windturbines_data(engine):
    # Récupérer les identifiants et last scada update de turbines depuis la table windturbine dans MariaDB
    stmt = select(windturbines.c['windturbine_id','last_scada_update'])
    turbines = []
    with Session(engine) as session:
        for row in session.execute(stmt):
            turbines.append(row)

    # Traiter les données SCADA pour chaque turbine
    return turbines

def import_scada_data(turbines, client_mongo):
    # Payload for API Scada multi-thread call
    payload = []

    tz = pytz.timezone('Europe/Paris')
    current_datetime = datetime.now(tz)
    d30_datetime = current_datetime - timedelta(days=30)

    # Payload construction for API Request with list comprehension
    payload = [{
        'windturbine_id': t[0], 
        'start_date': d30_datetime.strftime("%Y-%m-%d %H:%M:%S") if t[1] == None else t[1].strftime("%Y-%m-%d %H:%M:%S"), 
        'end_date': current_datetime.strftime("%Y-%m-%d %H:%M:%S") 
        } for t in turbines]

    # SCADA API Request with payload
    scada_api = WindAPI("https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-scada-data")
    df_scada = scada_api.multithread_get(payload)
    types_dict = {}
    for c in df_scada.columns:
        if c not in ['windturbine_id', 'wind_turbine', 'log_date'] :
            types_dict[c] = 'float'
        
    df_scada = df_scada.astype(types_dict)
    df_scada['log_date'] = to_datetime(df_scada['log_date'], utc=True)

    if (df_scada.empty == False):
        # Insert rows in MongoDB collection
        client_mongo["scada"].insert_many(df_scada.to_dict('records'))
        
    return df_scada

def update_windturbines_scada(turbines, df_scada, engine):
            
    timezone = pytz.timezone("Europe/Paris")
    params = []
    for turbine in turbines:
        windturbine_id = turbine[0]
        last_windturbine_log_date = df_scada[df_scada['wind_turbine'] == windturbine_id]['log_date'].max()
        if (pd.isnull(last_windturbine_log_date) == False):
            # Update Windturbine last scada update records
            last_windturbine_log_date = last_windturbine_log_date.tz_convert(timezone) + timedelta(minutes=10)
            params.append({
                "id": windturbine_id,
                "last_update": last_windturbine_log_date.strftime('%Y-%m-%d %H:%M:%S')
            })
    
    stmt = (
        update(windturbines)
        .where(windturbines.c.windturbine_id == bindparam("id"))
        .values(last_scada_update = bindparam("last_update"))
    )
    with Session(engine) as session:
        result = session.execute(
            stmt, params
        )
        session.commit()
        return result.rowcount
    
try:

    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    client_mongodb = mongodb_connection()
    engine_mariadb = mariadb_connection()
    
    # Fetch turbines to request
    turbines = fetch_windturbines_data(engine_mariadb)
    logging.info("Récupération de {} turbines dans la base MariaDB".format(len(turbines)))
    
    # Process SCADA DATA
    df_scada = import_scada_data(turbines, client_mongodb)
    logging.info("Total données SCADA importées : {}".format(df_scada.size))
    
    # Update windturbines last Scada update date
    wt_updated = update_windturbines_scada(turbines, df_scada, engine_mariadb)
    logging.info("Mise à jour de {} turbines dans la base MariaDB".format(wt_updated))
    
    print(f"Importation terminée")
except Exception as e:
    logging.error("Erreur lors de l'import des données Scada : %s", str(e))
    print(f"Importation échouée")