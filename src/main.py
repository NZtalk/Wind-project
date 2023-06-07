from fastapi import FastAPI
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from sqlalchemy import create_engine
from bson import ObjectId
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI()

load_dotenv()

def mongodb_connection():
    user=os.environ["MONGO_ROOT_USERNAME"]
    password=os.environ["MONGO_ROOT_PASSWORD"]
    dbname=os.environ["MONGO_DATABASE"]
    MONGODB_DATABASE_URI = f"mongodb://{user}:{password}@mongodb:27017/"
    client = MongoClient(MONGODB_DATABASE_URI)
    database= client["ITW_DB"]
    return database

db = mongodb_connection()

def mariadb_connection():
    user = os.environ["MARIADB_USER"]
    password = os.environ["MARIADB_PASSWORD"]
    dbname= os.environ["MARIADB_DATABASE"]
    SQLALCHEMY_DATABASE_URI= f'mysql+pymysql://{user}:{password}@mariadb:3306/{dbname}'
    eng = create_engine(SQLALCHEMY_DATABASE_URI).connect()
    return eng

eng = mariadb_connection()

class MongoModel(BaseModel):
    id: str

class CoordModel(BaseModel):
    lon: float
    lat: float

def convert_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

#Routes MongoDB

@app.get("/mongodb/current")
def get_mongodb_current():
    collection = mongodb_connection()["current"]
    data = list(collection.find())
    converted_data = [MongoModel(**{**item, 'id': convert_objectid(item["_id"])}) for item in data]
    return JSONResponse(content=jsonable_encoder(converted_data))

@app.get("/mongodb/forecast")
def get_mongodb_current():
    collection = mongodb_connection()["forecast"]
    data = list(collection.find())
    converted_data = [MongoModel(**{**item, 'id': convert_objectid(item["_id"])}) for item in data]
    return JSONResponse(content=jsonable_encoder(converted_data))

@app.get("/mongodb/scada")
def get_mongodb_current():
    collection = mongodb_connection()["scada"]
    data = list(collection.find())
    converted_data = [MongoModel(**{**item, 'id': convert_objectid(item["_id"])}) for item in data]
    return JSONResponse(content=jsonable_encoder(converted_data))


#Routes MariaDB

@app.get("/mariadb/powercurves")
def get_mariadb_powercurves():
    with eng.connect() as connection:
        result = connection.execute("SELECT * FROM powercurves")
        data = result.fetchall()
    return {"data": data}

@app.get("/mariadb/windfarms")
def get_mariadb_windfarms():
    with eng.connect() as connection:
        result = connection.execute("SELECT * FROM windfarms")
        data = result.fetchall()
    return {"data": data}

@app.get("/mariadb/windturbines")
def get_mariadb_windturbines():
    with eng.connect() as connection:
        result = connection.execute("SELECT * FROM windturbines")
        data = result.fetchall()
    return {"data": data}

    