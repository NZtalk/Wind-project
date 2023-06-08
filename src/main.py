from fastapi import FastAPI
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from sqlalchemy import create_engine
from bson import ObjectId
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional

app = FastAPI()

load_dotenv()

def mongodb_connection():
    user = os.environ["MONGO_ROOT_USERNAME"]
    password = os.environ["MONGO_ROOT_PASSWORD"]
    dbname = os.environ["MONGO_DATABASE"]
    MONGODB_DATABASE_URI = f"mongodb://{user}:{password}@mongodb:27017/"
    client = MongoClient(MONGODB_DATABASE_URI)
    database = client["ITW_DB"]
    return database

db = mongodb_connection()

def mariadb_connection():
    user = os.environ["MARIADB_USER"]
    password = os.environ["MARIADB_PASSWORD"]
    dbname = os.environ["MARIADB_DATABASE"]
    SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{user}:{password}@mariadb:3306/{dbname}'
    eng = create_engine(SQLALCHEMY_DATABASE_URI).connect()
    return eng

eng = mariadb_connection()

class MongoModel(BaseModel):
    id: Optional[str]

class CoordModel(BaseModel):
    lon: Optional[float]
    lat: Optional[float]

class WeatherModel(BaseModel):
    base: Optional[str]

class MainModel(BaseModel):
    temp: Optional[float]
    feels_like: Optional[float]
    temp_min: Optional[float]
    temp_max: Optional[float]
    pressure: Optional[int]
    humidity: Optional[int]
    sea_level: Optional[int]
    grnd_level: Optional[int]
    visibility: Optional[int]

class WindModel(BaseModel):
    speed: Optional[float]
    deg: Optional[int]
    gust: Optional[float]

class CloudsModel(BaseModel):
    all: Optional[int]

class SysModel(BaseModel):
    type: Optional[int]
    id: Optional[int]
    country: Optional[str]
    sunrise: Optional[int]
    sunset: Optional[int]
    timezone: Optional[int]
    name: Optional[str]
    cod: Optional[int]

class DataModel(BaseModel):
    coord: Optional[CoordModel]
    weather: Optional[List[WeatherModel]]
    main: Optional[MainModel]
    wind: Optional[WindModel]
    clouds: Optional[CloudsModel]
    dt: Optional[int]
    sys: Optional[SysModel]
    windfarm_id: Optional[str]
    name: Optional[str]

class MongoDBResponse(BaseModel):
    id: Optional[str] = Field(..., alias="id")
    extract_date: Optional[str]
    data: Optional[List[DataModel]]

class ForecastDataModel(BaseModel):
    coord: Optional[CoordModel]
    weather: Optional[List[WeatherModel]]
    main: Optional[MainModel]
    wind: Optional[WindModel]
    clouds: Optional[CloudsModel]
    dt: Optional[int]
    sys: Optional[SysModel]
    windfarm_id: Optional[str]
    name: Optional[str]
    

class ForescastModel(BaseModel):
    id: Optional[str] = Field(..., alias="id")
    extract_date: Optional[str]
    data: Optional[List[ForecastDataModel]]

def convert_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

# Routes MongoDB

@app.get("/mongodb/current")
def get_mongodb_current():
    collection = mongodb_connection()["current"]
    data = list(collection.find())
    converted_data = [MongoDBResponse(**{**item, 'id': convert_objectid(item["_id"])}) for item in data]
    return JSONResponse(content=jsonable_encoder(converted_data))

@app.get("/mongodb/forecast")
def get_mongodb_current():
    collection = mongodb_connection()["forecast"]
    data = list(collection.find())
    converted_data = [ForescastModel(**{**item, 'id': convert_objectid(item["_id"])}) for item in data]
    return JSONResponse(content=jsonable_encoder(converted_data))

@app.get("/mongodb/scada")
def get_mongodb_current():
    collection = mongodb_connection()["scada"]
    data = list(collection.find())
    converted_data = [MongoModel(**{**item, 'id': convert_objectid(item["_id"])}) for item in data]
    return JSONResponse(content=jsonable_encoder(converted_data))

# Routes MariaDB

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
