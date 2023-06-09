from fastapi import FastAPI, HTTPException, Depends, Response
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from bson import ObjectId
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional
from bson import ObjectId
import json
import requests
from sqlalchemy.dialects.mysql import WKBElement

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
    eng = create_engine(SQLALCHEMY_DATABASE_URI)
    return eng

eng = mariadb_connection()

class Coord(BaseModel):
    lon: Optional[float]
    lat: Optional[float]


class Weather(BaseModel):
    id: Optional[int]
    main: Optional[str]
    description: Optional[str]
    icon: Optional[str]


class Main(BaseModel):
    temp: Optional[float]
    feels_like: Optional[float]
    temp_min: Optional[float]
    temp_max: Optional[float]
    pressure: Optional[int]
    humidity: Optional[int]
    sea_level: Optional[int]
    grnd_level: Optional[int]
    visibility: Optional[int]


class Wind(BaseModel):
    speed: Optional[float]
    deg: Optional[int]
    gust: Optional[float]


class Clouds(BaseModel):
    all: Optional[int]


class Sys(BaseModel):
    type: Optional[int]
    id: Optional[int]
    country: Optional[str]
    sunrise: Optional[int]
    sunset: Optional[int]
    timezone: Optional[int]


class Data(BaseModel):
    coord: Optional[Coord]
    weather: Optional[List[Weather]]
    main: Optional[Main]
    wind: Optional[Wind]
    clouds: Optional[Clouds]
    dt: Optional[str]
    sys: Optional[Sys]
    windfarm_id: Optional[str]
    name: Optional[str]

class MongoDBResponse(BaseModel):
    id: Optional[str]
    extract_date: datetime
    data: List[Dict[str,Any]]

    class Config:
        json_encoders = {
            ObjectId: Optional[str],
            datetime: lambda v: v.isoformat(timespec='milliseconds') + 'Z'
        }

class ForecastDataModel(BaseModel):
    coord: Optional[Coord]
    weather: Optional[List[Weather]]
    main: Optional[Main]
    wind: Optional[Wind]
    clouds: Optional[Clouds]
    dt: Optional[datetime]
    sys: Optional[Sys]
    windfarm_id: Optional[str]
    name: Optional[str]
    

def convert_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

def sort_mongo_data(data):
    sorted_data = sorted(data, key= lambda x : x['extract_date'])
    return sorted_data

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)
    

   
# Routes MongoDB

@app.get("/mongodb/current")
def get_mongodb_current(limit: int = 20, offset: int = 0):
    collection = mongodb_connection()["current"]
    total_count = collection.count_documents({})  # Total count of documents

    # Retrieve paginated data
    data = list(collection.find().skip(offset).limit(limit))

    sorted_data = sort_mongo_data(data)

    response_data = MongoDBResponse(extract_date=datetime.now(), data=sorted_data)

    content = json.dumps(response_data.dict(), cls=CustomJSONEncoder)
    headers = {"Content-Type": "application/json"}

    return Response(content=content, headers=headers)


@app.get("/mongodb/forecast")
def get_mongodb_forecast(limit: int = 20, offset: int = 0):
    collection = mongodb_connection()["forecast"]
    total_count = collection.count_documents({})  # Total count of documents

    # Retrieve paginated data
    data = list(collection.find().skip(offset).limit(limit))

    sorted_data = sort_mongo_data(data)

    response_data = MongoDBResponse(extract_date=datetime.now(), data=sorted_data)

    content = json.dumps(response_data.dict(), cls=CustomJSONEncoder)
    headers = {"Content-Type": "application/json"}

    return Response(content=content, headers=headers)


@app.get("/mongodb/scada")
def get_mongodb_scada(limit: int = 20, offset: int = 0):
    collection = mongodb_connection()["scada"]
    total_count = collection.count_documents({})  # Total count of documents

    # Retrieve paginated data
    data = list(collection.find().skip(offset).limit(limit))

    # Convert and organize the data
    converted_data = []
    for item in data:
        converted_item = {}
        for key, value in item.items():
            if isinstance(value, ObjectId):
                converted_item[key] = str(value)
            elif isinstance(value, dict):
                converted_item[key] = {sub_key: convert_objectid(sub_value) for sub_key, sub_value in value.items()}
            else:
                converted_item[key] = value
        converted_data.append(converted_item)

    return JSONResponse(content=jsonable_encoder(converted_data))



# Routes MariaDB
@app.get("/mariadb/powercurves")
def get_mariadb_powercurves():
    with eng.connect() as connection:
        query = text("SELECT * FROM powercurves")
        result = connection.execute(query)
        data = [dict(row) for row in result]

        encoded_data = jsonable_encoder(data, custom_encoder={WKBElement: lambda _: None})
    
    return {"data": encoded_data}


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

@app.get("/api/endpoint")
def get_api_data():
    response = requests.get('https://votre-api.com/endpoint')

    if response.status_code == 200:
        data = response.json()
        converted_data = JSONResponse(content=jsonable_encoder(data))
        return converted_data

    else:
        raise HTTPException(status_code=response.status_code, detail=f"La requête API a échoué avec le code {response.status_code}")

