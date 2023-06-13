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
from sqlalchemy.dialects import mysql
from sqlalchemy.types import TypeDecorator
from sqlalchemy.exc import IntegrityError
import math



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
    
class PowerCurve(BaseModel):
    windturbine_id : str
    windspeed : int
    power: int

class WindTurbine(BaseModel):
    windturbine_id : str
    windfarm_id : str
    code: str
    latitude : float
    longitude : float
    last_scada_update: datetime

class WindFarm(BaseModel):
    windfarm_id : str
    code : str
    latitude : float
    longitude: float
    last_meteo_update : datetime

   
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
            elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                converted_item[key] = None  # Replace NaN or infinite value with None or a string representation
            else:
                converted_item[key] = value
        converted_data.append(converted_item)

    return JSONResponse(content=jsonable_encoder(converted_data))



# Routes MariaDB

@app.get("/mariadb/powercurves")
async def get_powercurves(page: int = 1, per_page: int = 10, all_data : bool =False):
    # Calculate the OFFSET value based on the requested page and per_page values
    if all_data:
        offset = 0
        limit = None  # No limit, retrieve all data
    else:
        offset = (page - 1) * per_page
        limit = per_page

    # Establish a connection to the database
    with eng.connect() as conn:
        
        # Select a subset of data from the table using LIMIT and OFFSET
        query = text("SELECT * FROM mariadb_itw.powercurves LIMIT :per_page OFFSET :offset;")
        result = conn.execute(query, {"per_page": per_page, "offset": offset})

        # Fetch all rows from the result
        rows = result.fetchall()

        # Convert the rows to a list of dictionaries
        data = []
        for row in rows:
                        
            # Create a dictionary with windturbine_id, windspeed, and power
            row_data = {
                "windturbine_id": row.windturbine_id,
                "windspeed": row.windspeed,
                "power": row.power
            }
            data.append(row_data)

    # Return the data as the response
    return {"data": data}

@app.post("/powercurves")
async def create_powercurves(powercurve: PowerCurve):
    # Extract the data from the PowerCurve Schema object
    windturbine_id = powercurve.windturbine_id
    windspeed = powercurve.windspeed
    power = powercurve.power

    # Check if the windturbine_id exists in the windturbines table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM windturbines WHERE windturbine_id = :windturbine_id")
        result = conn.execute(query, {"windturbine_id": windturbine_id})
        if result.scalar() == 0:
            # Wind turbine does not exist, create a new entry
            query = text("INSERT INTO windturbines (windturbine_id) VALUES (:windturbine_id)")
            try:
                conn.execute(query, {"windturbine_id": windturbine_id})
                conn.commit()
            except IntegrityError:
                raise HTTPException(status_code=500, detail="Failed to create wind turbine")

        # Check if the windturbine_id already exists in the powercurves table
        query = text("SELECT COUNT(*) FROM powercurves WHERE windturbine_id = :windturbine_id AND windspeed = :windspeed")
        result = conn.execute(query, {"windturbine_id": windturbine_id, "windspeed": windspeed})
        if result.scalar() > 0:
            raise HTTPException(status_code=409, detail="Winturbine_id already exists")

        # Insert the power curve into the powercurves table
        query = text("INSERT INTO powercurves (windturbine_id, windspeed, power) VALUES (:windturbine_id, :windspeed, :power)")
        try:
            conn.execute(query, {"windturbine_id": windturbine_id, "windspeed": windspeed, "power": power})
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Internal server error")

    # Return the created powercurve data
    return {"windturbine_id": windturbine_id, "windspeed": windspeed, "power": power}


@app.put('/powercurves')
async def update_powercurves(powercurve: PowerCurve):
    # Extract the data from the PowerCurve Schema object
    windturbine_id = powercurve.windturbine_id
    windspeed = powercurve.windspeed
    power = powercurve.power

    # Check if the windturbine_id exists in the windturbines table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM windturbines WHERE windturbine_id = :windturbine_id")
        result = conn.execute(query, {"windturbine_id": windturbine_id})
        if result.scalar() == 0:
            raise HTTPException(status_code=404, detail="Windturbine not found")

        # Update the power curve in the powercurves table
        query = text("UPDATE powercurves SET power = :power, windspeed= :windspeed WHERE windturbine_id = :windturbine_id")
        try:
            conn.execute(query, {"windturbine_id": windturbine_id, "windspeed": windspeed, "power": power})
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Internal server error")

    # Return the updated powercurve data
    return {"windturbine_id": windturbine_id, "windspeed": windspeed, "power": power}

@app.delete('/powercurves')
async def delete_powercurves(windturbine_id: str):
    # Check if the windturbine_id exists in the windturbines table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM powercurves WHERE windturbine_id = :windturbine_id")
        result = conn.execute(query, {"windturbine_id": windturbine_id})
        if result.scalar() == 0:
            raise HTTPException(status_code=404, detail="Windturbine not found")

    # Delete the powercurves with the specified windturbine_id
    with eng.connect() as conn:
        query = text("DELETE FROM powercurves WHERE windturbine_id = :windturbine_id")
        try:
            result = conn.execute(query, {"windturbine_id": windturbine_id})
            deleted_count = result.rowcount
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Internal server error")

    if deleted_count == 0:
        raise HTTPException(status_code=404, detail=" Data  in Powercurves not found")

    # Return success message
    return {"message": f"All data in powercurves with windturbine_id {windturbine_id} deleted successfully"}

   
@app.get("/mariadb/windfarms")
async def get_windfarms(page: int =1, per_page: int =10, all_data: bool = False):

    if all_data:
        offeset= 0
        limit = None
    else:
        offset = (page - 1) * per_page
        limit = per_page

    with eng.connect() as conn:
        query = text("SELECT * FROM mariadb_itw.windfarms LIMIT :limit OFFSET :offset;")
        result = conn.execute(query, {"limit": limit, "offset": offset})

        rows = result.fetchall()

        data=[]
        for row in rows:
            row_data = {
                "windfarm_id": row.windfarm_id,
                "code": row.code,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "last_meteo_update": row.last_meteo_update
            }
            data.append(row_data)
    return {"data":data}

@app.post("/windfarms")
async def create_windfarm(windfarm: WindFarm):
    # Extract the data from the WindFarm Schema object
    windfarm_id = windfarm.windfarm_id
    code = windfarm.code
    latitude = windfarm.latitude
    longitude = windfarm.longitude
    last_meteo_update = windfarm.last_meteo_update

    # Insert the windfarm into the windfarms table
    query = text("INSERT INTO windfarms (windfarm_id, code, latitude, longitude, last_meteo_update) "
                 "VALUES (:windfarm_id, :code, :latitude, :longitude, :last_meteo_update)")
    try:
        with eng.connect() as conn:
            conn.execute(query, {"windfarm_id": windfarm_id, "code": code, "latitude": latitude,
                                 "longitude": longitude, "last_meteo_update": last_meteo_update})
            conn.commit()
    except IntegrityError:
        raise HTTPException(status_code=500, detail="Failed to create wind farm")

    # Return the created windfarm data
    return {"windfarm_id": windfarm_id, "code": code, "latitude": latitude,
            "longitude": longitude, "last_meteo_update": last_meteo_update}


@app.put('/windfarms')
async def update_windfarm(windfarm: WindFarm):
    # Extract the data from the WindFarmSchema object
    windfarm_id = windfarm.windfarm_id
    code = windfarm.code
    latitude = windfarm.latitude
    longitude = windfarm.longitude
    last_meteo_update = windfarm.last_meteo_update

    # Check if the windfarm_id exists in the windfarms table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM windfarms WHERE windfarm_id = :windfarm_id")
        result = conn.execute(query, {"windfarm_id": windfarm_id})
        if result.scalar() == 0:
            raise HTTPException(status_code=404, detail="Windfarm not found")

        # Update the windfarm in the windfarms table
        query = text("UPDATE windfarms SET code = :code, latitude = :latitude, longitude = :longitude, "
                     "last_meteo_update = :last_meteo_update WHERE windfarm_id = :windfarm_id")
        try:
            conn.execute(query, {"windfarm_id": windfarm_id, "code": code, "latitude": latitude,
                                 "longitude": longitude, "last_meteo_update": last_meteo_update})
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Internal server error")

    # Return the updated windfarm data
    return {"windfarm_id": windfarm_id, "code": code, "latitude": latitude,
            "longitude": longitude, "last_meteo_update": last_meteo_update}


@app.delete('/windfarms')
async def delete_windfarm(windfarm_id: str):
    # Check if the windfarm_id exists in the windfarms table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM windfarms WHERE windfarm_id = :windfarm_id")
        result = conn.execute(query, {"windfarm_id": windfarm_id})
        if result.scalar() == 0:
            raise HTTPException(status_code=404, detail="Windfarm not found")

    # Delete the windfarm with the specified windfarm_id
    with eng.connect() as conn:
        query = text("DELETE FROM windfarms WHERE windfarm_id = :windfarm_id")
        try:
            result = conn.execute(query, {"windfarm_id": windfarm_id})
            deleted_count = result.rowcount
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Internal server error")

    if deleted_count == 0:
        raise HTTPException(status_code=404, detail="Data in Windfarms not found")

    # Return success message
    return {"message": f"All data in windfarms with windfarm_id {windfarm_id} deleted successfully"}


@app.get("/mariadb/windturbines")
async def get_windturbines(page: int =1, per_page: int =10, all_data: bool = False):
    if all_data:
        offset = 0
        limit = None
    else:
        offset = (page -1) * per_page
        limit = per_page
        
    with eng.connect() as conn:
        query = text("SELECT * FROM mariadb_itw.windturbines LIMIT :limit OFFSET :offset;")
        result = conn.execute(query, {"limit": limit, "offset": offset})

        rows = result.fetchall()

        data= []
        for row in rows:
            row_data = {
                "windturbine_id": row.windturbine_id,
                "windfarm_id": row.windfarm_id,
                "code": row.code,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "last_scada_update": row.last_scada_update
            }
            data.append(row_data)
    return {"data": data}

@app.post("/windturbines")
async def create_windturbine(windturbine: WindTurbine):
    # Extract the data from the WindTurbineSchema object
    windturbine_id = windturbine.windturbine_id
    windfarm_id = windturbine.windfarm_id
    code = windturbine.code
    latitude = windturbine.latitude
    longitude = windturbine.longitude
    last_scada_update = windturbine.last_scada_update

    # Check if the windfarm_id exists in the windfarms table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM windfarms WHERE windfarm_id = :windfarm_id")
        result = conn.execute(query, {"windfarm_id": windfarm_id})
        if result.scalar() == 0:
            # Wind farm does not exist, create a new entry
            query = text("INSERT INTO windfarms (windfarm_id) VALUES (:windfarm_id)")
            try:
                conn.execute(query, {"windfarm_id": windfarm_id})
                conn.commit()
            except IntegrityError:
                raise HTTPException(status_code=500, detail="Failed to create wind farm")

        # Insert the windturbine into the windturbines table
        query = text("INSERT INTO windturbines (windturbine_id, windfarm_id, code, latitude, longitude, last_scada_update) "
                     "VALUES (:windturbine_id, :windfarm_id, :code, :latitude, :longitude, :last_scada_update)")
        try:
            conn.execute(query, {"windturbine_id": windturbine_id, "windfarm_id": windfarm_id, "code": code,
                                 "latitude": latitude, "longitude": longitude, "last_scada_update": last_scada_update})
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Failed to create wind turbine")

    # Return the created windturbine data
    return {"windturbine_id": windturbine_id, "windfarm_id": windfarm_id, "code": code,
            "latitude": latitude, "longitude": longitude, "last_scada_update": last_scada_update}



@app.put('/windturbines')
async def update_windturbine(windturbine: WindTurbine):
    # Extract the data from the WindTurbineSchema object
    windturbine_id = windturbine.windturbine_id
    windfarm_id = windturbine.windfarm_id
    code = windturbine.code
    latitude = windturbine.latitude
    longitude = windturbine.longitude
    last_scada_update = windturbine.last_scada_update

    # Check if the windturbine_id exists in the windturbines table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM windturbines WHERE windturbine_id = :windturbine_id")
        result = conn.execute(query, {"windturbine_id": windturbine_id})
        if result.scalar() == 0:
            raise HTTPException(status_code=404, detail="Windturbine not found")

        # Check if the windfarm_id exists in the windfarms table
        query = text("SELECT COUNT(*) FROM windfarms WHERE windfarm_id = :windfarm_id")
        result = conn.execute(query, {"windfarm_id": windfarm_id})
        if result.scalar() == 0:
            raise HTTPException(status_code=404, detail="Windfarm not found")

        # Update the windturbine in the windturbines table
        query = text("UPDATE windturbines SET windfarm_id = :windfarm_id, code = :code, latitude = :latitude, "
                     "longitude = :longitude, last_scada_update = :last_scada_update "
                     "WHERE windturbine_id = :windturbine_id")
        try:
            conn.execute(query, {"windturbine_id": windturbine_id, "windfarm_id": windfarm_id, "code": code,
                                 "latitude": latitude, "longitude": longitude, "last_scada_update": last_scada_update})
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Internal server error")

    # Return the updated windturbine data
    return {"windturbine_id": windturbine_id, "windfarm_id": windfarm_id, "code": code,
            "latitude": latitude, "longitude": longitude, "last_scada_update": last_scada_update}


@app.delete('/windturbines')
async def delete_windturbine(windturbine_id: str):
    # Check if the windturbine_id exists in the windturbines table
    with eng.connect() as conn:
        query = text("SELECT COUNT(*) FROM windturbines WHERE windturbine_id = :windturbine_id")
        result = conn.execute(query, {"windturbine_id": windturbine_id})
        if result.scalar() == 0:
            raise HTTPException(status_code=404, detail="Windturbine not found")

    # Delete the windturbine with the specified windturbine_id
    with eng.connect() as conn:
        query = text("DELETE FROM windturbines WHERE windturbine_id = :windturbine_id")
        try:
            result = conn.execute(query, {"windturbine_id": windturbine_id})
            deleted_count = result.rowcount
            conn.commit()
        except IntegrityError:
            raise HTTPException(status_code=500, detail="Internal server error")

    if deleted_count == 0:
        raise HTTPException(status_code=404, detail="Data in Windturbines not found")

    # Return success message
    return {"message": f"All data in windturbines with windturbine_id {windturbine_id} deleted successfully"}




@app.get("/ping")
async def ping():
    """Vérifie que l'API est fonctionnelle."""
    return {"message": "API is functional."}


