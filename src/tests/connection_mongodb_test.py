from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

user=os.environ["MONGO_ROOT_USERNAME"]
password=os.environ["MONGO_ROOT_PASSWORD"]
dbname=os.environ["MONGO_DATABASE"]
MONGODB_DATABASE_URI = f"mongodb://{user}:{password}@mongodb:27017/"

def mongodb_connection():

    client = MongoClient(MONGODB_DATABASE_URI)
    database = client["ITW_DB"]

    return database