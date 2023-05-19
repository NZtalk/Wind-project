from sqlalchemy import *
from dotenv import load_dotenv
import os

load_dotenv()

user=os.environ["MARIADB_USER"]
password=os.environ["MARIADB_PASSWORD"]
dbname=os.environ["MARIADB_DATABASE"]
SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{user}:{password}@mariadb:3306/{dbname}'


# Test if it works
eng = create_engine(SQLALCHEMY_DATABASE_URI).connect()

#Create table
meta = MetaData()

windfarms = Table(
   'windfarms', meta, 
   Column('windfarm_id', VARCHAR(100), primary_key=True), 
   Column('code', VARCHAR(100)),
   Column('latitude', FLOAT),
   Column('longitude', FLOAT),
   Column('last_meteo_update', DATETIME)
  
) 


windturbines = Table(
   'windturbines', meta, 
   Column('windturbine_id', VARCHAR(100), primary_key=True), 
   Column('windfarm_id', VARCHAR(100),ForeignKey("windfarms.windfarm_id")),
   Column('code', VARCHAR(100)),
   Column('latitude', FLOAT),
   Column('longitude', FLOAT),
   Column('last_scada_udpdate', DATETIME)
)

powercurves = Table(
   'powercurves', meta, 
   Column('windturbine_id', VARCHAR(100), ForeignKey("windturbines.windturbine_id"),primary_key=True), 
   Column('windspeed', FLOAT,primary_key=True),
   Column('power', FLOAT)
)

meta.create_all(eng)


