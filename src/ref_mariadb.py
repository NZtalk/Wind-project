from sqlalchemy import *
from dotenv import load_dotenv
import os
from classes.WindAPI import *

load_dotenv()

user=os.environ["MARIADB_USER"]
password=os.environ["MARIADB_PASSWORD"]
dbname=os.environ["MARIADB_DATABASE"]
SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{user}:{password}@mariadb:3306/{dbname}'


# Test if it works
eng = create_engine(SQLALCHEMY_DATABASE_URI).connect()

#Create table
""" meta = MetaData()

windfarms = Table(
   'windfarms', meta, 
   Column('windfarm_id', VARCHAR(100), primary_key=True), 
   Column('code', VARCHAR(100))
) 

windturbines = Table(
   'windturbines', meta, 
   Column('windturbine_id', VARCHAR(100), primary_key=True), 
   Column('windfarm_id', VARCHAR(100),ForeignKey("windfarms.windfarm_id")),
   Column('latitude', FLOAT),
   Column('longitude', FLOAT)
)

powercurves = Table(
   'powercurves', meta, 
   Column('windturbines_id', VARCHAR(100), ForeignKey("windturbines.windturbine_id"),primary_key=True), 
   Column('windspeed', FLOAT,primary_key=True),
   Column('power', FLOAT)
)

meta.create_all(eng)
 """

#insert data
parc_api = WindAPI("https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windfarms")
df_parc = parc_api.get()
df_parc = df_parc.rename(columns={"id": "windfarm_id"})
df_parc.loc[df_parc['windfarm_id'] == '1ec6d7a1-4b96-67a4-9358-df2d2b033685', 'code'] = "test"
print(df_parc)
df_parc.to_sql('windfarms',con=eng,if_exists = 'append',index=false)


