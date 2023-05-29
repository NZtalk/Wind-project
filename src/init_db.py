from models import Windfarm, db
from flask import Flask
from classes.WindAPI import *

# create the app
app = Flask(__name__)
db.init_app(app)

url_windfarms = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windfarms"
url_windturbines = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windturbines"
url_powercurves = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-powercurves"

def get_farms(url):
    api = WindAPI(url)
    df = api.get()
    df = df.rename(columns={"id": "windfarm_id"})
    return(df)
 
def get_turbines_powercurves(url,list_payload,dict_rename=None):
    api = WindAPI(url)
    df = api.multithread_get(list_payload)
    if dict_rename:
        df = df.rename(columns={"id": "windturbine_id","lat":"latitude"
                                    ,"lng":"longitude"})
    return(df)

def add_geo_farms(df_farms,df_turbines):
    df_farms_geo = df_turbines[['windfarm_id','latitude','longitude']]
    df_farms_geo = df_farms_geo.dropna()
    df_farms_geo = df_farms_geo.groupby('windfarm_id', as_index=False)[['latitude','longitude']].mean()
    df = pd.merge(df_farms,df_farms_geo,on="windfarm_id",how="inner")
    return(df)

if __name__ == '__main__':
   with app.app_context():
        db.create_all()
      
        url_farms = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windfarms"
        url_turbines = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windturbines"
        url_powercurves = "https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-powercurves"
        
        #get data
        #farms
        df_farms = get_farms(url_farms)

        #turbines and add geo to farms
        #convert unique windfarm_id to list of dict
        list_payload = df_farms[['windfarm_id']].drop_duplicates().to_dict('records')
        dict_rename_col = {"id": "windturbine_id","lat":"latitude","lng":"longitude"}
        df_turbines = get_turbines_powercurves(url_turbines,list_payload,dict_rename_col)
        df_farms = add_geo_farms(df_farms,df_turbines)

        #keep only values with latitude and longitude
        list_farms = df_farms['windfarm_id'].values.tolist()
        df_turbines = df_turbines[df_turbines['windfarm_id'].isin(list_farms)]
    
        #powercurves
        list_payload = df_turbines[['windturbine_id']].drop_duplicates().to_dict('records')
        df_powercurves = get_turbines_powercurves(url_powercurves,list_payload)

        
        #insert to sql
        df_farms.to_sql('windfarms',con=eng,if_exists = 'append',index=false)
        df_turbines.to_sql('windturbines',con=eng,if_exists = 'append',index=false)
        df_powercurves.to_sql('powercurves',con=eng,if_exists = 'append',index=false)
        
        # Commit data and close session
        db.session.commit()
        db.session.close()