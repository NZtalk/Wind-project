from classes.WindAPI import *
from dotenv import load_dotenv


if __name__ == '__main__':

    load_dotenv()

    #get parcs
    parc_api = WindAPI("https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windfarms")
    df_parc = parc_api.get()
    df_parc = df_parc.rename(columns={"id": "windfarm_id"})
    print(df_parc)
    
    #get turbines
    turbines_api = WindAPI("https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-windturbines")
    #convert unique windfarm_id to list of dict
    list_payload = df_parc[['windfarm_id']].drop_duplicates().to_dict('records')
    df_turbines = turbines_api.multithread_get(list_payload)
    df_turbines = df_turbines.rename(columns={"id": "windturbine_id"})
    #print(df_turbines)

    df_farms = df_turbines[['windfarm_id','lat','lng']]
    pd.set_option('display.max_rows', 500)
    df_farms = df_farms.dropna()
    df_farms = df_farms.groupby('windfarm_id', as_index=False)[['lat','lng']].mean()
    print(df_farms)
 

    #get power curves
    powercurves_api= WindAPI("https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-powercurves")
    #convert unique windturbine_id to list of dict
    list_payload = df_turbines[['windturbine_id']].drop_duplicates().to_dict('records')
    df_powercurves = powercurves_api.multithread_get(list_payload)
    print(df_powercurves)

    #scada 10min
    #scada_api = WindAPI("https://api-staging.anavelbraz.app:8443/api/public/dst/fetch-scada-data")
    #start_date = "2023-04-27 00:00:00"
    #end_date = "2023-04-28 00:00:00"
    #test with 2 windturbines id for start date ad end date fixed
    #list_payload = [
    #    {
    #    "windturbine_id": '1ec6d7ce-943d-6ba6-abf4-53533bbf8f7e',
    #    "start_date": start_date,
    #    "end_date": end_date
    #    },
    #   {"windturbine_id": '1eca5108-218f-66d4-93df-8792954b671b',
    #   "start_date": start_date,
    #    "end_date": end_date
    #  }
    #]

    #df_scada = scada_api.multithread_get(list_payload)
    #print(df_scada)

    





 

