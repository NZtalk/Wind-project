import pandas as pd
from create_ref_mariadb import mariadb_connection
from create_ref_mongodb import mongodb_connection
import matplotlib.pyplot as plt
import folium
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from io import BytesIO
import base64
import matplotlib.dates as mdates


def get_farms():
    eng = mariadb_connection()
    df = pd.read_sql(
    """SELECT windfarm_id,latitude,longitude
        FROM mariadb_itw.windfarms ;""", con=eng)
    return(df)



def last_weather_forecast_to_df():
    client = mongodb_connection()
    # Get the last update
    last_forcast_update = client.forecast.find({}, {"data" : 1}).sort("extract_date", -1).limit(1)
    last_forcast_update = list(last_forcast_update)[0]["data"]

    # Select required data from the document
    list_data = []
    for windfarm in last_forcast_update:
        for forecast in windfarm["list"]:
            dict_forecast = {}
            for key, value in forecast.items():
                if key == "dt_txt":
                    dict_forecast["forecast_date"] = value
                elif key == "wind":
                    dict_forecast["windspeed"] = value["speed"]
                else :
                    continue
            dict_forecast["windfarm_id"] = windfarm["windfarm_id"]
            list_data.append(dict_forecast)

    # Transform list in to DataFrame
    df = pd.DataFrame(list_data)
    return df

def get_html_weather_plot(df):
         # Créer votre graphique Matplotlib
        fig, ax = plt.subplots(figsize=(4, 2))        
        ax.plot(df['forecast_date'], df['windspeed'],'-o', markersize=4)
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        figure_width, figure_height = fig.get_size_inches()
        font_size = min(figure_width, figure_height) * 3  # Ajustez le facteur d'échelle selon vos préférences
        ax.tick_params(axis='x', labelsize=font_size)
        ax.tick_params(axis='y', labelsize=font_size)
        
        fig.autofmt_xdate() 
        ax.set_xlabel('Date')
        ax.set_ylabel('Windspeed')
        ax.set_title('Weather')
        # Convertir le graphique en une image au format PNG
        canvas = FigureCanvas(fig)
        buffer = BytesIO()
        canvas.print_png(buffer)
        plt.close(fig)
        # Convertir l'image en représentation base64
        image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        # Créer une balise HTML pour afficher l'image dans la popup
        html = f'<img src="data:image/png;base64,{image_base64}">'
        return(html)

def save_map(df):
    location = df['latitude'].mean(), df['longitude'].mean()
    m = folium.Map(location=location,zoom_start=4)
    list_farms = df['windfarm_id'].unique().tolist()
    for f in list_farms:   
        df_farm = df[df.windfarm_id==f]        
        lat = df_farm['latitude'].iloc[0]
        long = df_farm['longitude'].iloc[0]
        html = get_html_weather_plot(df_farm)
        
        popup = folium.Popup(html, max_width=500)
        folium.Marker([lat,long],
                    popup=popup,icon=folium.Icon( icon='home', prefix='fa')).add_to(m)
        
    m.save('map.html')


   

if __name__ == '__main__':
    df_farms = get_farms()
    df_forecast = last_weather_forecast_to_df()
    df=df_farms.merge(df_forecast, on='windfarm_id', how='left')
    save_map(df)

