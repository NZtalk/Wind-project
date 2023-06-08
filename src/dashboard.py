import dash
from dash import Dash, dcc, html, dash_table, Input, Output
import pandas as pd
import plotly.express as px 
from from_db_to_df import *

# Definition of needed materials
client = mongodb_connection()
eng = mariadb_connection()

df_weather_forecast = last_forecast_weather_to_df(client)
df_power_curve = power_curve(eng)
df_power_forecast = forecast_power_by_turbine(df_weather_forecast, df_power_curve)
df_wf_turbine_id = pd.read_sql(
        """SELECT windturbine_id, windfarm_id
            FROM mariadb_itw.windturbines;""",
        con=eng
        )

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets,suppress_callback_exceptions=True)


app.layout = html.Div([
    html.H1("Tableau de bord du parc #123", style={'textAlign': 'left', 'color': 'black'}),

    #html.Div(dcc.Dropdown(id = "turbine_dropdown",
    #                    options= [{'label': i, 'value': i} for i in df_wf_turbine_id["windturbine_id"]],
    #                    value= df_wf_turbine_id["windturbine_id"][0]
    #)),

    html.Div(id = "output"),

    html.Div(dash_table.DataTable(
    id = "turbine_table",
    style_data={
        'whiteSpace': 'normal',
        'height': 'auto'
        },
    data = df_power_forecast.head(10).to_dict("records"),
    columns = [{"name": i, "id": i} for i in df_power_forecast],
    style_as_list_view = True,
    style_header={
        'backgroundColor': 'black',
        'fontWeight': 'bold',
        "color" : "white"
        }
        ), style = {
                 'display': 'inline-block',
                 'vertical-align': 'top'
                 }
    ),
    
    html.Div(dcc.Graph(id="turbine_graph"), 
             style = {
                 'display': 'inline-block',
                 'vertical-align': 'top'
                 }
    )

])


@app.callback(Output("turbine_graph", "figure"),
            Input("turbine_table", "active_cell"))
def update_graph(windturbine):
    df = df_power_forecast
    row = windturbine["row"]
    column = df.columns.get_loc("windturbine_id")
    windturbine = str(df.iloc[row,column])
    df = df_power_forecast[df_power_forecast["windturbine_id"] == windturbine]
    fig = px.bar(df,
                 x = df["forecast_date"],
                 y = df["power_kw"])
    return fig 


if __name__ == '__main__':
    app.run_server(debug=True,host="0.0.0.0")
