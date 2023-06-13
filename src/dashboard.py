import dash
from dash import Dash, dcc, html, dash_table, Input, Output
import pandas as pd
import plotly.express as px 
from from_db_to_df import *
import copy
import json

# Definition of needed materials
client = mongodb_connection()
eng = mariadb_connection()

df_forecast_weather = last_forecast_weather_to_df(client)
df_power_curve = power_curve(eng)
df_power_forecast = forecast_power_by_turbine(df_forecast_weather, df_power_curve)
df_wf_turbine = pd.read_sql(
        """SELECT *
            FROM mariadb_itw.windturbines;""",
        con=eng
        )

layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(l=30, r=30, b=20, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
    title="Windfarms overview",
    mapbox=dict(
        style="open-street-map",
        center=dict(lon=3.5, 
                    lat=49.2),
        zoom=7,
    ),
)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, 
                external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True)
app.title = "Into the Wind"

# Map windfarms
df = df_wf_turbine.groupby("windfarm_id")[["latitude", "longitude"]].mean()

def create_map_windfarm():
    df = df_wf_turbine.groupby("windfarm_id")[["latitude", "longitude"]].mean().reset_index()
    trace = [dict(
        type="scattermapbox",
        lon=df["longitude"],
        lat=df["latitude"],
        text=df["windfarm_id"],
        customdata=df["windfarm_id"],
        marker=dict(size=15, opacity=0.6),
    )]
    figure = dict(data=trace, layout=layout)
    return figure

map_windfarms = create_map_windfarm()


app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("dst-logo.png"),
                            id="dst_image",
                            style={
                                "height": "100px",
                                "width": "100px",
                                "margin-bottom": "5px",
                            },
                        )
                    ],
                    className="one-third column",
                ),
                html.Div(
                        html.H3(
                            "Into The Wind",
                            style={"margin-bottom": "0px"},
                        ),
                    className="one-half column",
                    id="title",
                ),
            ],
            id="header",
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),

        html.Div(
            [
            html.Div(
                [dcc.Graph(id = "map_windfarm",
                           figure = map_windfarms)],
                className = "pretty_container seven columns",
            ),
            html.Div(
                [html.Pre(id='click-data')],
                className = "pretty_container seven columns",
                )
            ],
            className="row flex-display"
        ),

        html.Div(
            [
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                dcc.Dropdown(
                                    id = "forecast_weather_dropdown",
                                    options = [{"label":"Temperature", "value":"temp"}, 
                                            {"label":"Humidity", "value":"humidity"}, 
                                            {"label":"Pressure", "value":"pressure"}, 
                                            {"label":"Clouds", "value":"clouds"}, 
                                            {"label":"Windspeed", "value":"wind_speed"}],
                                    value = "temp"
                                ),
                                style = {
                                #"height": "60px",
                                "width": "50%",
                                "display": "inline-block"
                                }
                            ),
                            html.Div(
                                html.P("Titre graph"),
                                style = {
                                #"height": "60px",
                                "width": "80%",
                                #"margin-left": "50px",
                                "display": "inline-block",
                                "justify-content":"center",
                                "align-items":"center"
                                }
                            ),
                            html.Div(
                                html.P("Etiquette update"),
                                style = {
                                #"height": "60px",
                                "width": "100%",
                                "display": "inline-block",
                                }
                            )
                        ],
                        className="row container-display",
                        id="weather_graph_header"
                    ),
                    dcc.Graph(id = "forecast_weather_graph")
                ],
                className = "pretty_container seven columns",
            ),

            html.Div(
                [dcc.Graph(id = "forecast_power_wf_graph")],
                className = "pretty_container seven columns",
                )
            ],
            className="row flex-display"
        )

    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"}
)


@app.callback(
    Output('click-data', 'children'),
    Input('map_windfarm', 'clickData'))
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)


# Map, slicer -> forecast weather
@app.callback(
    Output("forecast_weather_graph", "figure"),
    [
        Input("map_windfarm", "clickData"),
        Input("forecast_weather_dropdown", "value")
    ]
)
def chart_forecast_weather(clickData, dropdown):
    layout_fw = copy.deepcopy(layout)
    if clickData is None:
        clickData = {"points": [{"customdata": "1ec6d7a1-4b96-67a4-9358-df2d2b033685"}]}
    df = df_forecast_weather[df_forecast_weather["windfarm_id"] == clickData["points"][0]["customdata"]]
    data = [
            dict(
                type = "scatter",
                mode = "lines",
                name = "Forecast weather",
                x = df.forecast_date,
                y = df[dropdown],
                line = dict(shape = "spline", smoothing = "2", color = "#F9ADA0"),
            )
        ]
    
    yaxis = {"temp": "Celsius", "humidity": "%", "pressure": "hPa", "clouds": "", "wind_speed": "m/s"}
    layout_fw["title"] = "Forecast weather - "+dropdown
    layout_fw["yaxis"] = {'anchor': 'y', 'domain': [0.0, 1.0], 'title': {'text': yaxis[dropdown]}}

    figure = dict(data=data, layout=layout_fw)
    return figure

# Map windfarm -> forecast power
@app.callback(
    Output("forecast_power_wf_graph", "figure"),
    [
        Input("map_windfarm", "clickData")
    ]
)
def chart_forecast_power_wf(clickData):
    layout_fpw = copy.deepcopy(layout)
    df = forecast_power_by_turbine(df_forecast_weather, df_power_curve).groupby(["forecast_date", "windfarm_id"])["power_kw"].sum().reset_index()
    if clickData is None:
        clickData = {"points": [{"customdata": "1ec6d7a1-4b96-67a4-9358-df2d2b033685"}]}
    df = df[df["windfarm_id"] == clickData["points"][0]["customdata"]]

    data = [
            dict(
                type = "scatter",
                mode = "lines",
                name = "Forecast power",
                x = df.forecast_date,
                y = df.power_kw,
                line = dict(shape = "spline", smoothing = "2", color = "#F9ADA0"),
            )
        ]
    
    layout_fpw["title"] = "Forecast power"

    figure = dict(data=data, layout=layout_fpw)
    return figure



if __name__ == '__main__':
    app.run_server(debug=True,host="0.0.0.0")
