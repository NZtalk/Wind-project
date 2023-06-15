import dash
from dash import dcc, html, Input, Output
from from_db_to_df import *
import copy
import json

# Template layout
layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(l=30, r=30, b=20, t=30),
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9", #251f1f
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
default_windfarm = "1ec6d7a1-4b96-67a4-9358-df2d2b033685"
default_turbine = "1ec6d7ce-93f7-6ef8-abda-53533bbf8f7e"
df_map_wf = df_wf_turbine.groupby("windfarm_id")[["latitude", "longitude"]].mean().reset_index()
data = [dict(
    type="scattermapbox",
    lon=df_map_wf["longitude"],
    lat=df_map_wf["latitude"],
    text=df_map_wf["windfarm_id"],
    customdata=df_map_wf["windfarm_id"],
    marker=dict(size=15, opacity=0.6)
    )]
map_windfarms = dict(data=data, layout=layout)

# Dashboard structure
app.layout = html.Div(
    [
        # Header
        html.Div(
            [
                html.Div(
                    html.Img(
                        src=app.get_asset_url("dst-logo.png"),
                        id="dst_image",
                        style={
                            "height": "10vh",
                            "width": "10vh"
                        },
                    ),
                    className="header"
                    ),

                html.Div(
                    html.P("DataScientest Data Engineer Project |", id="title_dash_1"),
                    className="header"
                ),
                html.Div(
                    html.P("Into The Wind", id="title_dash_2"),
                    className="header"
                )
            ],
            className="row flex-display",
            style={"margin-bottom": "5px"},
        ),

        # First row
        html.Div(
            [
            html.Div(
                [dcc.Graph(id = "map_windfarm",
                           figure = map_windfarms,
                           style = {"height" : "45vh"}
                           )],
                className = "pretty_container seven columns",
            ),
            html.Div(
                dcc.Loading(id = "loading_2", 
                            children=[html.Div(dcc.Graph(id='scada_power_wf_graph',style = {"height" : "50vh"}))],
                            type="default"
                    ),
                    className = "pretty_container seven columns",
                )
            ],
            className="row flex-display"
        ),

        # Second row
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
                                className="dropdown"
                            ),
                            html.Div(
                                html.P("Last update: "+date_forecast.strftime("%d/%m/%Y, %H:%M:%S")),
                                id="date"
                            )
                        ],
                        className="row flex-display"
                    ),
                    dcc.Graph(id = "forecast_weather_graph", style = {"height" : "45vh"})
                ],
                className = "pretty_container seven columns",
            ),
            html.Div(
                dcc.Loading(id = "loading_1", 
                            children=[html.Div(dcc.Graph(id='forecast_power_wf_graph',style = {"height" : "50vh"}))],
                            type="default"),
                            className = "pretty_container seven columns",
                            )
            ],
            className="row flex-display"
        ),

        # Third row
        html.Div(
            [
            html.Div(
                [dcc.Graph(id = "map_turbines", style = {"height" : "45vh"})],
                className = "pretty_container seven columns",
            ),
            html.Div(
                [dcc.Graph(id = "scada_power_wt_graph", style = {"height" : "50vh"})],
                className = "pretty_container seven columns",
                )
            ],
            className="row flex-display"
        ),

        # Fourth row
        html.Div(
            [
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                dcc.Dropdown(
                                    id = "scada_stats_dropdown",
                                    options = [{"label":"Temperature", "value":"temp"}, 
                                            {"label":"Humidity", "value":"humidity"}, 
                                            {"label":"Pressure", "value":"pressure"}, 
                                            {"label":"Clouds", "value":"clouds"}, 
                                            {"label":"Windspeed", "value":"wind_speed"}],
                                    value = "temp",
                                ),
                                className="dropdown"
                            )
                        ],
                        className="row flex-display"
                    ),
                    dcc.Graph(id = "scada_stats_graph", style = {"height" : "45vh"})
                ],
                className = "pretty_container seven columns",
            ),

            html.Div(
                [dcc.Graph(id = "forecast_power_wt_graph", style = {"height" : "50vh"})],
                className = "pretty_container seven columns",
                )
            ],
            className="row flex-display"
        )

    ],
    id="main_container",
    style={"display": "flex", "flex-direction": "column"}
)

@app.callback(
    Output('click-data2', 'children'),
    Input('map_turbines', 'clickData'))
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)

# Map windfarm -> forecast and SCADA power windfarm
@app.callback(
        [Output("forecast_power_wf_graph", "figure"), Output("scada_power_wf_graph", "figure")],
        Input("map_windfarm", "clickData")
)
def graph_forecast_power_wf(clickData):
    
    if clickData is None:
        clickData = {"points": [{"customdata": default_windfarm}]}
        
    windfarm_id = clickData["points"][0]["customdata"]
    # Layout for Forecast Power Datas
    layout_fpw = copy.deepcopy(layout)
    df = forecast_power_by_turbine(df_forecast_weather, df_power_curve).groupby(["forecast_date", "windfarm_id"])["power_kw"].sum().reset_index()

    df = df[df["windfarm_id"] == windfarm_id]

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
    
    layout_fpw["title"] = "Forecast power in kW"
    figure_fpw = dict(data=data, layout=layout_fpw)
    
    # Layout for SCADA WF Datas
    wfCode = df_windfarms[df_windfarms['wfId'] == windfarm_id]['wfCode'].unique()[0]
    
    df_wf = df_scada_final[df_scada_final['wfId'] == windfarm_id].groupby(["log_date"])["active_power"].sum().reset_index()
    
    data_scada = [
        dict(
            type = "scatter",
            mode = "lines",
            name = "{}".format(wfCode),
            x = df_wf.log_date,
            y = df_wf.active_power,
            line = dict(shape = "spline", smoothing = "2"),
        )
    ]
    
    layout_scada = copy.deepcopy(layout)
    layout_scada["title"] = "SCADA Windfarm power production {}".format(wfCode)
    figure_scada = dict(data=data_scada, layout=layout_scada)
    
    return figure_fpw, figure_scada

# Map, slicer -> forecast weather
@app.callback(
    Output("forecast_weather_graph", "figure"),
    [
        Input("map_windfarm", "clickData"),
        Input("forecast_weather_dropdown", "value")
    ]
)
def graph_forecast_weather(clickData, dropdown):
    layout_fw = copy.deepcopy(layout)
    if clickData is None:
        clickData = {"points": [{"customdata": default_windfarm}]}
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
    
    yaxis = {"temp": "°C", "humidity": "%", "pressure": "hPa", "clouds": "", "wind_speed": "m/s"}
    layout_fw["title"] = "Forecast weather"
    layout_fw["yaxis"] = {'automargin': True,'title': {'text': yaxis[dropdown]}}

    figure = dict(data=data, layout=layout_fw)
    return figure

# Map windfarms -> Map turbines
@app.callback(Output("map_turbines", "figure"),[Input("map_windfarm", "clickData")])
def map_turbines(clickData):
    layout_map = copy.deepcopy(layout)
    if clickData is None:
        clickData = {"points": [{"customdata": default_windfarm}]}
    df = df_wf_turbine[df_wf_turbine["windfarm_id"] == clickData["points"][0]["customdata"]]
    data = [dict(
        type="scattermapbox",
        lon=df["longitude"],
        lat=df["latitude"],
        text=df["windturbine_id"],
        customdata=df["windturbine_id"],
        marker=dict(size=15, opacity=0.6)
        )]
    
    layout_map["title"] = "Turbines overview"
    layout_map["mapbox"]=dict(
        style="open-street-map",
        center=dict(lon=df[["latitude", "longitude"]].mean()[1], 
                    lat=df[["latitude", "longitude"]].mean()[0]),
        zoom=12,
        )

    figure = dict(data=data, layout=layout_map)
    return figure

# Map turbines -> forecast and SCADA windturbine power
@app.callback(
        [Output("forecast_power_wt_graph", "figure"), Output("scada_power_wt_graph", "figure")],
        Input("map_turbines", "clickData")
)
def graph_forecast_power_turbine(clickData):
    layout_fpmt = copy.deepcopy(layout)
    df = forecast_power_by_turbine(df_forecast_weather, df_power_curve)
    if clickData is None:
        clickData = {"points": [{"customdata": default_turbine}]}
        
    wtId = clickData["points"][0]["customdata"]
    df = df[df["windturbine_id"] == wtId]

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
    
    layout_fpmt["title"] = "Forecast power in kW"
    figure = dict(data=data, layout=layout_fpmt)

    # Layout for SCADA WF Datas
    wtCode = df_windturbines[df_windturbines['wtId'] == wtId]['wtCode'].unique()[0]
    
    df_wt = df_scada_final[df_scada_final['wtId'] == wtId].groupby(["log_date"])["active_power"].sum().reset_index()
    
    data_scada = [
        dict(
            type = "scatter",
            mode = "lines",
            name = "{}".format(wtCode),
            x = df_wt.log_date,
            y = df_wt.active_power,
            line = dict(shape = "spline", smoothing = "2"),
        )
    ]
    
    layout_scada = copy.deepcopy(layout)
    layout_scada["title"] = "SCADA Windturbine power production {}".format(wtCode)
    figure_scada = dict(data=data_scada, layout=layout_scada)
    
    return figure, figure_scada

if __name__ == '__main__':
    app.run_server(debug=True,host="0.0.0.0")
