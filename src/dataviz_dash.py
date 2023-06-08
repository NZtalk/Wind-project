
import dash
from dash.dependencies import Output, Input, State
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from flask import Flask
import dash_html_components as html

server = Flask(__name__)
app = dash.Dash(server=server, external_stylesheets=[dbc.themes.FLATLY])
app.title = 'Dashboard'



app.layout = html.Div([
  html.H1('MAP'),
  html.Iframe(id='map',srcDoc=open('map.html','r').read(),width='100%',height='600')]
)
if __name__ == '__main__' :
    app.run_server(debug=True, host='0.0.0.0')