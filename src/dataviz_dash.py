
import dash
from dash.dependencies import Output, Input, State
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from flask import Flask

server = Flask(__name__)
app = dash.Dash(server=server, external_stylesheets=[dbc.themes.FLATLY])
app.title = 'Dashboard'



app.layout = dcc.Dropdown(
  id = 'dropdown',
  options=[
    {'label':'life expendancy', 'value': 'lifeExp'},
    {'label':'population', 'value': 'pop'}
    ],
    value = 'pop', # Valeur affichée par défaut dans le menus.
    multi = False # Spécifier si c'est un menu dropdown à multiple choix ou non.
)
if __name__ == '__main__' :
    app.run_server(debug=True, host='0.0.0.0')