import json
import base64
import datetime
import requests
import pathlib
import math
import pandas as pd, numpy as np
import flask
import dash
import dash_core_components as dcc
import dash_html_components as html
import chart_studio.plotly as py
import plotly.graph_objs as go

from dash.dependencies import Input, Output, State
from plotly import tools


app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)

server = app.server

URLS = {"state_codes":"http://3.134.92.99:8081/covid19/india/statecodes",
        "india_rt":"http://3.134.92.99:8081/covid19/india/",
        "india_his":"http://3.134.92.99:8081/covid19/india/historical",
        "states_rt":"http://3.134.92.99:8081/covid19/india/statewise/realtime",
        "district_rt":"http://3.134.92.99:8081/covid19/india/districtwise/realtime",
        "news_search":"http://3.134.92.99:5001/newsapi/search/"}

dark_theme =  {
    'dark': True,
    'detail': '#007439',
    'primary': '#00EA64',
    'secondary': '#6E6E6E',
}

state_codes = requests.get(URLS['state_codes']).json()['data']
statecode_dict = dict({})
for state in state_codes:
    statecode_dict[state['state_code']] = state['state_name']
del state_codes

# Display big numbers in readable format
def human_format(num):
    try:
        num = float(num)
        # If value is 0
        if num == 0:
            return 0
        # Else value is a number
        if num < 1000000:
            return num
        magnitude = int(math.log(num, 1000))
        mantissa = str(int(num / (1000 ** magnitude)))
        return mantissa + ["", "K", "M", "G", "T", "P"][magnitude]
    except:
        return num

bg_gradients=['linear-gradient(to top, #a18cd1 0%, #fbc2eb 100%)',
              'linear-gradient(120deg, #a1c4fd 0%, #c2e9fb 100%)',
              'linear-gradient(to right, #43e97b 0%, #38f9d7 100%)']

colors=['#262833','#4ACFAC','#7E8CE0','#36C7D0','#FFA48E']

def get_top_bar_cell(cellTitle, cellValue1, cellValue2,color):
    if cellValue2 == None:
        return html.Div(
            className="two-col",
            children=[
                html.P(className="p-top-bar", children=cellTitle),
                html.P(id=cellTitle, className="display-none", children=cellValue1),
                html.P(children=human_format(cellValue1)),
                html.P(children="-")
            ],
            style={'color':color,'background':colors[0], 'margin':'1%','border-radius':'10px'}
        )
    else:
        return html.Div(
            className="two-col",
            children=[
                html.P(className="p-top-bar", children=cellTitle),
                html.P(id=cellTitle, className="display-none", children=cellValue1),
                html.P(children=human_format(cellValue1)),
                html.P(children="(+"+str(cellValue2)+")")
            ],
            style={'color':color,'background':colors[0], 'margin':'1%','border-radius':'10px'}
        )

def get_single_graph(id, x, y, type,title):
    return dcc.Graph(
        id=id,
        className="chart-graph",
        figure={
            'data': [
                {'x': x, 'y': y, 'type':type},
            ],
            'layout': {
                'title': title['title'],
                'xaxis':{"title":title['xtitle']},
                'yaxis':{"title":title['ytitle']},
                'plot_bgcolor': '#1a1c23',
                'paper_bgcolor': '#1a1c23',
                'font': {
                    'color': '#EEE'
                }
            }
        },
    )
    

def get_india_historical():
    india_his = requests.get(URLS['india_his'])
    json_data = india_his.json()["data"]
    data = pd.DataFrame(json_data)
    data['date_of_record'] = pd.to_datetime(data['date_of_record'], unit='ms')
    return dcc.Graph(
        id='india-historical',
        className="chart-graph",
        figure={
            'data': [
                {'x': data['date_of_record'], 'y': data['total_confirmed_cases'], 'type':'line','name':'Confirmed Cases','marker':{'color':'#ffae42'}},
                {'x': data['date_of_record'], 'y': data['total_recovered_cases'], 'type':'line','name':'Recovered Cases','marker':{'color':'#00FF00'}},
                {'x': data['date_of_record'], 'y': data['total_deaths'], 'type':'line','name':'Death Cases','marker':{'color':'#FF0000'}},
            ],
            'layout': {
                'title': "Cases Trend over Time(in Days)",
                'xaxis':{"title":"Date"},
                'yaxis':{"title":"Number of Cases"},
                'plot_bgcolor': '#1a1c23',
                'paper_bgcolor': '#1a1c23',
                'font': {
                    'color': '#EEE'
                }
            }
        },
    )

def get_statewise_rt():
    news_requests = requests.get(URLS['states_rt'])
    if news_requests.json()["status"] == "OK":
        data = news_requests.json()["data"]
        data = pd.DataFrame(data)
        data['last_updated_time'] = pd.to_datetime(data['last_updated_time'], unit='ms')
        data.sort_values(by='active_cases',ascending=False,inplace=True)
        return html.Div(
                children=[
                    html.H6("Statewise Realtime data"),
                    html.P(
                        className="p-news float-right",
                        children="Last update : "
                        + datetime.datetime.now().strftime("%H:%M:%S"),
                    ),
                    html.Table(
                        className="table-news",
                        children=[
                            html.Tr(
                                children=[
                                    html.Th("State/UT"),
                                    html.Th("Active"),
                                    html.Th("Confirmed"),
                                    html.Th("Recovered"),
                                    html.Th("Deaths"),
                                    html.Th("Last Updated")
                                ]
                            )
                            ]+[
                            html.Tr(
                                children=[
                                    html.Td(statecode_dict[row[8]]),
                                    html.Td(row[0]),
                                    html.Td(children=[html.P(children="+"+str(row[4])+" ",className="confirmed-cases"),html.P(row[1])]),
                                    html.Td(children=[html.P(children="+"+str(row[6])+" ",className="recovered-cases"),html.P(row[7])]),
                                    html.Td(children=[html.P(children="+"+str(row[5])+" ",className="death-cases"),html.P(row[2])]),
                                    html.Td(row[3]),
                                ]
                            )
                            for row in data.values
                        ],
                    ),
                ]
            )

search_keywords_list = ['corona%20india','corona%20update%20india','corona%20vaccine','covid%2019%20india%20update','coronavirus%20research',
                        'highest%20corona%20cases%20india']
    
# API Call to update news
def update_news():
    news_requests = requests.get(URLS['news_search']+search_keywords_list[np.random.randint(6)])
    json_data = news_requests.json()["articles"]
    df = pd.DataFrame(json_data)
    df = pd.DataFrame(df[["title", "url"]])
    max_rows = 10
    article_list = list(set([0,1,2] + [np.random.randint(3,high=min(len(df), max_rows)) for i in range(10)]))
    return html.Div(
        children=[
            html.P(className="p-news",children="Headlines"),
            html.P(
                className="p-news float-right",
                children="Last update : "
                + datetime.datetime.now().strftime("%H:%M:%S"),
            ),
            html.Table(
                className="table-news",
                children=[
                    html.Tr(
                        children=[
                            html.Td(
                                children=[
                                    html.A(
                                        className="td-link",
                                        children=df.iloc[i]["title"],
                                        href=df.iloc[i]["url"],
                                        target="_blank",
                                    )
                                ]
                            )
                        ]
                    )
                    for i in article_list
                ],
            ),
        ]
    )

def get_india_data():
    news_requests = requests.get(URLS['india_rt'])
    if news_requests.json()["status"] == "OK":
        data = news_requests.json()["data"][0]
        new_active_cases = int(data['new_confirmed_cases']) - int(data['new_deaths']) - int(data['new_recovered_cases'])
        return [
            get_top_bar_cell("Active Cases", data['active_cases'], new_active_cases, '#0000FF'),
            get_top_bar_cell("Confirmed Cases", data['confirmed_cases'], data['new_confirmed_cases'],'#ffae42'),
            get_top_bar_cell("Deaths", data['deaths'],data['new_deaths'],'#FF0000'),
            get_top_bar_cell("Recovered Cases", data['recovered_cases'],data['new_recovered_cases'],'#00FF00'),
        ]


app.title="COVID-19 India"
app.layout = html.Div(
    className="row",
    children=[
        dcc.Interval(id="i_news", interval=1 * 60000, n_intervals=0),
        dcc.Interval(id="i_india_data", interval=1 * 60000, n_intervals=0),
        dcc.Interval(id="i_india_graph", interval=1 * 60000, n_intervals=0),
        # Left Panel Div
        html.Div(
            className="three columns div-left-panel",
            children=[
                # Div for Left Panel App Info
                html.Div(
                    className="div-info",
                    children=[
                        html.Img(
                            className="logo", src=app.get_asset_url("covid-19-img.png")
                        ),
                        html.H6(className="title-header", children="COVID-19 DASHBOARD"),
                        html.P(
                            """
                            This app is for India's COVID-19 information. It contains and updates India's and Statewise realtime COVID-19 data every one hour. 
                            This app is developed and maintained by Jeevan J.
                            """
                        ),
                    ],
                ),
                # Div for News Headlines
                html.Div(
                    className="div-news",
                    children=[html.Div(id="news", children=update_news())],
                ),
            ],
        ),
        html.Div(
            className="nine columns div-right-panel",
            children=[
                html.H6("COVID-19 India's Realtime data"),
                html.Div(
                    id="top_bar", className="row div-top-bar", children=get_india_data()
                ),
                html.Div(
                    id="india_graphs", className="row", children=get_india_historical()
                ),
                html.Div(
                    id="statewise_data", className="row", children=get_statewise_rt()
                )]
        )
    ],
)

# Callback to update news
@app.callback(Output("news", "children"), [Input("i_news", "n_intervals")])
def update_news_div(n):
    return update_news()

# Callback to update news
@app.callback(Output("top_bar", "children"), [Input("i_india_data", "n_intervals")])
def update_india_data(n):
    return get_india_data()

# Callback to update news
@app.callback(Output("india_graphs", "children"), [Input("i_india_graph", "n_intervals")])
def update_india_data(n):
    return get_india_historical()

if __name__ == "__main__":
    app.run_server(host="0.0.0.0",port=80,debug=False)