from dash import Dash, html, dcc, Input, Output, State
import pandas as pd
from src.visualization.data_graph import DataGraph  # Assuming DataGraph is a custom class
import plotly.express as px

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=["https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css"], suppress_callback_exceptions=True)

# Assuming this initializes your data using DataGraph
data = DataGraph()
state_codes = pd.read_parquet("data/external/state_codes.parquet")
state_options = [{'label': state_name, 'value': state_code} for state_name, state_code in zip(state_codes['state_name'], state_codes['fips'])]

# Define the layout of the app
app.layout = html.Div([
    dcc.Tabs(id='tabs-nav', value='map-tab', children=[
        dcc.Tab(label='Map', value='map-tab'),
        dcc.Tab(label='About', value='about-tab'),
        dcc.Tab(label='Data', value='data-tab')
    ]),
    html.Div(id='tabs-content')
])

# Define callbacks for each tab
@app.callback(
    Output('tabs-content', 'children'),
    [Input('tabs-nav', 'value')]
)
def render_content(tab):
    if tab == 'map-tab':
        return html.Div([
            html.H1(children='Average Travel Time by State and PUMA'),

            html.Div(children='''
                
            '''),

            html.Div([
                dcc.Dropdown(
                    id='state-dropdown',
                    options=state_options,
                    value=state_codes.iloc[0]['fips']
                ),
                dcc.Dropdown(
                    id='sex-dropdown',
                    options=[
                        {'label': 'Male', 'value': 1},
                        {'label': 'Female', 'value': 2},
                        {'label': 'All', 'value': 3}
                    ],
                    value=3
                ),
                dcc.Dropdown(
                    id='race-dropdown',
                    options=[
                        {'label': 'American Indian', 'value': 'RACAIAN'},
                        {'label': 'Asian', 'value': 'RACASN'},
                        {'label': 'Black', 'value': 'RACBLK'},
                        {'label': 'Native Hawaiian', 'value': 'RACNUM'},
                        {'label': 'White', 'value': 'RACWHT'},
                        {'label': 'Some Other Race', 'value': 'RACSOR'},
                        {'label': 'Hispanic', 'value': 'HISP'},
                        {'label': 'All', 'value': 'ALL'}
                    ],
                    value='ALL'
                ),
                html.Button('Update Graph', id='update-graph-btn')
            ], style={'width': '50%', 'margin': 'auto', 'text-align': 'center', 'padding': '10px'}),

            dcc.Graph(
                id='map-graph',
                style={'width': '150vh', 'height': '90vh'}
            )
        ])
    elif tab == 'about-tab':
        return html.Div([
            html.Div([
                dcc.Markdown('''
                    ## About the Project
                    '''
                )
            ], style={'padding': 10, 'text-align': 'center'}),
            html.Div([
                dcc.Markdown('''
                    
                    #### Introduction
                    
                    This is my research for the AEA Summer Program in collaboration with the Census. This project consists of measuring the interaction with unemployment and
                    average travel distance incorporating spatial patterns. 
                    
                    #### Methodology
                    
                    This project uses two different regressions. The first one is an OLS regression to estimate the coefficients of the MOVS dataset. This is with the objective
                    of moving the data from state level to county level. The second one is the main regression used for the research. I used a Panel spatial regression with fixed effects model.
                    We incorporate space into our regression as it gives us a way to measure the interaction between neighboring counties with each other. The model used is stated as follows:
                    
                    $$
                    y_{it} = \\rho \\sum_{j=1}^N w_{ij} y_{jt}  +  x_{it} \\beta  +  \\mu_i  +  e_{it}
                    $$
                    ''',
                    mathjax=True),
            ]),
        ], style={'width': '70%', 'margin': 'auto'})
    elif tab == 'data-tab':
        return html.Div([
            html.Div([
                dcc.Markdown('''
                    ## The Data
                    '''
                )
            ], style={'padding': 10, 'text-align': 'center'}),
            html.Div([
                dcc.Markdown('''
                    #### Sources

                    The data for this project is obtained from multiple sources, including:
                    - LEHD Origin-Destination Employment Statistics ([LEHD](https://lehd.ces.census.gov/data/lodes/LODES8/)): This dataset
                    contains the data for origin and destination of people by census block.
                    - The Mobility, Opportunity, and Volatility Statistics ([MOVS](https://www.census.gov/programs-surveys/ces/data/public-use-data/mobility-opportunity-volatility-statistics.html)):
                    This dataset contains the average income by race and gender.
                    - The [GENZ2018](https://www2.census.gov/geo/tiger/GENZ2018/shp/) is used to obtain the shapes for the states to make the map.
                    - The [TIGER2023](https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/) is used to obtain the shapes for the census block for each individual state.
                    - Data Profile ACS 3-year estimates (DP03): From this dataset comes most of the control variables and the variables used to create the MOVS dataset
                    at a county level.
                    '''
                ),
            ]),
        ], style={'width': '70%', 'margin': 'auto'})

# Callback to update the map graph based on the selected options
@app.callback(
    Output('map-graph', 'figure'),
    [Input('update-graph-btn', 'n_clicks')],
    [State('state-dropdown', 'value'),
     State('sex-dropdown', 'value'),
     State('race-dropdown', 'value')]
)
def update_figure(n_clicks, state, sex, race):
    df = data.graph(state, sex, race)  # Update dataframe based on the selected options
    fig = px.choropleth_mapbox(df,
                                geojson=df.geometry,
                                locations=df.index,
                                color="avg_time",
                                center={"lat": 37.0902, "lon": -95.7129},
                                mapbox_style="carto-positron",
                                range_color=[0, 45],
                                color_continuous_scale="Viridis",
                                zoom=3)
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=7050)
