from dash import Dash, html, dcc, Input, Output, State
import pandas as pd
import plotly.express as px
from src.visualization.data_graph import DataGraph

# Initialize the Dash app
app = Dash(__name__, suppress_callback_exceptions=True, meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1.0"}])
app.title = "Average Travel Time by State and PUMA"
server = app.server

# Data initialization
data = DataGraph()
state_codes = pd.read_parquet("data/external/state_codes.parquet").sort_values(by='fips')
state_options = [{'label': state_name, 'value': state_code} for state_name, state_code in zip(state_codes['state_name'], state_codes['fips'])]

# Create a list of years for the slider
years = list(range(2012, 2020))

# Define the layout of the app
app.layout = html.Div([
    html.Div(id="header", children=[
        html.H1("Average Travel Time by State and PUMA"),
        dcc.Markdown('''
            #### Introduction
            
            This is my research for the AEA Summer Program in collaboration with the Census. This project consists of measuring the interaction with unemployment and
            average travel distance incorporating spatial patterns.
        '''),
        dcc.Tabs(id='tabs-nav', value='map-tab', children=[
            dcc.Tab(label='Map', value='map-tab'),
            dcc.Tab(label='About', value='about-tab'),
            dcc.Tab(label='Data', value='data-tab')
        ]),
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
                dcc.Dropdown(
                    id='mode-dropdown',
                    options=[
                        {'label': 'Car', 'value': 'car'},
                        {'label': 'Bus', 'value': 'bus'},
                        {'label': 'Streetcar', 'value': 'streetcar'},
                        {'label': 'Subway', 'value': 'subway'},
                        {'label': 'Railroad', 'value': 'railroad'},
                        {'label': 'Ferry', 'value': 'ferry'},
                        {'label': 'Taxi', 'value': 'taxi'},
                        {'label': 'Motorcycle', 'value': 'motorcycle'},
                        {'label': 'Bicycle', 'value': 'bicycle'},
                        {'label': 'Walking', 'value': 'walking'}
                    ],
                    value='car'
                ),
                dcc.Slider(
                    id='year-slider',
                    min=min(years),
                    max=max(years),
                    step=1,
                    marks={year: str(year) for year in years},
                    value=min(years)
                ),
                html.Button('Update Graph', id='update-graph-btn', n_clicks=0)
            ], style={'width': '50%', 'margin': 'auto', 'text-align': 'center', 'padding': '10px'}),

            dcc.Graph(
                id='map-graph',
                style={'width': '100%', 'height': '80vh'}
            )
        ])
    elif tab == 'about-tab':
        return html.Div([
            dcc.Markdown('''
                ## About the Project
            '''),
            dcc.Markdown('''
                #### Methodology
                
                This project uses two different regressions. The first one is an OLS regression to estimate the coefficients of the MOVS dataset. This is with the objective
                of moving the data from state level to county level. The second one is the main regression used for the research. I used a Panel spatial regression with fixed effects model.
                We incorporate space into our regression as it gives us a way to measure the interaction between neighboring counties with each other. The model used is stated as follows:
                
                $$
                y_{it} = \\rho \\sum_{j=1}^N w_{ij} y_{jt}  +  x_{it} \\beta  +  \\mu_i  +  e_{it}
                $$
            ''', mathjax=True)
        ], style={'width': '70%', 'margin': 'auto'})
    elif tab == 'data-tab':
        return html.Div([
            dcc.Markdown('''
                ## The Data
            '''),
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
            '''),
        ], style={'width': '70%', 'margin': 'auto'})

# Callback to update the map graph based on the selected options
@app.callback(
    Output('map-graph', 'figure'),
    [Input('update-graph-btn', 'n_clicks')],
    [State('state-dropdown', 'value'),
     State('sex-dropdown', 'value'),
     State('race-dropdown', 'value'),
     State('mode-dropdown', 'value'),
     State('year-slider', 'value')]
)
def update_figure(n_clicks, state, sex, race, mode, year):
    df = data.graph(state, sex, race)
    df = df[df['year'] == year]  # Filter data for the selected year
    
    fig = px.choropleth_mapbox(df,
                                geojson=df.geometry,
                                locations=df.index,
                                color=mode,
                                center={"lat": 37.0902, "lon": -95.7129},
                                mapbox_style="carto-positron",
                                color_continuous_scale="Viridis",
                                zoom=3)
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=7050)
