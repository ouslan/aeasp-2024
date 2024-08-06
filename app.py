from dash import Dash, html, dcc, Input, Output, State
import pandas as pd
import plotly.express as px
from dash import dash_table
from src.visualization.data_graph import DataGraph

# Initialize the Dash app
app = Dash(__name__, suppress_callback_exceptions=True, meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1.0"}])
app.title = "Road Infrastructure & Its Effects on Commute Time"
server = app.server

# Data initialization
data = DataGraph()
state_codes = pd.read_parquet("data/external/state_codes.parquet").sort_values(by='fips')
state_options = [{'label': state_name, 'value': state_code} for state_name, state_code in zip(state_codes['state_name'], state_codes['fips'])]

# Load the CSV file
all_data = pd.read_csv("data/processed/all.csv")
table_data = all_data[['name', 'coef', 'z_value']]

# Create a list of years for the slider
years = list(range(2012, 2020))

# Define the layout of the app
app.layout = html.Div([
    html.Div(id="header", children=[
        html.H1("Road Infrastructure & Its Effects on Commute Time"),
        dcc.Markdown('''
            #### Overview
            
            This project visualizes the interaction between unemployment and average travel distance by incorporating spatial patterns. 
            The data visualization is done using a Dash web application, which displays a map, project details, and data sources.
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
                
                This project uses two different regressions:
                
                1. **Panel Spatial Regression with Fixed Effects**: Incorporates spatial interaction between neighboring counties. The model used is:

                $$
                y_{it} = \\rho \\sum_{j=1}^N w_{ij} y_{jt}  +  x_{it} \\beta  +  \\mu_i  +  e_{it}
                $$

                ## Results
            ''', mathjax=True),
            dash_table.DataTable(
                id='coefficients-table',
                columns=[
                    {"name": col, "id": col} for col in table_data.columns
                ],
                data=table_data.to_dict('records'),
                style_table={'overflowX': 'auto'},
                style_cell={'textAlign': 'left'}
            )
        ], style={'width': '70%', 'margin': 'auto'})
    elif tab == 'data-tab':
        return html.Div([
            dcc.Markdown('''
                ## The Data
            '''),
            dcc.Markdown('''
                #### Sources

                The data for this project comes from several sources:
                - **TIGER2019**: Shapes for the census PUMAs and for state, as well as historical roads.
                - **Public Use Microdata Areas (PUMAs)**: Contains most control variables.
                - The [TIGER2023](https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/) is used to obtain the shapes for the census block for each individual state.
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
