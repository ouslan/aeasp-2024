from dash import Dash, html, dcc, Input, Output
from src.data.data_process import DataClean
import plotly.express as px

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=["https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css"], suppress_callback_exceptions=True)

# Data processing
data = DataClean()
df = data.graph(2016)

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
            html.H1(children='Average Travel Distance by State'),

            html.Div(children='''
                Income and Household Measures of Working-Age Adults: 2005-2015
            '''),

            dcc.Slider(
                min=data.lodes['year'].min(),
                max=data.lodes['year'].max(),
                step=None,
                value=data.df['year'].max(),
                marks={str(year): str(year) for year in data.df['year'].unique()},
                id="year-slider"
            ),

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

# Callback to update the map graph based on the selected year
@app.callback(
    Output('map-graph', 'figure'),
    [Input('year-slider', 'value')]
)
def update_figure(selected_year):
    df = data.graph(selected_year)  # Update dataframe based on the selected year
    fig = px.choropleth_mapbox(df,
                                geojson=df.geometry,
                                locations=df.index,
                                hover_name=df.state_name,
                                color="avg_distance",
                                center={"lat": 37.0902, "lon": -95.7129},
                                mapbox_style="carto-positron",
                                zoom=2.5)
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=False, host="0.0.0.0", port=7050)
