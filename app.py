from dash import Dash, html, dcc
import plotly.express as px
import geopandas as gpd
import pandas as pd
import polars as pl

app = Dash(__name__)

df = gpd.read_file(open("dev.geojson"))
df2 = pl.read_csv("data/raw/movs_st_main2005.csv", ignore_errors=True)


fig = px.choropleth_mapbox(df,
                           geojson=df.geometry,
                           locations=df.index,
                           color="avg_meqinc",
                           center={"lat": 37.0902, "lon": -95.7129},
                           mapbox_style="carto-positron",
                           hover_name=df.NAME,
                           zoom=2.5)

app.layout = html.Div(children=[
    html.H1(children='Mobility, Opportunity, and Volatility Statistics (MOVS)'),

    html.Div(children='''
        Income and Household Mesures of Working-Age Adults:2005-2015
    '''),

    html.Br(),
    html.Label('Sex'),
        dcc.Dropdown(['All', 'Male', 'Female'], "All"),
 
    html.Br(),
    html.Label('Race/Ethnicity'),
        dcc.Dropdown(['All', 'Hispanic, any race', 'White non-Hispanic', 'American-Indian and Alaska Native, Non-Hispanic', 'Asian, Non-Hispanic', 'Some other Race'], multi=True),

    html.Br(),
    html.Label('Income Decile in 2005'),
        dcc.Dropdown(['All', 'First (lowest income decile)', 'Second', "third", "Fourth", "Fifth", "Sixth", "Seventh", "Eighth", "Nineth", "Tenth (highest income decile)"], "All"),
    
    html.Br(),
    dcc.Slider(
        df2['year'].min(),
        df2['year'].max(),
        step=None,
        id='year--slider',
        value=df2['year'].max(),
        marks={str(year): str(year) for year in df2['year'].unique()},
    ),
    
    html.Br(),
    dcc.Graph(
        id='example-graph',
        figure=fig,
        style={'width': '150vh', 'height': '90vh'}
    ),
])

if __name__ == '__main__':
    app.run(debug=True)
