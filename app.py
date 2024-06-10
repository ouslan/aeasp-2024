from dash import Dash, html, dcc, Input, Output, callback
from src.data.data_process import DataClean
import plotly.express as px

app = Dash(__name__)

data = DataClean()
df = data.graph(2016)

app.layout = html.Div(children=[
    html.H1(children='Mobility, Opportunity, and Volatility Statistics (MOVS)'),

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
    ),
])

@app.callback(
    Output('map-graph', 'figure'),
    [Input('year-slider', 'value')]
)
def update_figure(selected_year):
    df = data.graph(selected_year)  # Update dataframe based on selected year
    fig = px.choropleth_mapbox(df,
                                geojson=df.geometry,
                                locations=df.index,
                                hover_name=df.state_name,
                                color="avg_distance",
                                center={"lat": 37.0902, "lon": -95.7129},
                                mapbox_style="carto-positron",
                                zoom=2.5)
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
