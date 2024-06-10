from dash import Dash, html, dcc, Input, Output
from src.data.data_process import DataClean
import plotly.express as px

app = Dash(__name__, external_stylesheets=["https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css"], suppress_callback_exceptions=True)

data = DataClean()
df = data.graph(2016)

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
    Input('tabs-nav', 'value')
)
def render_content(tab):
    if tab == 'map-tab':
        return html.Div([
            html.H1(children='Average travel distance by State'),

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
            
            html.H2(children="Income and Household Measures of Working-Age Adults: 2005-2015"),

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
                    
                    #### Source
                    
                    Project Gutenberg offers [mirroring via rsync](https://www.gutenberg.org/help/mirroring.html). However, in June 2016, [Allison Parrish](https://www.decontextualize.com/) released a [corpus](https://github.com/aparrish/gutenberg-dammit) of all text files and metadata up to that point in time, which was used here instead of the raw data. 
                    
                    #### Transformation
                    
                    After unpacking the JSON metadata into a table and mapping "?" to NULL, the books could now be inserted, and split using double newline into paragraphs (5 books were further split manually due to still exceeding the [limit for tsvector](https://www.postgresql.org/docs/current/textsearch-limitations.html), which here was around 720,000 characters). 
                    
                    There are some logical inconsistencies in the data such as authors having different birth dates per book, which are left alone for now. Without any information outside the texts and metadata, particularly popularity and academic importance today, search may be less relevant. Some famous quotes occur more often in derivative works than the original. 
                    
                    #### Search
                    
                    The search uses the language in metadata to phraseto_query the indexed column to preserve ordering of the lexemes, which gave far more precise and less numerous results. [Textsearch headline](https://www.postgresql.org/docs/11/textsearch-controls.html#TEXTSEARCH-HEADLINE) with MaxFragments=1000 was used to generate a quote around matched terms which were aggregated per book with the ts_rank_cd averaged across all matched paragraphs. 
                
                    '''
                ),
            ]),
        ], style={'width': '70%', 'margin': 'auto'})
    elif tab == 'data-tab':
        return html.Div([
            html.Div([
                dcc.Markdown('''
                    ## The data
                    '''
                )
            ], style={'padding': 10, 'text-align': 'center'}),
            html.Div([
                dcc.Markdown('''
                    
                    #### Source
                    
                    Project Gutenberg offers [mirroring via rsync](https://www.gutenberg.org/help/mirroring.html). However, in June 2016, [Allison Parrish](https://www.decontextualize.com/) released a [corpus](https://github.com/aparrish/gutenberg-dammit) of all text files and metadata up to that point in time, which was used here instead of the raw data. 
                    
                    #### Transformation
                    
                    After unpacking the JSON metadata into a table and mapping "?" to NULL, the books could now be inserted, and split using double newline into paragraphs (5 books were further split manually due to still exceeding the [limit for tsvector](https://www.postgresql.org/docs/current/textsearch-limitations.html), which here was around 720,000 characters). 
                    
                    There are some logical inconsistencies in the data such as authors having different birth dates per book, which are left alone for now. Without any information outside the texts and metadata, particularly popularity and academic importance today, search may be less relevant. Some famous quotes occur more often in derivative works than the original. 
                    
                    #### Search
                    
                    The search uses the language in metadata to phraseto_query the indexed column to preserve ordering of the lexemes, which gave far more precise and less numerous results. [Textsearch headline](https://www.postgresql.org/docs/11/textsearch-controls.html#TEXTSEARCH-HEADLINE) with MaxFragments=1000 was used to generate a quote around matched terms which were aggregated per book with the ts_rank_cd averaged across all matched paragraphs. 
                
                    '''
                ),
            ]),
        ], style={'width': '70%', 'margin': 'auto'})

# Callback to update the map graph based on selected year
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
    app.run_server()
