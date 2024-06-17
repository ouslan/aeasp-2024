## README

### Project Overview
This project visualizes the interaction between unemployment and average travel distance by incorporating spatial patterns. The data visualization is done using a Dash web application, which displays a map, project details, and data sources.

### Requirements
To run this project, ensure you have the following installed:

- Python 3.7+
- dash
- plotly
- pandas
- geopandas
- polars
- numpy

You can install the necessary packages using:
```bash
pip install -r requirements.txt
```

### File Structure
- `app.py`: Main application file that sets up the Dash app, defines the layout, callbacks, and runs the server.
- `src/data/data_process.py`: Contains the `DataClean` class that handles data loading, cleaning, and processing.

### Data Sources
The data for this project comes from several sources:
- **LEHD Origin-Destination Employment Statistics** ([LEHD](https://lehd.ces.census.gov/data/lodes/LODES8/)): Data for origin and destination of people by census block.
- **Mobility, Opportunity, and Volatility Statistics** ([MOVS](https://www.census.gov/programs-surveys/ces/data/public-use-data/mobility-opportunity-volatility-statistics.html)): Average income by race and gender.
- **GENZ2018** ([GENZ2018](https://www2.census.gov/geo/tiger/GENZ2018/shp/)): Shapes for the states to make the map.
- **TIGER2023** ([TIGER2023](https://www2.census.gov/geo/tiger/TIGER2023/TABBLOCK20/)): Shapes for the census block for each state.
- **Data Profile ACS 3-year estimates (DP03)**: Contains most control variables and variables used to create the MOVS dataset at the county level.

### Usage

1. **Data Preparation**
   The `DataClean` class in `data_process.py` handles the downloading, cleaning, and processing of the necessary data files. When instantiated, it will automatically load or download the required datasets.
   
2. **Running the App**
   To run the Dash app, execute:
   ```bash
   docker compose up
   ```
   The app will be available at `http://0.0.0.0:7050`.

### Application Layout
The app has three main tabs:
1. **Map Tab**: Displays a choropleth map showing the average travel distance by state.
2. **About Tab**: Provides an introduction to the project and details on the methodology.
3. **Data Tab**: Lists the data sources used in the project.

### Callbacks
The app includes callbacks to handle interactions:
- **Tab Navigation**: Updates the content based on the selected tab.
- **Year Slider**: Updates the map based on the selected year.

### Data Processing
The `DataClean` class processes the data in several steps:
1. **Load MOV Data**: Downloads and loads the MOVS data.
2. **Load Shape Data**: Downloads and loads the shape data.
3. **Load State Codes**: Loads or generates state codes.
4. **Load Blocks Data**: Downloads and processes the census blocks data.
5. **Load LODES Data**: Downloads and processes the LODES data.
6. **Create Graph Dataset**: Merges the LODES and shape data for graphing.

### Methodology
The project uses two regression models:
1. **OLS Regression**: Estimates coefficients of the MOVS dataset to move data from state level to county level.
2. **Panel Spatial Regression with Fixed Effects**: Incorporates spatial interaction between neighboring counties. The model used is:
   \[
   y_{it} = \rho \sum_{j=1}^N w_{ij} y_{jt} + x_{it} \beta + \mu_i + e_{it}
   \]

### Contact
For any queries, feel free to contact the project maintainer.