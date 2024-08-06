Certainly! Here’s a README for your project formatted similarly to the provided example:

# Road Infrastructure & it's Effects on Comute Time

This project visualizes the interaction between unemployment and average travel distance by incorporating spatial patterns. The data visualization is done using a Dash web application, which displays a map, project details, and data sources.

## Prerequisites

Before running the project, make sure you have Python 3.9+ installed and set up a virtual environment:

```bash
git clone https://github.com/ouslan/mov
cd mov
```

a `.env` in the root directory with the following content:

```bash
CENSUS_API_KEY=YOUR_API_KEY
```

### With Conda

You can create a new Conda environment using the provided `environment.yml` file:

```bash
conda env create -f environment.yml
```

### With pip

Alternatively, you can install the required libraries with pip:

```bash
pip install -r requirements.txt
```

> [!IMPORTANT]  
> This project uses `polars` as one of its dependencies, which requires Rust to be installed on your system. You can install Rust from the [official Rust website](https://www.rust-lang.org/).

## Usage

To run the Dash application, use the following command:

```bash
docker-compose up
```

This will host the Dash application at `http://0.0.0.0:7058`.

## Docker

You can also run the website locally using Docker. To build and start the Docker containers, run:

```bash
docker-compose up --build
```

This will host the Dash application at [http://localhost:7050](http://localhost:7050) and the documentation at [http://localhost:8005](http://localhost:8005).

## File Structure

- `app.py`: Main application file that sets up the Dash app, defines the layout, callbacks, and runs the server.
- `src/data/data_pull.py`: Contains the `DataPull` class that handles data retrieval.
- `src/data/data_process.py`: Contains the `DataClean` class that handles data loading, cleaning, and processing.
- `src/graphs/data_graph.py`: Contains the `DataGraph` class for processing and visualizing data.

## Data Sources

The data for this project comes from several sources:

-  [**TIGER2019**](https://www2.census.gov/geo/tiger/TIGER2019/TABBLOCK20/): Shapes for the census PUMAS and for state, as well as historical roads.
- [**Public Use Microdata Areas (PUMAs)**](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/pumas.html): Contains most control variables

## usage

You can use the following package to generate all the data of the project:
```bash
python app.py
```


> [!IMPORTANT]  
> It is important to note that this replication will take a while to run. Given my a high end computer with 68GB of RAM and 12 threads, it will take around 22 hours to download and run the project.

> [!WARNING]  
> The data used for this project is very large, be sure to have enough space on your computer to download it. It requires at least 120GB of free space.

> [!CAUTION]
> Given that the there is a recalculation of the road shape files from counties to PUMAs, and it is done in paraller to save time it maya very resorce intensive to run the project.
## Methodology

The project uses two regression models:

1. **OLS Regression**: Estimates coefficients of the MOVS dataset to move data from state level to county level.
2. **Panel Spatial Regression with Fixed Effects**: Incorporates spatial interaction between neighboring counties. The model used is:
   $$y_{it} = \rho \sum_{j=1}^N w_{ij} y_{jt} + x_{it} \beta + \mu_i + e_{it}$$

## Sponsors

<p align="center">
  <a href="https://www.aeaweb.org/">
    <img src='https://upload.wikimedia.org/wikipedia/en/thumb/e/e4/American_Economic_Association_logo.svg/2880px-American_Economic_Association_logo.svg.png' alt="AEA Seal" style="width: 150px; height: auto; margin: 0 10px;" />
  </a>
  <a href="https://www.census.gov/">
    <img src='https://upload.wikimedia.org/wikipedia/commons/c/c1/United_States_Census_Bureau_Wordmark.svg' alt="Census Logo" style="width: 150px; height: auto; margin: 0 10px;" />
  </a>
  <a href="https://howard.edu/">
    <img src='https://upload.wikimedia.org/wikipedia/en/thumb/2/21/Howard_University_logo.svg/2880px-Howard_University_logo.svg.png' alt="Howard University Seal" style="width: 150px; height: auto; margin: 0 10px;" />
  </a>
</p>
