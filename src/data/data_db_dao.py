from src.data.data_process import DataProcess
from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry
from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
import psycopg2
import os

load_dotenv()

class DAO(DataProcess):

    def __init__(self):
        super().__init__()
        db_user = os.environ.get('POSTGRES_USER')
        db_password = os.environ.get('POSTGRES_PASSWORD')
        db_name = os.environ.get('POSTGRES_DB')
        db_host = os.environ.get('POSTGRES_HOST')
        db_port = os.environ.get('POSTGRES_PORT')
        self.conn2 = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        self.conn = psycopg2.connect(
                                     host=db_host,
                                     database=db_name,
                                     user=db_user,
                                     password=db_password,
                                     port=db_port)
        with open('src/data/schema.sql', 'r') as file:
            sql_query = file.read()
        cursor = self.conn.cursor()
        cursor.execute(sql_query)
        self.conn.commit()
        #self.insert_states()
        #self.insert_pumas()
        #self.insert_blocks()
        #self.insert_roads()
        self.insert_acs()

    def data_exists(self, table_name):
        with self.conn2.connect() as con:
            result = con.execute(text(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1);"))
            return result.scalar()

    def insert_states(self):
        if not self.data_exists('states_shp'):
            gdf = gpd.read_file("data/shape_files/states.zip", engine="pyogrio")
            gdf.rename(columns={"STATEFP": "state_id", "STUSPS": "state_abbr", "NAME": "state_name"}, inplace=True)
            gdf = gdf[["state_id", "state_abbr", "state_name", "geometry"]].astype({"state_id": "int64"}).set_crs(3857, allow_override=True)
            gdf.to_postgis(
                           name='states_shp', 
                           con=self.conn2, 
                           if_exists='append', 
                           chunksize=5000, 
                           dtype={'geometry': Geometry('GEOMETRY', srid=3857)})
            print("\033[0;36mPROCESS: \033[0m" + f"Finished inserting states")
        else:
            print("\033[0;36mPROCESS: \033[0m" + "States data already exists")

    def insert_blocks(self):
        if not self.data_exists('blocks_shp'):
            id_count = 0
            for file in os.listdir('data/shape_files/'):
                if file.endswith('.zip') and file.startswith('block_'):
                    gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                    gdf.rename(columns={"STATEFP20": "state_id", "GEOID20": "block_num"}, inplace=True)
                    gdf["block_id"] = gdf.index.values + 1 + id_count
                    gdf = gdf.astype({'state_id': 'int64'}).set_crs(3857, allow_override=True).sort_values(by='block_num').reset_index(drop=True)
                    gdf = gdf[["block_id", "state_id", "block_num", "geometry"]]
                    gdf.to_postgis(
                                   name='blocks_shp', 
                                   con=self.conn2, 
                                   if_exists='append', 
                                   chunksize=5000, 
                                   dtype={'geometry': Geometry('GEOMETRY', srid=3857)})
                    id_count += len(gdf)
                    print("\033[0;36mPROCESS: \033[0m" + f"Finished inserting {file} blocks")
        else:
            print("\033[0;36mPROCESS: \033[0m" + "Blocks data already exists")

    def insert_pumas(self):
        if not self.data_exists('pumas_shp'):
            id_count = 0
            for file in os.listdir('data/shape_files/'):
                if file.endswith('.zip') and file.startswith('puma_'):
                    gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                    gdf.rename(columns={"STATEFP10": "state_id", "GEOID10": "puma_num", "NAMELSAD10": "puma_name"}, inplace=True)
                    gdf = gdf.astype({'state_id': 'int64'}).set_crs(3857, allow_override=True).sort_values(by='puma_num').reset_index(drop=True)
                    gdf["puma_id"] = gdf.index.values + 1 + id_count
                    gdf = gdf[["puma_id", "state_id","puma_num", "puma_name", "geometry"]]
                    gdf.to_postgis(
                            name='pumas_shp', 
                            con=self.conn2, if_exists='append', 
                            chunksize=5000, 
                            dtype={'geometry': Geometry('GEOMETRY', srid=3857)})
                    id_count += len(gdf)
                    print("\033[0;36mPROCESS: \033[0m" + f"Finished inserting {file} pumas")
        else:
            print("\033[1;33mWARNING: \033[0m" + "Pumas data already exists")
    
    def insert_roads(self):
        if not self.data_exists('roads_table'):
            for file in os.listdir('data/shape_files/'):
                if file.startswith('roads'):
                    year = file.split('_')[1]
                    gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                    gdf.rename(columns={"LINEARID": "linear_id"}, inplace=True)
                    gdf["year"] = pd.to_datetime(year, format='%Y')
                    gdf = gdf[["linear_id", "year", "geometry"]].set_crs(3857, allow_override=True)
                    gdf.to_postgis(
                                   name='roads_table', 
                                   con=self.conn2, 
                                   if_exists='append', 
                                   chunksize=1000, 
                                   dtype={'geometry': Geometry('GEOMETRY', srid=3857)})
                    print("\033[0;36mPROCESS: \033[0m" + f"Finished inserting {file} roads")

        else:
            print("\033[0;36mPROCESS: \033[0m" + "Roads data already exists")
    
    def insert_acs(self):
        if not self.data_exists('acs_table'):
            df = self.process_acs()
            df = df.rename({"state": "state_id", "PUMA": "puma_id", "race": "race_id", "sex": "sex_id"})
            df.write_database(
                table_name="acs_table",  
                connection=self.conn2,
                if_table_exists="append",
            )

if __name__ == "__main__":
    DAO()