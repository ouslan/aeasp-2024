from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import create_engine
from dotenv import load_dotenv
import geopandas as gpd
import psycopg2
import os
load_dotenv()

class DAO:
    def __init__(self):
        db_user = os.environ.get('POSTGRES_USER')
        db_password = os.environ.get('POSTGRES_PASSWORD')
        db_name = os.environ.get('POSTGRES_DB')
        db_host = os.environ.get('POSTGRES_HOST')
        db_port = os.environ.get('POSTGRES_PORT')
        self.conn2 = con = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
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
        self.insert_pumas()
        self.insert_blocks()
        
    
    def insert_blocks(self):
        id_count = 0
        for file in os.listdir('data/shape_files/'):
            if file.endswith('.zip') and file.startswith('block_'):
                gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                gdf.rename(columns={"STATEFP20": "state_id", "GEOID20": "block_num"}, inplace=True)
                gdf["block_id"] = gdf.index.values + 1 + id_count
                gdf = gdf.astype({'state_id': 'int64'}).set_crs(3857, allow_override=True).sort_values(by='block_num').reset_index(drop=True)
                gdf = gdf[["block_id", "state_id", "block_num", "geometry"]]
                gdf.to_postgis(name='blocks_shp', con=self.conn2, if_exists='append', index=False, dtype={'geometry': Geometry('GEOMETRY', srid=3857)})
                id_count += len(gdf)
                print("\033[0;36mPROCESS: \033[0m" + f"Finished inserting {file} blocks")

    def insert_pumas(self):
        id_count = 0
        for file in os.listdir('data/shape_files/'):
            if file.endswith('.zip') and file.startswith('puma_'):
                gdf = gpd.read_file(f"data/shape_files/{file}", engine="pyogrio")
                gdf.rename(columns={"STATEFP20": "state_id", "GEOID20": "puma_num", "NAMELSAD20": "puma_name"}, inplace=True)
                gdf = gdf.astype({'state_id': 'int64'}).set_crs(3857, allow_override=True).sort_values(by='puma_num').reset_index(drop=True)
                gdf["puma_id"] = gdf.index.values + 1 + id_count
                gdf = gdf[["puma_id", "state_id","puma_num", "puma_name", "geometry"]]
                gdf.to_postgis(name='pumas_shp', con=self.conn2, if_exists='append', index=False, dtype={'geometry': Geometry('GEOMETRY', srid=3857)})
                id_count += len(gdf)
                print("\033[0;36mPROCESS: \033[0m" + f"Finished inserting {file} blocks")

if __name__ == "__main__":
    CreateDAO()