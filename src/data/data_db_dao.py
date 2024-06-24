import os
import psycopg2
from dotenv import load_dotenv
load_dotenv()

class CreateDAO:
    def __init__(self):
        df_user = os.environ.get('POSTGRES_USER')
        df_password = os.environ.get('POSTGRES_PASSWORD')
        df_name = os.environ.get('POSTGRES_DB')
        df_host = os.environ.get('POSTGRES_HOST')
        df_port = os.environ.get('POSTGRES_PORT')
        self.conn = psycopg2.connect(
                                     host=df_host,
                                     database=df_name,
                                     user=df_user,
                                     password=df_password,
                                     port=df_port
)
        self.create_extension()
        self.create_state_tables()
        self.create_blocks_tables()
        self.create_puma_tables()
        self.create_lodes_tables()
        self.create_distance_tables()
        #self.create_movs_tables()
        #self.create_sex_tables()
        #self.create_race_tables()

    def create_extension(self):
        cursor = self.conn.cursor()
        query = """CREATE EXTENSION IF NOT EXISTS postgis;"""
        cursor.execute(query)
        self.conn.commit()


if __name__ == "__main__":
    CreateDAO()