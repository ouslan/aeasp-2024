import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

class tables_DAO:
    def __init__(self):
        df_user = os.environ.get('POSTGRES_USER')
        df_password = os.environ.get('POSTGRES_PASSWORD')
        df_name = os.environ.get('POSTGRES_DB')
        df_host = os.environ.get('POSTGRES_HOST')
        df_port = os.environ.get('POSTGRES_PORT')
        connection_url = (f"dbname={df_name} " f"user={df_user} " f"password={df_password} " 
             f"host={df_port} " 
             f"port={df_port}")
        self.conn = psycopg2.connect(connection_url)

    def create_state_tables(self):
        cursor = self.conn.cursor()
        query = """CREATE TABLE IF NOT EXISTS states(
                       id serial primary key,
                       state_abbr varchar(2) NOT NULL,
                       state_name varchar(100) NOT NULL,
                       geom geometry NOT NULL
                    );
                """
        cursor.execute(query,)
        self.conn.commit()

    def create_block_tables(self):
        cursor = self.conn.cursor()
        query = """CREATE TABLE IF NOT EXISTS blocks(
                       block_id serial primary key,
                       
                       states foreign key (state_name) references states(state_name),
                       geom geometry NOT NULL
                    );
                """
        cursor.execute(query,)
        self.conn.commit()
    
    def create_lodes_tables(self):
        cursor = self.conn.cursor()
        query = """CREATE TABLE IF NOT EXISTS ledes(
                       
                       state_name foreign key (state_name) references states(state_name),
                   );
                """
        cursor.execute(query,)
        self.conn.commit()

    def create_movs_tables(self):
        cursor = self.conn.cursor()
        query = """CREATE TABLE IF NOT EXISTS ledes(
                    block_id serial primary key,
                    state_name foreign key (state_name) references states(state_name),
                    exports FLOAT NOT NULL,
                    imports FLOAT NOT NULL,
                    net_value FLOAT NOT NULL
            );
            """
        cursor.execute(query,)
        self.conn.commit()

    def create_hypertable(self):
        cursor = self.conn.commit()
        query = """select create_hypertable(
            'ledes',
            interval '1 year'
        )
        """
        cursor.execute(query,)
        self.conn.commit()

if __name__ == "__main__":
    dao = tables_DAO()
    dao.create_imp_exp_tables()