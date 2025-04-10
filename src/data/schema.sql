-- add postgis extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- create the spatial tables
CREATE TABLE IF NOT EXISTS "states_shp"(
    "state_id" INT,
    "state_abbr" VARCHAR(2) NOT NULL UNIQUE,
    "state_name" TEXT NOT NULL UNIQUE,
    "geometry" GEOMETRY(MULTIPOLYGON,3857) NOT NULL,
    PRIMARY KEY ("state_id")
);

CREATE TABLE IF NOT EXISTS "blocks_shp"(
    "block_id" INT,
    "state_id" INT,
    "block_num" VARCHAR(15) NOT NULL UNIQUE,
    "geometry" GEOMETRY(MULTIPOLYGON,3857) NOT NULL,
    PRIMARY KEY ("block_id")
);

CREATE TABLE IF NOT EXISTS "pumas_shp"(
    "puma_id" INT,
    "state_id" INT,
    "puma_name" TEXT,
    "puma_num" INT NOT NULL UNIQUE,
    "geometry" GEOMETRY(MULTIPOLYGON,3857) NOT NULL
);

CREATE TABLE IF NOT EXISTS "counts_shp"(
    "count_id" INTEGER,
    "name" VARCHAR(6) NOT NULL,
    "geom" GEOMETRY(MULTIPOLYGON,3857) NOT NULL,
    PRIMARY KEY ("count_id")
);

-- create demographic tables
CREATE TABLE IF NOT EXISTS "sex_table"(
    "sex_id" SERIAL PRIMARY KEY,
    "name" VARCHAR(6) NOT NULL UNIQUE
);
INSERT INTO "sex_table" ("name") VALUES ('male'), ('female') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS "race_table"(
    "race_id" SERIAL PRIMARY KEY,
    "name" VARCHAR(8) NOT NULL UNIQUE
);

INSERT INTO "race_table" ("name") VALUES ('hispanic'), ('white'), ('black'), ('native'), ('asian'), ('other') ON CONFLICT DO NOTHING;

-- create data tables

CREATE TABLE IF NOT EXISTS "roads_table"(
    "linear_id" VARCHAR(14) NOT NULL,
    "year" TIMESTAMPTZ NOT NULL,
    "geometry" GEOMETRY(MULTILINESTRING,3857) NOT NULL
);

CREATE TABLE IF NOT EXISTS "acs_table"(
    "year" TIMESTAMPTZ NOT NULL,
    "state_id" INT NOT NULL,
    "puma_id" INT NOT NULL,
    "avg_time" FLOAT NOT NULL,
    "sex_id" INT NOT NULL,
    "race_id" TEXT NOT NULL
);

-- create hypertables

SELECT create_hypertable(
    'roads_table', 
    'year', 
    chunk_time_interval => INTERVAL '1 year', 
    if_not_exists => TRUE
);
