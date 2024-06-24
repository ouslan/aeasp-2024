
-- add postgis extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- create the spatial tables
CREATE TABLE IF NOT EXISTS "states_shp"(
    "state_id" INTEGER
    "geom" GEOMETRY NOT NULL,
    PRIMARY KEY ("state_id")
);

CREATE TABLE IF NOT EXISTS "blocks_shp"(
    "block_id" INT,                    
    "geom" GEOMETRY NOT NULL,
    PRIMARY KEY ("block_id")
);

CREATE TABLE IF NOT EXISTS "pumas_shp"(
    "puma" SERIAL PRIMARY KEY,                    
    "geom" GEOMETRY NOT NULL
);

CREATE TABLE IF NOT EXISTS "counts_shp"(
    "count_id" INTEGER,
    "name" VARCHAR(6) NOT NULL,
    "geom" GEOMETRY NOT NULL,
    PRIMARY KEY ("count_id")
);

-- create demographic tables
CREATE TABLE IF NOT EXISTS "sex_table"(
    "sex_id" SERIAL PRIMARY KEY,
    "name" VARCHAR(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS "race_table"(
    "race_id" SERIAL PRIMARY KEY,
    "name" VARCHAR(6) NOT NULL
);

-- create raw data tables
CREATE TABLE IF NOT EXISTS "ledes_table"(                    
    "lodes_id" INTEGER,
    "year" TIMESTAMPTZ NOT NULL,
    "data" FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS "distance_table"(                    
    "distance_id" SERIAL PRIMARY KEY,
    "year" TIMESTAMPTZ NOT NULL,
    "data" FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS "moves_table"(
    "moves_id" SERIAL PRIMARY KEY,
);