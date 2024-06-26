-- add postgis extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- create the spatial tables
CREATE TABLE IF NOT EXISTS "states_shp"(
    "state_id" INTEGER,
    "geometry" GEOMETRY(MULTIPOLYGON,3857) NOT NULL,
    PRIMARY KEY ("state_id")
);

CREATE TABLE IF NOT EXISTS "blocks_shp"(
    "block_id" INT,
    "block_num" VARCHAR(21),
    "state_id" INT,        
    "geometry" GEOMETRY(MULTIPOLYGON,3857) NOT NULL,
    PRIMARY KEY ("block_id")
);

CREATE TABLE IF NOT EXISTS "pumas_shp"(
    "puma" SERIAL PRIMARY KEY,                    
    "geom" GEOMETRY(POLYGON,3857) NOT NULL
);

CREATE TABLE IF NOT EXISTS "counts_shp"(
    "count_id" INTEGER,
    "name" VARCHAR(6) NOT NULL,
    "geom" GEOMETRY(POLYGON,3857) NOT NULL,
    PRIMARY KEY ("count_id")
);

-- create demographic tables
CREATE TABLE IF NOT EXISTS "sex_table"(
    "sex_id" SERIAL PRIMARY KEY,
    "name" VARCHAR(6) NOT NULL
);
INSERT INTO "sex_table" ("name") VALUES ('male'), ('female');

CREATE TABLE IF NOT EXISTS "race_table"(
    "race_id" SERIAL PRIMARY KEY,
    "name" VARCHAR(8) NOT NULL
);

INSERT INTO "race_table" ("name") VALUES ('hispanic'), ('white'), ('black'), ('native'), ('asian'), ('other');