import logging
import geofileops as gfo

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    gfo.union(input1_path="data/interim/puma.gpkg", input2_path="data/interim/roads_2010.gpkg", output_path="dev.gpkg")
