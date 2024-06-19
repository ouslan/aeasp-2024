    
class DataLoad:     
    
    def load_mov_data(self) -> pl.DataFrame:
        if not os.path.exists(self.mov_file_path):
            self.retrieve_file(self.mov_file_url, self.mov_file_path)
        return pl.read_csv(self.mov_file_path, ignore_errors=True)
    
    def load_shape_data(self):
        if not os.path.exists(self.shape_file_path):
            self.retrieve_file(self.shape_file_url, self.shape_file_path)
        shp = gpd.read_file(self.shape_file_path, engine="pyogrio")
        shp.rename({"GEOID": "state", "NAME": "state_name"}, axis=1, inplace=True)
        shp["state"] = shp["state"].astype(int)
        return shp
    
    def load_state_codes(self) -> pl.DataFrame:
        if not os.path.exists(self.state_code_file_path):
            codes = self.mov.select(pl.col("state_abbr").str.to_lowercase().unique())
            codes = codes.filter(pl.col("state_abbr") != "us")
            codes = codes.join(self.mov.with_columns(pl.col("state_abbr").str.to_lowercase()), on="state_abbr", how="inner")
            codes = codes.select(pl.col("state_abbr", "fips", "state_name")).unique()
            codes.write_parquet(self.state_code_file_path)
        else:
            codes = pl.read_parquet(self.state_code_file_path)
        return codes
    
    def load_blocks_data(self) -> pl.DataFrame:

        if not os.path.exists(self.blocks_file_path):
            empty_df = [
                pl.Series("STATEFP20", [], dtype=pl.String),
                pl.Series("GEOID20", [], dtype=pl.String),
                pl.Series("lon", [], dtype=pl.Float64),
                pl.Series("lat", [], dtype=pl.Float64),
            ]
            blocks = pl.DataFrame(empty_df).clear()
            self.retrieve_shps(blocks)
        else:
            blocks = pl.read_parquet(self.blocks_file_path).unique()
        return blocks
    
    def load_lodes_data(self):

        if not os.path.exists(self.lodes_file_path):
            empty_df = [
                pl.Series("state", [], dtype=pl.String),
                pl.Series("fips", [], dtype=pl.String),
                pl.Series("state_abbr", [], dtype=pl.String),
                pl.Series("year", [], dtype=pl.Int64),
                pl.Series("avg_distance", [], dtype=pl.Float64),
            ]
            lodes = pl.DataFrame(empty_df).clear()
            self.retrieve_lodes(lodes)
        else:
            lodes = pl.read_parquet(self.lodes_file_path)
        return lodes