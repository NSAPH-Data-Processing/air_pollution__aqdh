import duckdb
import pyarrow.parquet as pq
import hydra

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    conn = duckdb.connect()

    input_path = f"{cfg.datapaths.base_path}/output/daily/air_pollution__aqdh__{cfg.shp_id}_daily__{cfg.yyyy}.parquet"
    output_filename = f"{cfg.datapaths.base_path}/output/yearly/air_pollution__aqdh__{cfg.shp_id}_yearly__{cfg.yyyy}.parquet"
    
    # Averge the daily values to yearly values
    #obtain the column names using pyarrow metadata
    colnames = pq.read_metadata(input_path).schema.names
    colnames = [col for col in colnames if col not in ["date", cfg.shp_id]]
    
    # Create the query, concatenating the average of each pollutant
    query = f"""
        SELECT date, {cfg.shp_id},
        {", ".join([f"AVG({col}) AS {col}" for col in colnames])}
        FROM '{input_path}'
        GROUP BY date, {cfg.shp_id}
        ORDER BY date, {cfg.shp_id}
    """
    print(query)
        
    conn.execute(f"COPY ({query}) TO '{output_filename}'")
    conn.close()
    
if __name__ == "__main__":
    main()