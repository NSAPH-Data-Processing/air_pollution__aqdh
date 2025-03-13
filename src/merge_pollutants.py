import duckdb
import hydra

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    file_prefix = f"{cfg.pollutant}_aqdh__{cfg.shp_id}_daily__{cfg.yyyy}"
    input_prefix = f"data/intermediate/{file_prefix}"
    output_prefix = f"data/output/daily/{file_prefix}"
    
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT * 
            FROM '{input_prefix}*.parquet'
        ) 
        TO '{output_prefix}.parquet'
    """)
    conn.close()

    
if __name__ == "__main__":
    main()