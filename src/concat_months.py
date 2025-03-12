import duckdb
import hydra

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    input_prefix = f"data/intermediate/{cfg.pollutant}_aqdh__{cfg.shp_id}_daily__{cfg.yyyy}"
    output_prefix = f"data/output/{cfg.shp_id}_daily/{cfg.pollutant}_aqdh__{cfg.shp_id}_daily__{cfg.yyyy}"
    
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