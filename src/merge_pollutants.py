import duckdb
import hydra
import pandas as pd
import datetime

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    pollutant_list = cfg.pollutant_code_map.keys()
    conn = duckdb.connect()

    # Step 1: Create `index` table
    # create unique dates
    start_date = datetime.date(int(cfg.yyyy), 1, 1)
    end_date = datetime.date(int(cfg.yyyy), 12, 31)
    unique_dates_df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
    # create unique shp ids
    unique_shp_ids_df = conn.execute(f"""
        SELECT {cfg.shp_id} 
        FROM '{cfg.datapaths.base_path}/input/unique_id/{cfg.unique_id_filename.format(yyyy=cfg.yyyy)}'
    """).fetch_df()
    # create a full combination of all unique dates and unique shp ids
    index_df = unique_dates_df.merge(unique_shp_ids_df, how="cross")
    conn.execute(f"""
        CREATE TABLE index AS SELECT * FROM index_df ORDER BY date, {cfg.shp_id}
    """)

    # Step 2: Register each pollutant table separately
    for pollutant in pollutant_list:
        print(f"Creating table for {pollutant}")
        query = f"""
            CREATE TABLE {pollutant}_table AS
            SELECT i.date, i.{cfg.shp_id}, p.{pollutant} AS {pollutant}
            FROM index AS i
            LEFT JOIN '{cfg.datapaths.base_path}/intermediate/{pollutant}_aqdh__{cfg.shp_id}_daily__{cfg.yyyy}*.parquet' AS p
            ON i.date = p.date AND i.{cfg.shp_id} = p.{cfg.shp_id}
            ORDER BY i.date, i.{cfg.shp_id}
        """
        print(query)
        conn.execute(query)

    # Step 3: Perform the final **INNER JOIN** on all pollutant tables
    query = f"""
        SELECT i.date, i.{cfg.shp_id},
        {", ".join([f"{pollutant}_table.{pollutant} AS {pollutant}" for pollutant in pollutant_list])}
        FROM index i
        {" ".join([
            f"JOIN {pollutant}_table ON i.date = {pollutant}_table.date AND i.{cfg.shp_id} = {pollutant}_table.{cfg.shp_id}"
            for pollutant in pollutant_list
        ])}
        ORDER BY i.date, i.{cfg.shp_id}
    """
    print(query)

    output_filename = f"{cfg.datapaths.base_path}/output/daily/air_pollution__aqdh__{cfg.shp_id}_daily__{cfg.yyyy}.parquet"
    conn.execute(f"COPY ({query}) TO '{output_filename}'")
    conn.close()
    
if __name__ == "__main__":
    main()