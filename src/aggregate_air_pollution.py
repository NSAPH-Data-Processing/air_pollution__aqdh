import calendar
import hydra
import rasterio
import geopandas as gpd
from utils.faster_zonal_stats import polygon_to_raster_cells
import numpy as np
from tqdm import tqdm
import logging
import pandas as pd
from datetime import datetime

# configure logger to print at info level
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # read shapefile
    shp_path = f"{cfg.datapaths.base_path}/input/shapefiles/{cfg.shp_filename.format(yyyy=cfg.yyyy)}/{cfg.shp_filename.format(yyyy=cfg.yyyy)}.shp"
    LOGGER.info(f"Reading shapefile {shp_path}")
    shp = gpd.read_file(shp_path)
    LOGGER.info(f"Read shapefile with head\n: {shp.drop(columns='geometry').head()}")
    LOGGER.info(f"Shapefile CRS: {shp.crs}")
        
    # read tif
    replacements = {
        "pollutant_code": cfg.pollutant_code_map[cfg.pollutant],
        "yyyy": cfg.yyyy,
        "mm": cfg.mm
    }
    zip_filename = f"{cfg.datapaths.base_path}/intermediate/{cfg.zip_filename.format(**replacements)}/"

    yyyymmdd_list = [
    f"{cfg.yyyy}{cfg.mm}{d:02d}"
    for d in range(1, calendar.monthrange(int(cfg.yyyy), int(cfg.mm))[1] + 1)
    ]
    
    # predetermine the polygon to cell mapping using the first day 
    tif_path = f"{zip_filename}{cfg.tif_filename[cfg.pollutant].format(yyyymmdd=yyyymmdd_list[0])}.tif"
    with rasterio.open(tif_path) as src:
        num_layers = src.count  # Number of bands
        LOGGER.info(f"Number of bands: {num_layers}")

        # Print error and exit if there is more than 1 band
        if num_layers > 1:
            LOGGER.info("Error: More than 1 band in the tif file.")
            exit(1)
        
        transform = src.transform
        crs = src.crs
        nodata = src.nodata
        width, height = src.width, src.height  # Dimensions of the raster
        
        LOGGER.info(f"""
            Original characteristics:
            Transform: {transform}
            CRS: {crs}
            NoData: {nodata}
            Width: {width}
            Height: {height}
        """)
        
        # Check if the CRS is ESRI:102010 (North America Lambert Conformal Conic)
        if src.crs != "ESRI:102010":
            LOGGER.info("Error: The CRS of the tif file is not ESRI:102010.")
            exit(1)
        
        raster_array = src.read(1)  # Reads band 1 (shape: [height, width])
        
        # # plot the raster: for debugging purposes
        # import matplotlib.pyplot as plt
        # raster_array[raster_array == nodata] = np.nan
        # plt.imshow(raster_array)
        # plt.colorbar()
        # plt.savefig("temp_raster.png")
        
        # Reproject the shapefile to the raster's CRS
        # It must be ESRI:102010 (North America Lambert Conformal Conic)
        shp = shp.to_crs("ESRI:102010") 
        #LOGGER.info("Reprojected CRS:", shp.crs)
        print("Reprojected CRS:", shp.crs)
        
        # Crop the shapefile to the raster's extent
        shp = shp.cx[src.bounds.left:src.bounds.right, src.bounds.bottom:src.bounds.top]
        #LOGGER.info("Cropped shapefile with head\n:", shp.drop(columns='geometry').head())
        print("Cropped shapefile with head\n:", shp.drop(columns='geometry').head())
        
        # # plot the shapefile: for debugging purposes
        # import matplotlib.pyplot as plt
        # shp.plot()
        # plt.savefig("temp_shp.png")

        # Generate the cell mapping
        LOGGER.info(f"Obtaining cell mapping from {shp_path} and {tif_path}")
        
        poly2cells = polygon_to_raster_cells(
            shp.geometry.values,
            raster_array,
            affine=transform,
            nodata=nodata,
            all_touched=True,
            verbose=True #cfg.show_progress,
        )
        LOGGER.info(f"Generated polygon to cell mapping with length {len(poly2cells)}")
    
    df_list = []
    for yyyymmdd in tqdm(yyyymmdd_list):
        tif_path = f"{zip_filename}{cfg.tif_filename[cfg.pollutant].format(yyyymmdd=yyyymmdd)}.tif"
        
        stats = []
        with rasterio.open(tif_path) as src:
            x = src.read(1)
            x[x == src.nodata] = np.nan

            for indices in poly2cells:
                if len(indices[0]) == 0:
                    # no cells found for this polygon
                    stats.append(np.nan)
                else:
                    cells = x[indices]
                    if sum(~np.isnan(cells)) == 0:
                        # no valid cells found for this polygon
                        stats.append(np.nan)
                        continue
                    else:
                        # compute mean of valid cells
                        stats.append(np.nanmean(cells))

        # convert yyyymmdd to date
        date = datetime.strptime(yyyymmdd, '%Y%m%d').date()
        polygon_ids = shp[cfg.shp_id].values
        df = pd.DataFrame(
            {"date": date, cfg.pollutant: stats},
            index=pd.Index(polygon_ids, name=cfg.shp_id)
        )
        df_list.append(df)

        # # plot the result: for debugging purposes
        # import matplotlib.pyplot as plt
        # # convert to geopandas for image
        # gdf = gpd.GeoDataFrame(df, geometry=shp.geometry.values, crs=shp.crs)
        # gdf.plot(column=cfg.pollutant, legend=True)
        # plt.savefig("temp_output.png")
    
    # concatenate all periods
    df = pd.concat(df_list).reset_index()
    df = df.sort_values(by=[cfg.shp_id, "date"])
    LOGGER.info(f"Generated dataframe with head\n: {df.head()}")
    
    # store in parquet
    output_filename = f"{cfg.datapaths.base_path}/intermediate/{cfg.pollutant}_aqdh__{cfg.shp_id}_daily__{cfg.yyyy}{cfg.mm}.parquet"
    LOGGER.info(f"Writing output to {output_filename}")
    df.to_parquet(output_filename, index=False)
    LOGGER.info("Done")
    
if __name__ == "__main__":
    main()