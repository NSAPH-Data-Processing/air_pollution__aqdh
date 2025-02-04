#!/bin/bash

# write output of CMR search collections into cmr_aqdh_collections.json
curl -G "https://cmr.earthdata.nasa.gov/search/collections.json" --data-urlencode "project=AQDH" > notes/cmr_aqdh_collections.jsonl

# write output of CMR search granules into a json
# NO2 Concentrations in the Contiguous United States, 1 km, 2000-2016
curl -G "https://cmr.earthdata.nasa.gov/search/granules.json" --data-urlencode "collection_concept_id=C2848642691-SEDAC" > notes/cmr_aqdh_no2_granules.jsonl
# O3 Concentrations in the Contiguous United States, 1 km, 2000-2016
curl -G "https://cmr.earthdata.nasa.gov/search/granules.json" --data-urlencode "collection_concept_id=C2187535796-SEDAC" > notes/cmr_aqdh_o3_granules.jsonl
# PM2.5 Concentrations in the Contiguous United States, 1 km, 2000-2016
curl -G "https://cmr.earthdata.nasa.gov/search/granules.json" --data-urlencode "collection_concept_id=C2848642054-SEDAC" > notes/cmr_aqdh_pm25_granules.jsonl


# open in a web browser: https://sedac.ciesin.columbia.edu/downloads/data/aqdh/aqdh-no2-concentrations-contiguous-us-1-km-v1-10-2000-2016/aqdh-no2-concentrations-contiguous-us-1-km-v1-10-2000-2016-200001-geotiff.zip
# open in a web browser: https://sedac.ciesin.columbia.edu/downloads/data/aqdh/aqdh-o3-concentrations-contiguous-us-1-km-v1-10-2000-2016/aqdh-o3-concentrations-contiguous-us-1-km-v1-10-2000-2016-200001-geotiff.zip
# open in a web browser: https://sedac.ciesin.columbia.edu/downloads/data/aqdh/aqdh-pm2-5-concentrations-contiguous-us-1-km-v1-10-2000-2016/aqdh-pm2-5-concentrations-contiguous-us-1-km-v1-10-2000-2016-200001-geotiff.zip"
