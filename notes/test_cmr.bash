#!/bin/bash

# write output of CMR search collections into cmr_aqdh_collections.json
curl -G "https://cmr.earthdata.nasa.gov/search/collections.json" --data-urlencode "project=AQDH" > notes/cmr_aqdh_collections.json

# write output of CMR search granules into cmr_aqdh_granules.json
curl -G "https://cmr.earthdata.nasa.gov/search/granules.json" --data-urlencode "collection_concept_id=C2302636732-SEDAC" > notes/cmr_aqdh_granules.json

# open in a web browser: https://sedac.ciesin.columbia.edu/downloads/data/aqdh/aqdh-no2-concentrations-contiguous-us-1-km-2000-2016/aqdh-no2-concentrations-contiguous-us-1-km-2000-2016-200001-geotiff.zip
