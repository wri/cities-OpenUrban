# Example .parquet file in R
1. Download https://storage.cloud.google.com/wri-cities-ssc/Overture/box-buildings.parquet
2. Run `parquet.R`


# Get new data using DuckDB and open-buildings
## Requirements
1. DuckDB - https://duckdb.org/docs/installation
2. open-buildings - https://github.com/opengeos/open-buildings?tab=readme-ov-file#installation

## Simple Example
`ob get_buildings 1.json 1-buildings.geojson --country_iso RW`

## Actual City wide data
1. Get a bounding box using `python bbox.py` 
   * Assumes `SSC_CensusUrbanAreas2020.zip` is in this folder
2. Download data `ob get_buildings box.json box-buildings.parquet --country_iso US`
   * Change the country_iso to match the bounding box location.


# Background
1. We want to get data from https://github.com/OvertureMaps/data but the documented options either require using a cloud account (AWS/Azure) or DuckDB (which doesn't support spatial queries).
2. Someone already ran into this issue and created https://github.com/opengeos/open-buildings which gets the data from https://source.coop/ and makes it "easy" to to download just the data in a specified polygon
3. open-buildings is a bit particular about how the polygon is provided so we have to make sure that is generated as a single feature geojson file.
4. While you can save the output in any format, they recommend `.parquet` because it is compressed out of the box
