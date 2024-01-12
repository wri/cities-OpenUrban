import geopandas as gpd
from shapely.geometry import box
from geojson import Feature
import json

# Get all Cities
SSC_CensusUrbanAreas2020 = gpd.read_file('SSC_CensusUrbanAreas2020.zip')

# Specify the index of the city you want
row_index = 0  # Example: Save the first row

# Extract the row as a GeoDataFrame
single_row_gdf = SSC_CensusUrbanAreas2020.iloc[[row_index]]

# Convert to polygon
bbox = box(*single_row_gdf.total_bounds)

# Convert to geojson
box = Feature(geometry=bbox)

# Write the Feature to a GeoJSON file
with open("box.json", "w") as f:
    f.write(json.dumps(box))

