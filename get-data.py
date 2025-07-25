import os
import numpy as np

from city_metrix.metrix_model import GeoExtent

from utils.download import *
from utils.grid import create_grid_for_city

data_path = 'data'
copy_to_s3 = False

city = 'ARG-Buenos_Aires'

# Get city_polygon -------------------------------------------------
city_polygon, boundaries_file = get_city_polygon(city, data_path=data_path, copy_to_s3=copy_to_s3)

# Get UTM -------------------------------------------------
from utils.utm import get_utm
utm_info = get_utm(city_polygon)
print(f"UTM EPSG: {utm_info['epsg']}, Earth Engine: {utm_info['ee']}")
#crs = utm_info['ee']
crs = 'EPSG:4326'

# Create the grid for the city -------------------------------------------------
city_grid, city_grid_file = create_grid_for_city(city, city_polygon, data_path=data_path, copy_to_s3=copy_to_s3)

# Check file with https://geojson.io/ or https://mapshaper.org/


# Loop over grid cells to get data for each cell -------------------------------------------------
for idx, cell in city_grid.iterrows():
    grid_cell_id = cell['ID']
    geometry = cell['geometry']
    # Get the bounding box of the cell
    bbox = GeoExtent(bbox=geometry.bounds)
    print(f"Processing grid cell {grid_cell_id} with bbox: {bbox}")
    print(f"Cell {grid_cell_id}")
    
    # Roads ---------------------
    print("Fetching roads data...")
    roads, roads_file = get_roads(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)

    # Open Space ---------------------
    print("Fetching open space data...")
    open_space, open_space_file = get_open_space(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)

    # Water ---------------------
    print("Fetching water data...")
    water, water_file = get_water(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)

    # Overture Buildings ---------------------
    print("Fetching Overture buildings data...")
    overture_buildings, overture_file = get_buildings(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)

    # Urban Land Use ---------------------
    print("Fetching Urban Land Use data...")
    urban_land_use, urban_land_use_file = get_urban_land_use(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)

    # ESA World Cover ---------------------
    print("Fetching ESA World Cover data...")
    esa, esa_file = get_esa(city, bbox, grid_cell_id=grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
