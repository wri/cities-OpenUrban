import os
import numpy as np

from city_metrix.metrix_model import GeoExtent
from city_metrix.layers import OpenStreetMap, OpenStreetMapClass, EsaWorldCoverClass, EsaWorldCover

from utils.download import get_city_polygon
from utils.grid import create_grid_for_city
from utils.upload import to_s3

# Force reload of local modules in interactive mode
import importlib
from utils import download, grid, upload
importlib.reload(download)
importlib.reload(grid)
importlib.reload(upload)

data_path = 'data'
copy_to_s3 = False

city = 'ARG-Buenos_Aires'
crs = 'EPSG:4326'

# Get city_polygon -------------------------------------------------
city_polygon, boundaries_file = get_city_polygon(city, crs=crs, data_path=data_path)

if copy_to_s3:
    to_s3(boundaries_file)


# Create the grid for the city -------------------------------------------------
city_grid, city_grid_file = create_grid_for_city(city, crs, city_polygon, data_path=data_path)

if copy_to_s3:
    to_s3(city_grid_file)

# Check file with https://geojson.io/ or https://mapshaper.org/


# Loop over grid cells to get data for each cell -------------------------------------------------
for idx, cell in city_grid.iterrows():
    grid_cell_id = cell['ID']
    # Get the bounding box of the cell
    bbox = GeoExtent(bbox=cell['geometry'].bounds, crs=crs)
    print(f"Cell {grid_cell_id}")
    
    # Roads ---------------------
    print("Fetching roads data...")

    # Create roads folder if it doesn't exist
    roads_path = f'{data_path}/{city}/roads'
    if not os.path.exists(roads_path):
        os.makedirs(roads_path)
    roads_file = f'{roads_path}/roads_{grid_cell_id}.geojson'

    # If the roads file already exists, skip fetching
    if os.path.exists(roads_file):
        print(f"Roads data already exists at {roads_file}, skipping fetch.")
    else:
        city_roads = OpenStreetMap(osm_class=OpenStreetMapClass.ROAD).get_data(bbox)
    
        # Save to a GeoJSON file
        city_roads.to_file(roads_file, driver='GeoJSON')

        if copy_to_s3:
            to_s3(roads_file)

    # ESA ---------------------
    print("Fetching ESA land cover data...")
    esa_path = f'{data_path}/{city}/esa'
    esa_file = f'{esa_path}/esa_{grid_cell_id}.tif'

    # If the esa file already exists, skip fetching
    if os.path.exists(esa_file):
        print(f"ESA data already exists at {esa_file}, skipping fetch.")
    else:
        # Create esa folder if it doesn't exist
        if not os.path.exists(esa_path):
            os.makedirs(esa_path)

        # Check if all pixels are water (value 80 in ESA WorldCover)
        # If so, skip this tile
        city_esa = EsaWorldCover().get_data(bbox, spatial_resolution=10)
        esa_unique_values = np.unique(city_esa.values)
        esa_unique_values = esa_unique_values[~np.isnan(esa_unique_values)]  # Remove NaN values

        # ESA WorldCover value 80 = Permanent water bodies
        if len(esa_unique_values) == 1 and esa_unique_values[0] == 80:
            print(f"Cell {grid_cell_id} is all water, skipping...")
        else:
            city_esa = EsaWorldCover().get_data(bbox, spatial_resolution=1)
            ## Write raster to tif file
            city_esa.rio.to_raster(raster_path=esa_file)

            if copy_to_s3:
                to_s3(esa_file)
