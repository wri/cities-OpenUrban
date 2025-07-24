import os
from enum import Enum

import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon

from city_metrix.metrix_model import GeoExtent
from city_metrix.layers import OpenStreetMap
from city_metrix.layers import OpenStreetMapClass

data_path = './data'


def create_grid_for_city(city_polygon, cell_size=0.15):
    """
    Generates a grid of polygons covering a city's geometry

    Args:
        city_polygon (geopandas.GeoDataFrame): A GeoDataFrame containing the city's geometry.
        cell_size (float): The width and height of the grid cells in decimal degrees.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame containing the grid cells that
                                intersect with the city's geometry.
    """
    # 1. Ensure it's in WGS84 (EPSG:4326) as the cell size is in degrees
    city_polygon = city_polygon.to_crs("EPSG:4326")

    # Dissolve to a single polygon geometry to handle multi-part features
    # and simplify intersection checks.
    city_unary = city_polygon.unary_union

    # 2. Get the total bounds of the geometry
    minx, miny, maxx, maxy = city_unary.bounds
    print(f"City bounds: {minx, miny, maxx, maxy}")

    # 3. Generate grid cell polygons
    grid_cells = []
    # Create a list of x and y coordinates for the grid
    x_coords = np.arange(minx, maxx + cell_size, cell_size)
    y_coords = np.arange(miny, maxy + cell_size, cell_size)

    for x in x_coords[:-1]:
        for y in y_coords[:-1]:
            # Create a polygon for each cell
            poly = Polygon([
                (x, y),
                (x + cell_size, y),
                (x + cell_size, y + cell_size),
                (x, y + cell_size)
            ])
            grid_cells.append(poly)

    print(f"Created a coarse grid with {len(grid_cells)} cells.")

    # 4. Create a GeoDataFrame from the grid cells
    grid_gdf = gpd.GeoDataFrame(grid_cells, columns=['geometry'], crs="EPSG:4326")

    # 5. Filter the grid to keep only cells that intersect the city geometry
    # This is equivalent to st_filter(city_geom) in the R script.
    intersecting_mask = grid_gdf.intersects(city_unary)
    final_grid = grid_gdf[intersecting_mask].copy()
    print(f"Filtered to {len(final_grid)} cells that intersect the city geometry.")

    # 6. Add a unique ID, starting from 1
    final_grid['ID'] = range(1, len(final_grid) + 1)
    
    # Reset index for a clean GeoDataFrame
    final_grid = final_grid.reset_index(drop=True)

    return final_grid


# Get city_polygon -------------------------------------------------
city = 'ARG-Buenos_Aires'
crs = 'EPSG:4326'

city_polygon_url = f'https://wri-cities-data-api.s3.us-east-1.amazonaws.com/data/prd/boundaries/geojson/{city}.geojson'
city_gdf = gpd.read_file(city_polygon_url).to_crs(crs)

# Keep just the top level city polygon
city_polygon = city_gdf[city_gdf['geo_name'] == city_gdf['geo_parent_name']]

# Keep just the geometry column
city_polygon = city_polygon[['geometry']]

# Create boundaries folder if it doesn't exist
boundaries_path = f'{data_path}/{city}/boundaries'
if not os.path.exists(boundaries_path):
    os.makedirs(boundaries_path)

# Save to a GeoJSON file
city_polygon.to_file(f'{boundaries_path}/city_polygon.geojson', driver='GeoJSON')



# Create the grid for the city -------------------------------------------------
city_grid = create_grid_for_city(city_polygon)

# Create boundaries folder if it doesn't exist
city_grid_path = f'{data_path}/{city}/city_grid'
if not os.path.exists(city_grid_path):
    os.makedirs(city_grid_path)

# Save to a GeoJSON file
city_grid.to_file(f'{city_grid_path}/city_grid.geojson', driver='GeoJSON')

# Check file with https://geojson.io/ or https://mapshaper.org/


# Loop over grid cells to get data for each cell -------------------------------------------------
for idx, cell in city_grid.iterrows():
    grid_cell_id = cell['ID']
    # Get the bounding box of the cell
    bbox = GeoExtent(bbox=cell['geometry'].bounds, crs=crs)
    print(f"Cell {grid_cell_id}")
    
    # Roads ---------------------
    print("Fetching roads data...")
    city_roads = OpenStreetMap(osm_class=OpenStreetMapClass.ROAD).get_data(bbox)

    # Create boundaries folder if it doesn't exist
    roads_path = f'{data_path}/{city}/roads'
    if not os.path.exists(roads_path):
        os.makedirs(roads_path)

    # Save to a GeoJSON file
    city_roads.to_file(f'{roads_path}/roads_{grid_cell_id}.geojson', driver='GeoJSON')



