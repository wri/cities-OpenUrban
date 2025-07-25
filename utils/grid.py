import os
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon

from utils.upload import to_s3


def create_grid_for_city(city, city_polygon, data_path, copy_to_s3, crs='EPSG:4326', cell_size=0.15):
    """
    Generates a grid of polygons covering a city's geometry

    Args:
        city_polygon (geopandas.GeoDataFrame): A GeoDataFrame containing the city's geometry.
        cell_size (float): The width and height of the grid cells in decimal degrees.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame containing the grid cells that
                                intersect with the city's geometry.
    """
    # 1. Check it's in WGS84 (EPSG:4326) as the cell size is in degrees
    if crs != 'EPSG:4326':
        print(f"WARNING: city polygon not in EPSG:4326.")
    city_polygon = city_polygon.to_crs(crs)

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
    city_grid = final_grid.reset_index(drop=True)

    # Create boundaries folder if it doesn't exist
    city_grid_path = f'{data_path}/{city}/city_grid'
    if not os.path.exists(city_grid_path):
        os.makedirs(city_grid_path)

    city_grid_file = f'{city_grid_path}/city_grid.geojson'
    # Save to a GeoJSON file
    city_grid.to_file(city_grid_file, driver='GeoJSON')

    print(f"City grid saved to: {city_grid_file}")

    if copy_to_s3:
        to_s3(city_grid_file)

    return city_grid, city_grid_file
