# import os
# import numpy as np
# import geopandas as gpd
# from shapely.geometry import Polygon
# 
# from utils.upload import to_s3


# def create_grid_for_city(city, city_polygon, data_path, copy_to_s3, crs='EPSG:4326', cell_size=0.15):
#     """
#     Generates a grid of polygons covering a city's geometry
# 
#     Args:
#         city_polygon (geopandas.GeoDataFrame): A GeoDataFrame containing the city's geometry.
#         cell_size (float): The width and height of the grid cells in decimal degrees.
# 
#     Returns:
#         geopandas.GeoDataFrame: A GeoDataFrame containing the grid cells that intersect the city geometry.
#     """
#     # Create boundaries folder if it doesn't exist
#     city_grid_path = f'{data_path}/{city}/city_grid'
#     city_grid_file = f'{city_grid_path}/city_grid.geojson'
# 
#     if os.path.exists(city_grid_file):
#         print(f"City grid already exists at {city_grid_file}, loading...")
#         city_grid = gpd.read_file(city_grid_file)
# 
#         if copy_to_s3:
#             to_s3(city_grid_file, data_path)
#         return city_grid
#     else:
#         print(f"Fetching city grid data for {city}...")
# 
#         # 1. Check it's in WGS84 (EPSG:4326) as the cell size is in degrees
#         if crs != 'EPSG:4326':
#             print(f"WARNING: city polygon not in EPSG:4326.")
#         city_polygon = city_polygon.to_crs(crs)
#         
#         # Get UTM to calculate buffer
#         utm_crs = city_polygon.estimate_utm_crs()
#         city_polygon = city_polygon.to_crs(utm_crs)
#         
#         # Buffer in meters
#         city_buff = city_polygon.buffer(804.672)
# 
#         # Dissolve to a single polygon geometry to handle multi-part features
#         # and simplify intersection checks.
#         city_unary = city_buff.unary_union
# 
#         # 2. Get the total bounds of the geometry
#         minx, miny, maxx, maxy = city_unary.bounds
#         print(f"City bounds: {minx, miny, maxx, maxy}")
# 
#         # 3. Generate grid cell polygons
#         grid_cells = []
#         # Create a list of x and y coordinates for the grid
#         x_coords = np.arange(minx, maxx + cell_size, cell_size)
#         y_coords = np.arange(miny, maxy + cell_size, cell_size)
# 
#         for x in x_coords[:-1]:
#             for y in y_coords[:-1]:
#                 # Create a polygon for each cell
#                 poly = Polygon([
#                     (x, y),
#                     (x + cell_size, y),
#                     (x + cell_size, y + cell_size),
#                     (x, y + cell_size)
#                 ])
#                 grid_cells.append(poly)
# 
#         print(f"Created a coarse grid with {len(grid_cells)} cells.")
# 
#         # 4. Create a GeoDataFrame from the grid cells
#         grid_gdf = gpd.GeoDataFrame(grid_cells, columns=['geometry'], crs="EPSG:4326")
#         
#         # Transform back to 4326
#         grid_gdf.to_crs('EPSG:4326')
# 
#         # 5. Filter the grid to keep only cells that intersect the city geometry
#         # This is equivalent to st_filter(city_geom) in the R script.
#         intersecting_mask = grid_gdf.intersects(city_unary)
#         final_grid = grid_gdf[intersecting_mask].copy()
#         print(f"Filtered to {len(final_grid)} cells that intersect the city geometry.")
# 
#         # 6. Add a unique ID, starting from 1
#         final_grid['ID'] = range(1, len(final_grid) + 1)
#         
#         # Reset index for a clean GeoDataFrame
#         city_grid = final_grid.reset_index(drop=True)
# 
# 
#         if not os.path.exists(city_grid_path):
#             os.makedirs(city_grid_path)
# 
#         # Save to a GeoJSON file
#         city_grid.to_file(city_grid_file, driver='GeoJSON')
# 
#         print(f"City grid saved to: {city_grid_file}")
# 
#         if copy_to_s3:
#             to_s3(city_grid_file, data_path)
# 
#         return city_grid

import os
import numpy as np
import geopandas as gpd
from shapely.geometry import box

from city_metrix.metrix_tools import get_utm_zone_from_latlon_point

from utils.upload import to_s3

HALF_MILE_M = 804.672

def create_grid_for_city(
    city,
    city_polygon,
    data_path,
    copy_to_s3=False,
    cell_size_m=15_000,
):
    """
    Generates a grid of polygons covering a city's (buffered) geometry.

    - Uses city centroid + get_utm_zone_from_latlon_point() for CRS
    - Buffers city boundary by 0.5 mile (804.672 m) in UTM
    - Builds grid in meters (UTM, 15 km tiles)
    - Writes GeoJSON in EPSG:4326 (portable)
    """

    city_grid_path = f"{data_path}/{city}/city_grid"
    city_grid_file = f"{city_grid_path}/city_grid.geojson"

    if os.path.exists(city_grid_file):
        print(f"City grid already exists at {city_grid_file}, loading...")
        city_grid = gpd.read_file(city_grid_file)
        if copy_to_s3:
            to_s3(city_grid_file, data_path)
        return city_grid

    print(f"Creating city grid for {city}...")

    # ------------------------------------------------------------------
    # 1) Ensure geographic CRS and compute centroid
    # ------------------------------------------------------------------
    city_4326 = city_polygon.to_crs("EPSG:4326")
    city_centroid = city_4326.unary_union.centroid

    # ------------------------------------------------------------------
    # 2) Determine UTM CRS from centroid (your function)
    # ------------------------------------------------------------------
    utm_crs = get_utm_zone_from_latlon_point(city_centroid)
    print(f"Using UTM CRS: {utm_crs}")

    # ------------------------------------------------------------------
    # 3) Project to UTM, dissolve, buffer in meters
    # ------------------------------------------------------------------
    city_unary_utm = city_4326.to_crs(utm_crs).unary_union
    city_buff_utm = city_unary_utm.buffer(HALF_MILE_M)

    # ------------------------------------------------------------------
    # 4) Grid bounds in UTM meters
    # ------------------------------------------------------------------
    minx, miny, maxx, maxy = city_buff_utm.bounds
    print(f"Buffered city bounds (UTM): {minx, miny, maxx, maxy}")

    # ------------------------------------------------------------------
    # 5) Generate 15 km × 15 km grid in UTM
    # ------------------------------------------------------------------
    x_coords = np.arange(minx, maxx + cell_size_m, cell_size_m)
    y_coords = np.arange(miny, maxy + cell_size_m, cell_size_m)

    grid_cells = [
        box(x, y, x + cell_size_m, y + cell_size_m)
        for x in x_coords[:-1]
        for y in y_coords[:-1]
    ]

    grid_utm = gpd.GeoDataFrame({"geometry": grid_cells}, crs=utm_crs)
    print(f"Created {len(grid_utm)} candidate tiles (UTM).")

    # ------------------------------------------------------------------
    # 6) Keep only tiles intersecting buffered city
    # ------------------------------------------------------------------
    grid_utm = grid_utm.loc[grid_utm.intersects(city_buff_utm)].copy()
    grid_utm.reset_index(drop=True, inplace=True)
    print(f"Kept {len(grid_utm)} tiles after intersection filter.")

    # ------------------------------------------------------------------
    # 7) Add stable IDs
    # ------------------------------------------------------------------
    grid_utm["ID"] = np.arange(1, len(grid_utm) + 1)
    grid_utm["tile_name"] = (
        grid_utm["ID"].astype(str).str.zfill(5).radd("tile_")
    )

    # ------------------------------------------------------------------
    # 8) Save GeoJSON in EPSG:4326
    # ------------------------------------------------------------------
    os.makedirs(city_grid_path, exist_ok=True)

    city_grid_4326 = grid_utm.to_crs("EPSG:4326")
    city_grid_4326.to_file(city_grid_file, driver="GeoJSON")
    print(f"City grid saved to: {city_grid_file}")

    if copy_to_s3:
        to_s3(city_grid_file, data_path)

    return city_grid_4326
