import os
import numpy as np
import geopandas as gpd
from shapely.geometry import box

from city_metrix.metrix_tools import get_utm_zone_from_latlon_point

from utils.upload import to_s3

BUFFER = 1500

def create_grid_for_city(
    city,
    city_polygon,
    data_path,
    copy_to_s3=True,
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
    city_buff_utm = city_unary_utm.buffer(BUFFER)

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
