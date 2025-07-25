import os
import numpy as np
import geopandas as gpd
import rasterio
from enum import Enum

from city_metrix.layers import OpenStreetMap, OpenStreetMapClass, EsaWorldCover


def get_city_polygon(city, data_path, crs='EPSG:4326'):
    """
    Fetches the city polygon from a remote URL and saves it to a local GeoJSON file.
    
    Args:
        city (str): The name of the city to fetch.
        crs (str): The coordinate reference system to use for the polygon.
    
    Returns:
        geopandas.GeoDataFrame: The city polygon as a GeoDataFrame.
    """
    # Check if file already exists
    boundaries_path = f'{data_path}/{city}/boundaries'
    boundaries_file = f'{boundaries_path}/city_polygon.geojson'

    if os.path.exists(boundaries_file):
        print(f"City polygon already exists at {boundaries_file}, skipping fetch.")
        city_polygon = gpd.read_file(boundaries_file).to_crs(crs)
    else:
        print(f"Fetching city polygon for {city}...")
        # Create boundaries folder if it doesn't exist
        if not os.path.exists(boundaries_path):
            os.makedirs(boundaries_path)

        city_polygon_url = f'https://wri-cities-data-api.s3.us-east-1.amazonaws.com/data/prd/boundaries/geojson/{city}.geojson'
        city_gdf = gpd.read_file(city_polygon_url).to_crs(crs)

        # Keep just the top level city polygon
        city_polygon = city_gdf[city_gdf['geo_name'] == city_gdf['geo_parent_name']]

        # Keep just the geometry column
        city_polygon = city_polygon[['geometry']]

        # Save to a GeoJSON file
        city_polygon.to_file(boundaries_file, driver='GeoJSON')

    return city_polygon, boundaries_file

def get_roads(city, bbox, grid_cell_id, data_path):
    """
    Fetches the city roads from OpenStreetMap and saves it to a local GeoJSON file.
    
    Args:
        city (str): The name of the city to fetch roads for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch roads for.
        data_path (str): The path to save the roads data.
    
    Returns:
        geopandas.GeoDataFrame: Roads as a GeoDataFrame.
    """
    roads_path = f'{data_path}/{city}/roads'
    roads_file = f'{roads_path}/roads_{grid_cell_id}.geojson'

    # If the roads file already exists, skip fetching
    if os.path.exists(roads_file):
        print(f"Roads data already exists at {roads_file}, skipping fetch.")
        roads = gpd.read_file(roads_file)
    else:
        print(f"Fetching roads data for {city}...")
        roads = OpenStreetMap(osm_class=OpenStreetMapClass.ROAD).get_data(bbox)

        # Create roads folder if it doesn't exist
        if not os.path.exists(roads_path):
            os.makedirs(roads_path)

        # Save to a GeoJSON file

        roads.to_file(roads_file, driver='GeoJSON')

    return roads, roads_file


def get_esa(city, bbox, grid_cell_id, data_path):
    """
    Fetches the ESA land cover data for the specified city and grid cell.
    
    Args:
        city (str): The name of the city to fetch ESA data for.
        crs (str): The coordinate reference system to use for the data.
        grid_cell_id (int): The ID of the grid cell to fetch data for.
    
    Returns:
        geopandas.GeoDataFrame: The ESA land cover data as a GeoDataFrame.
    """
    # Create ESA folder if it doesn't exist
    esa_path = f'{data_path}/{city}/esa'
    esa_file = f'{esa_path}/esa_{grid_cell_id}.tif'

    # If the ESA file already exists, skip fetching
    if os.path.exists(esa_file):
        print(f"ESA data already exists at {esa_file}, skipping fetch.")
        esa = rasterio.open(esa_file)
    else:
        print(f"Fetching ESA LULC data for {city}...")
        # Check if all pixels are water (value 80 in ESA WorldCover)
        # If so, skip this tile
        esa = EsaWorldCover().get_data(bbox, spatial_resolution=10)
        esa_unique_values = np.unique(esa.values)
        esa_unique_values = esa_unique_values[~np.isnan(esa_unique_values)]  # Remove NaN values
        
        # ESA WorldCover value 80 = Permanent water bodies
        if len(esa_unique_values) == 1 and esa_unique_values[0] == 80:
            print(f"Cell {grid_cell_id} is all water, skipping...")
        else:
            esa = EsaWorldCover().get_data(bbox, spatial_resolution=1)

            if not os.path.exists(esa_path):
                os.makedirs(esa_path)

            # Write raster to tif file
            esa.rio.to_raster(raster_path=esa_file)

    return esa, esa_file

    
