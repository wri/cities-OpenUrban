import os
import numpy as np
import geopandas as gpd
import rasterio
from enum import Enum

from city_metrix.layers import (
    OpenStreetMap, 
    OpenStreetMapClass, 
    EsaWorldCover, 
    OvertureBuildings, 
    UrbanLandUse
)

from utils.upload import to_s3


def get_city_polygon(city, data_path, copy_to_s3=False, crs='EPSG:4326'):
    """
    Fetches the city polygon from a remote URL and saves it to a local GeoJSON file.
    
    Args:
        city (str): The name of the city to fetch.
        data_path (str): The path to save the city polygon data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
        crs (str): The coordinate reference system to use for the polygon.
    
    Returns:
        tuple: (geopandas.GeoDataFrame, str) - The city polygon as a GeoDataFrame and the file path.
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

        if copy_to_s3:
            to_s3(boundaries_file)

    return city_polygon, boundaries_file


def get_roads(city, bbox, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the city roads from OpenStreetMap and saves it to a local GeoJSON file.
    
    Args:
        city (str): The name of the city to fetch roads for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch roads for.
        data_path (str): The path to save the roads data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
    
    Returns:
        tuple: (geopandas.GeoDataFrame, str) - Roads as a GeoDataFrame and the file path.
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

        if copy_to_s3:
            to_s3(roads_file)

    return roads, roads_file


class OpenUrbanOpenStreetMapClass(Enum):
    OPEN_SPACE = {
        'leisure': [
            'pitch', 
            'park', 
            'garden', 
            'playground', 
            'nature_reserve', 
            'golf_course',
            'common',
            'dog_park',
            'recreation_ground',
            'disc_golf_course'
            ],
        'boundary': [
            'protected_area', 
            'national_park',
            'forest_compartment',
            'forest'
            ]
        }

def get_open_space(city, bbox, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the open space data from OpenStreetMap and saves it to a local GeoJSON file.
    
    Args:
        city (str): The name of the city to fetch open space for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch open space for.
        data_path (str): The path to save the open space data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
    
    Returns:
        tuple: (geopandas.GeoDataFrame, str) - The open space as a GeoDataFrame and the file path.
    """
    open_space_path = f'{data_path}/{city}/open_space'
    open_space_file = f'{open_space_path}/open_space_{grid_cell_id}.geojson'

    # If the open space file already exists, skip fetching
    if os.path.exists(open_space_file):
        print(f"Open space data already exists at {open_space_file}, skipping fetch.")
        open_space = gpd.read_file(open_space_file)
    else:
        print(f"Fetching open space data for {city}...")
        open_space = OpenStreetMap(osm_class=OpenUrbanOpenStreetMapClass.OPEN_SPACE).get_data(bbox)

        # Create open space folder if it doesn't exist
        if not os.path.exists(open_space_path):
            os.makedirs(open_space_path)

        # Save to a GeoJSON file
        open_space.to_file(open_space_file, driver='GeoJSON')

        if copy_to_s3:
            to_s3(open_space_file)

    return open_space, open_space_file

def get_water(city, bbox, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the water data from OpenStreetMap and saves it to a local GeoJSON file.
    
    Args:
        city (str): The name of the city to fetch water for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch water for.
        data_path (str): The path to save the water data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
    
    Returns:
        tuple: (geopandas.GeoDataFrame, str) - The water as a GeoDataFrame and the file path.
    """
    water_path = f'{data_path}/{city}/water'
    water_file = f'{water_path}/water_{grid_cell_id}.geojson'

    # If the water file already exists, skip fetching
    if os.path.exists(water_file):
        print(f"Water data already exists at {water_file}, skipping fetch.")
        water = gpd.read_file(water_file)
    else:
        print(f"Fetching water data for {city}...")
        water = OpenStreetMap(osm_class=OpenStreetMapClass.WATER).get_data(bbox)

        # Create water folder if it doesn't exist
        if not os.path.exists(water_path):
            os.makedirs(water_path)

        # Save to a GeoJSON file
        water.to_file(water_file, driver='GeoJSON')

        if copy_to_s3:
            to_s3(water_file)

    return water, water_file

def get_buildings(city, bbox, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the building data from Overture Maps and saves it to a local GeoJSON file.

    Args:
        city (str): The name of the city to fetch buildings for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch buildings for.
        data_path (str): The path to save the buildings data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.

    Returns:
        tuple: (geopandas.GeoDataFrame, str) - The buildings as a GeoDataFrame and the file path.
    """
    buildings_path = f'{data_path}/{city}/buildings'
    buildings_file = f'{buildings_path}/buildings_{grid_cell_id}.geojson'

    # If the buildings file already exists, skip fetching
    if os.path.exists(buildings_file):
        print(f"Buildings data already exists at {buildings_file}, skipping fetch.")
        buildings = gpd.read_file(buildings_file)
    else:
        print(f"Fetching buildings data for {city}...")
        try:
            buildings = OvertureBuildings().get_data(bbox)
            
            # Check if buildings data is empty
            if buildings is None or len(buildings) == 0:
                print(f"No buildings found for grid cell {grid_cell_id}")
                # Create empty GeoDataFrame with the same structure
                buildings = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
            
            # Create buildings folder if it doesn't exist
            if not os.path.exists(buildings_path):
                os.makedirs(buildings_path)

            # Save to a GeoJSON file
            buildings.to_file(buildings_file, driver='GeoJSON')
            
            if copy_to_s3:
                to_s3(buildings_file)
            
        except (OSError, ConnectionError, TimeoutError, Exception) as e:
            print(f"Error fetching buildings data for grid cell {grid_cell_id}: {e}")
            print(f"Creating empty buildings dataset for grid cell {grid_cell_id}")
            
            # Create empty GeoDataFrame
            buildings = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
            
            # Create buildings folder if it doesn't exist
            if not os.path.exists(buildings_path):
                os.makedirs(buildings_path)
            
            # Save empty dataset to file so we don't retry
            buildings.to_file(buildings_file, driver='GeoJSON')

    return buildings, buildings_file

def get_urban_land_use(city, bbox, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the urban land use data for the specified city and grid cell.

    Args:
        city (str): The name of the city to fetch urban land use data for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch data for.
        data_path (str): The path to save the urban land use data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.

    Returns:
        tuple: (geopandas.GeoDataFrame, str) - The urban land use data as a GeoDataFrame and the file path.
    """
    urban_land_use_path = f'{data_path}/{city}/urban_land_use'
    urban_land_use_file = f'{urban_land_use_path}/urban_land_use_{grid_cell_id}.tif'

    # If the urban land use file already exists, skip fetching
    if os.path.exists(urban_land_use_file):
        print(f"Urban land use data already exists at {urban_land_use_file}, skipping fetch.")
        urban_land_use = gpd.read_file(urban_land_use_file)
    else:
        print(f"Fetching urban land use data for {city}...")
        urban_land_use = UrbanLandUse().get_data(bbox)

        # Create urban land use folder if it doesn't exist
        if not os.path.exists(urban_land_use_path):
            os.makedirs(urban_land_use_path)

        # Write raster to tif file
        urban_land_use.rio.to_raster(raster_path=urban_land_use_file)

        if copy_to_s3:
            to_s3(urban_land_use_file)

    return urban_land_use, urban_land_use_file

def get_esa(city, bbox, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the ESA land cover data for the specified city and grid cell.
    
    Args:
        city (str): The name of the city to fetch ESA data for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch data for.
        data_path (str): The path to save the ESA data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
    
    Returns:
        tuple: (raster data, str) - The ESA land cover data and the file path.
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
            esa = EsaWorldCover().get_data(bbox, spatial_resolution=100)

            if not os.path.exists(esa_path):
                os.makedirs(esa_path)

            # Write raster to tif file
            esa.rio.to_raster(raster_path=esa_file)

            if copy_to_s3:
                to_s3(esa_file)

    return esa, esa_file

