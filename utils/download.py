import os
import geopandas as gpd

def get_city_polygon(city, crs, data_path):
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
