import geopandas as gpd
import utm

def get_utm(aoi):
    """
    Get UTM zone information for an area of interest (AOI).
    
    Args:
        aoi (geopandas.GeoDataFrame): Area of interest as a GeoDataFrame
        
    Returns:
        dict: Dictionary with 'epsg' (int) and 'ee' (str) keys
    """
    # Transform to WGS84 (EPSG:4326) and get centroid
    aoi_4326 = aoi.to_crs(4326)
    centroid = aoi_4326.centroid.iloc[0]
    
    # Get latitude and longitude from centroid
    lon, lat = centroid.x, centroid.y
    
    # Calculate UTM zone using utm library
    _, _, zone_number, zone_letter = utm.from_latlon(lat, lon)
    
    # Determine hemisphere and calculate EPSG code
    hemisphere = 'N' if lat >= 0 else 'S'
    utm_epsg = 32600 + zone_number if hemisphere == 'N' else 32700 + zone_number
    
    # Create Earth Engine compatible string
    utm_ee = f"EPSG:{utm_epsg}"
    
    return {
        'epsg': utm_epsg,
        'ee': utm_ee
    }
