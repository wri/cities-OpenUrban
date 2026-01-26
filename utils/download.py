import os
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from enum import Enum
import glob

from city_metrix.layers import (
    OpenStreetMap, 
    OpenStreetMapClass, 
    EsaWorldCover, 
    OvertureBuildings, 
    UrbanLandUse,
    AverageNetBuildingHeight
)

from utils.upload import to_s3

from shapely.geometry import (
    LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection
)

def _extract_allowed_parts(geom, allowed_types):
    """Return a flat list of parts from `geom` whose geom_type is in allowed_types."""
    if geom is None or geom.is_empty:
        return []
    gt = geom.geom_type
    if gt in allowed_types:
        return [geom]
    if isinstance(geom, GeometryCollection):
        parts = []
        for g in geom.geoms:
            parts.extend(_extract_allowed_parts(g, allowed_types))
        return parts
    # e.g., got a Polygon but we only want lines → drop it
    return []

def keep_only(gdf, allowed=("LineString", "MultiLineString"),
              combine=True, force_multi=False, drop_empty=True):
    """
    Keep only geometries whose type is in `allowed`. Handles GeometryCollections.

    Parameters
    ----------
    gdf : GeoDataFrame
    allowed : iterable of geometry type names
        e.g. ("LineString","MultiLineString") or ("Polygon","MultiPolygon")
    combine : bool
        If True, combine parts into a single LineString/MultiLineString or Polygon/MultiPolygon per row.
        If the parts are mixed linear+areal, a GeometryCollection is returned.
    force_multi : bool
        If True and `combine` is True, always return MultiLineString/MultiPolygon even for 1 part.
    drop_empty : bool
        Drop rows that result in empty/None geometry.

    Returns
    -------
    GeoDataFrame with only the requested geometry kinds.
    """
    linear_set = {"LineString", "MultiLineString"}
    areal_set  = {"Polygon", "MultiPolygon"}

    out = gdf.copy()
    new_geoms = []

    for geom in out.geometry:
        parts = _extract_allowed_parts(geom, set(allowed))
        if not parts:
            new_geoms.append(None)
            continue

        if not combine:
            new_geoms.append(GeometryCollection(parts))
            continue

        # combine by family if possible
        if all(p.geom_type in linear_set for p in parts):
            lines = []
            for p in parts:
                if isinstance(p, LineString):
                    lines.append(p)
                elif isinstance(p, MultiLineString):
                    lines.extend(list(p.geoms))
            if force_multi or len(lines) > 1:
                new_geoms.append(MultiLineString(lines))
            else:
                new_geoms.append(lines[0])

        elif all(p.geom_type in areal_set for p in parts):
            polys = []
            for p in parts:
                if isinstance(p, Polygon):
                    polys.append(p)
                elif isinstance(p, MultiPolygon):
                    polys.extend(list(p.geoms))
            if force_multi or len(polys) > 1:
                new_geoms.append(MultiPolygon(polys))
            else:
                new_geoms.append(polys[0])

        else:
            # Mixed families (you allowed both, or the source is messy)
            new_geoms.append(GeometryCollection(parts))

    out.geometry = new_geoms
    if drop_empty:
        out = out[out.geometry.notnull() & ~out.geometry.is_empty].copy()
    return out



def get_city_polygon(city, data_path, copy_to_s3=False, crs='EPSG:4326'):
    """
    Fetches the city polygon from a remote URL and saves it to a local GeoJSON file.
    
    Args:
        city (str): The name of the city to fetch.
        data_path (str): The path to save the city polygon data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
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

        if copy_to_s3:
            to_s3(boundaries_file, data_path)

    return city_polygon


def get_roads(city, bbox, grid_cell_id, data_path, copy_to_s3=False, compression="snappy"):
    """Fetch OSM roads → GeoParquet."""
    roads_path = f"{data_path}/{city}/roads"
    roads_file = f"{roads_path}/roads_{grid_cell_id}.parquet"
    os.makedirs(roads_path, exist_ok=True)

    if os.path.exists(roads_file):
        print(f"Roads data already exists at {roads_file}, skipping fetch.")
        return

    print(f"Fetching roads data for {city}...")
    roads = OpenStreetMap(osm_class=OpenStreetMapClass.ROAD).get_data(bbox)
    roads = keep_only(roads, allowed=("LineString", "MultiLineString"))

    # ensure geometry column is set
    if roads is None or roads.empty:
        roads = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    roads.to_parquet(roads_file, index=False, compression=compression)
    if copy_to_s3:
        to_s3(roads_file, data_path)



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

def get_open_space(city, bbox, grid_cell_id, data_path, copy_to_s3=False, compression="snappy"):
    """Fetch OSM open space → GeoParquet."""
    open_space_path = f"{data_path}/{city}/open_space"
    open_space_file = f"{open_space_path}/open_space_{grid_cell_id}.parquet"
    os.makedirs(open_space_path, exist_ok=True)

    if os.path.exists(open_space_file):
        print(f"Open space data already exists at {open_space_file}, skipping fetch.")
        return

    print(f"Fetching open space data for {city}...")
    open_space = OpenStreetMap(osm_class=OpenUrbanOpenStreetMapClass.OPEN_SPACE).get_data(bbox)
    open_space = keep_only(open_space, allowed=("Polygon", "MultiPolygon"))

    if open_space is None or open_space.empty:
        open_space = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    open_space.to_parquet(open_space_file, index=False, compression=compression)
    if copy_to_s3:
        to_s3(open_space_file, data_path)



def get_water(city, bbox, grid_cell_id, data_path, copy_to_s3=False, compression="snappy"):
    """Fetch OSM water → GeoParquet."""
    water_path = f"{data_path}/{city}/water"
    water_file = f"{water_path}/water_{grid_cell_id}.parquet"
    os.makedirs(water_path, exist_ok=True)

    if os.path.exists(water_file):
        print(f"Water data already exists at {water_file}, skipping fetch.")
        return

    print(f"Fetching water data for {city}...")
    water = OpenStreetMap(osm_class=OpenStreetMapClass.WATER).get_data(bbox)
    water = keep_only(water, allowed=("Polygon", "MultiPolygon"))

    if water is None or water.empty:
        water = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    water.to_parquet(water_file, index=False, compression=compression)
    if copy_to_s3:
        to_s3(water_file, data_path)



def get_parking(city, bbox, grid_cell_id, data_path, copy_to_s3=False, compression="snappy"):
    """Fetch OSM parking → GeoParquet."""
    parking_path = f"{data_path}/{city}/parking"
    parking_file = f"{parking_path}/parking_{grid_cell_id}.parquet"
    os.makedirs(parking_path, exist_ok=True)

    if os.path.exists(parking_file):
        print(f"Parking data already exists at {parking_file}, skipping fetch.")
        return

    print(f"Fetching parking data for {city}...")
    parking = OpenStreetMap(osm_class=OpenStreetMapClass.PARKING).get_data(bbox)
    parking = keep_only(parking, allowed=("Polygon", "MultiPolygon"))

    if parking is None or parking.empty:
        parking = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    parking.to_parquet(parking_file, index=False, compression=compression)
    if copy_to_s3:
        to_s3(parking_file, data_path)

def get_buildings(city, bbox_fetch, grid_cell_id, data_path, copy_to_s3=False, compression="snappy"):
    """Fetch Overture buildings → GeoParquet."""
    buildings_path = f"{data_path}/{city}/buildings"
    buildings_file = f"{buildings_path}/buildings_{grid_cell_id}.parquet"
    os.makedirs(buildings_path, exist_ok=True)

    if os.path.exists(buildings_file):
        print(f"Buildings data already exists at {buildings_file}, skipping fetch.")
        return

    print(f"Fetching buildings data for {city} (cell {grid_cell_id})...")
    try:
        gdf = OvertureBuildings().get_data(bbox)

        if gdf is None or len(gdf) == 0:
            print(f"No buildings found for grid cell {grid_cell_id}")
            gdf = gpd.GeoDataFrame(columns=["id","geometry"], geometry="geometry", crs="EPSG:4326")

        # Ensure an id column exists
        if "id" not in gdf.columns:
            if "building_id" in gdf.columns:
                gdf = gdf.rename(columns={"building_id": "id"})
            else:
                gdf = gdf.reset_index().rename(columns={"index": "id"})
        gdf["id"] = gdf["id"].astype(str)

        gdf.to_parquet(buildings_file, index=False, compression=compression)
        print(f"Wrote {len(gdf)} features → {buildings_file}")

        if copy_to_s3:
            to_s3(buildings_file, data_path)

    except (OSError, ConnectionError, TimeoutError, Exception) as e:
        print(f"Error fetching buildings for cell {grid_cell_id}: {e}")
        gdf = gpd.GeoDataFrame(columns=["id","geometry"], geometry="geometry", crs="EPSG:4326")
        gdf.to_parquet(buildings_file, index=False, compression=compression)
        print(f"Wrote EMPTY tile → {buildings_file}")
        if copy_to_s3:
            to_s3(buildings_file, data_path)

            
def merge_building_tiles(city, data_path, keep_tiles=False, copy_to_s3=False, compression="snappy"):
    """
    Merge all buildings_*.parquet tiles into buildings_all.parquet.
    """
    bdir = os.path.join(data_path, city, "buildings")
    os.makedirs(bdir, exist_ok=True)
    out_path = os.path.join(bdir, "buildings_all.parquet")

    tile_paths = sorted(
        os.path.join(bdir, f)
        for f in os.listdir(bdir)
        if f.startswith("buildings_") and f.endswith(".parquet") and f != os.path.basename(out_path)
    )
    if not tile_paths:
        print(f"[merge_building_tiles] No Parquet tiles found in {bdir}")
        return

    gdfs = []
    for p in tile_paths:
        gdf = gpd.read_parquet(p)
        if gdf.empty:
            continue
        if "id" not in gdf.columns:
            raise ValueError(f"'id' column missing in {p}")
        gdf["id"] = gdf["id"].astype(str)
        gdfs.append(gdf)

    if not gdfs:
        gpd.GeoDataFrame(columns=["id","geometry"], geometry="geometry", crs="EPSG:4326") \
          .to_parquet(out_path, index=False, compression=compression)
        print(f"[merge_building_tiles] Wrote 0 features → {out_path}")
        if copy_to_s3:
            to_s3(out_path, data_path)
        return

    all_bldg = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    before = len(all_bldg)
    all_bldg = all_bldg.drop_duplicates(subset=["id"], keep="first")
    print(f"[merge_building_tiles] Dropped {before - len(all_bldg)} duplicates by id.")

    all_bldg.to_parquet(out_path, index=False, compression=compression)
    print(f"[merge_building_tiles] Wrote {len(all_bldg)} features → {out_path}")

    if not keep_tiles:
        removed = 0
        for p in tile_paths:
            try:
                os.remove(p); removed += 1
            except Exception as e:
                print(f"[merge_building_tiles] Could not remove {p}: {e}")
        print(f"[merge_building_tiles] Deleted {removed} tile files.")

    if copy_to_s3:
        to_s3(out_path, data_path)

def get_urban_land_use(city, bbox, bbox_fetch, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the urban land use data for the specified city and grid cell.

    Args:
        city (str): The name of the city to fetch urban land use data for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch data for.
        data_path (str): The path to save the urban land use data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
    """
    urban_land_use_path = f'{data_path}/{city}/urban_land_use'
    urban_land_use_file = f'{urban_land_use_path}/urban_land_use_{grid_cell_id}.tif'

    # If the urban land use file already exists, skip fetching
    if os.path.exists(urban_land_use_file):
        print(f"Urban land use data already exists at {urban_land_use_file}, skipping fetch.")
    else:
        print(f"Fetching urban land use data for {city}...")
        urban_land_use = UrbanLandUse().get_data(bbox_fetch)
        urban_land_use = extract_bbox_aoi(urban_land_use, bbox)            # trim to original tile extent

        # Create urban land use folder if it doesn't exist
        if not os.path.exists(urban_land_use_path):
            os.makedirs(urban_land_use_path)

        # Write raster to tif file
        urban_land_use.rio.to_raster(raster_path=urban_land_use_file)

        if copy_to_s3:
            to_s3(urban_land_use_file, data_path)


def get_esa(city, bbox, bbox_fetch, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the ESA land cover data for the specified city and grid cell.
    
    Args:
        city (str): The name of the city to fetch ESA data for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch data for.
        data_path (str): The path to save the ESA data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
    """
    # Create ESA folder if it doesn't exist
    esa_path = f'{data_path}/{city}/esa'
    esa_file = f'{esa_path}/esa_{grid_cell_id}.tif'

    # If the ESA file already exists, skip fetching
    if os.path.exists(esa_file):
        print(f"ESA data already exists at {esa_file}, skipping fetch.")
    else:
        print(f"Fetching ESA LULC data for {city}...")
        # Check if all pixels are water (value 80 in ESA WorldCover)
        # If so, skip this tile
        esa = EsaWorldCover().get_data(bbox_fetch, spatial_resolution=10)
        esa_unique_values = np.unique(esa.values)
        esa_unique_values = esa_unique_values[~np.isnan(esa_unique_values)]  # Remove NaN values
        
        # ESA WorldCover value 80 = Permanent water bodies
        if len(esa_unique_values) == 1 and esa_unique_values[0] == 80:
            print(f"Cell {grid_cell_id} is all water, skipping...")
        else:
            esa = EsaWorldCover().get_data(bbox_fetch, spatial_resolution=1)
            esa = extract_bbox_aoi(esa, bbox)

            if not os.path.exists(esa_path):
                os.makedirs(esa_path)

            # Write raster to tif file
            esa.rio.to_raster(raster_path=esa_file)

            if copy_to_s3:
                to_s3(esa_file, data_path)

def get_anbh(city, bbox, bbox_fetch, grid_cell_id, data_path, copy_to_s3=False):
    """
    Fetches the Average Net Building Height data for the specified city and grid cell.
    
    Args:
        city (str): The name of the city to fetch ANBH data for.
        bbox (tuple): The bounding box of the grid cell as (minx, miny, maxx, maxy).
        grid_cell_id (int): The ID of the grid cell to fetch data for.
        data_path (str): The path to save the ANBH data.
        copy_to_s3 (bool): Whether to copy the file to S3 after saving locally.
    """
    # Create ANBH folder if it doesn't exist
    anbh_path = f'{data_path}/{city}/anbh'
    anbh_file = f'{anbh_path}/anbh_{grid_cell_id}.tif'

    # If the ANBH file already exists, skip fetching
    if os.path.exists(anbh_file):
        print(f"ANBH data already exists at {anbh_file}, skipping fetch.")
    else:
        print(f"Fetching ANBH data for {city}...")
        anbh = AverageNetBuildingHeight().get_data(bbox_fetch)
        anbh = extract_bbox_aoi(anbh, bbox)

        # Create urban land use folder if it doesn't exist
        if not os.path.exists(anbh_path):
            os.makedirs(anbh_path)

        # Write raster to tif file
        anbh.rio.to_raster(raster_path=anbh_file)

        if copy_to_s3:
            to_s3(anbh_file, data_path)
      

def summarize_average_lanes(city, data_path, copy_to_s3=False, default_lanes=2):
    """
    Make average_lanes.csv from roads_all.parquet and optionally upload to S3.

    Steps:
      - read GeoParquet
      - drop geometry
      - lanes -> numeric
      - group by 'highway', mean(na.rm=TRUE), ceil
      - NA -> default_lanes (default 2)
      - write CSV
    """
    roads_dir = os.path.join(data_path, city, "roads")
    in_parquet = os.path.join(roads_dir, "roads_all.parquet")
    out_csv = os.path.join(roads_dir, "average_lanes.csv")

    if not os.path.exists(in_parquet):
        print(f"[summarize_average_lanes] Not found: {in_parquet}")
        return

    # Read and drop geometry
    gdf = gpd.read_parquet(in_parquet)
    if gdf.empty:
        print(f"[summarize_average_lanes] Empty parquet: {in_parquet}")
        # still write a tiny CSV with just header
        pd.DataFrame(columns=["highway", "avg_lanes"]).to_csv(out_csv, index=False)
        if copy_to_s3:
            to_s3(out_csv, data_path)
        return

    df = gdf.drop(columns=[gdf.geometry.name], errors="ignore")

    # lanes -> numeric
    lanes = pd.to_numeric(df.get("lanes"), errors="coerce")
    df = df.assign(lanes_num=lanes)

    # group and summarize (keep NA 'highway' group to match dplyr behavior)
    avg = df.groupby("highway", dropna=False)["lanes_num"].mean()
    avg = np.ceil(avg)  # ceiling of mean
    # replace NaN means with default
    avg = avg.fillna(default_lanes).astype(int)

    out = avg.reset_index().rename(columns={"lanes_num": "avg_lanes"})

    # If you prefer the NA highway to appear as blank string in CSV:
    # out["highway"] = out["highway"].fillna("")

    out.to_csv(out_csv, index=False)
    print(f"[summarize_average_lanes] Wrote {len(out)} rows → {out_csv}")

    if copy_to_s3:
        to_s3(out_csv, data_path)

