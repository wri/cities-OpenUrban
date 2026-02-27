import os
import argparse
import numpy as np

from city_metrix.metrix_model import GeoExtent

import importlib, utils.download as download
importlib.reload(download)

from utils.download import *
from utils.grid import create_grid_for_city

import geopandas as gpd
import pandas as pd

from shapely.geometry import box


def make_vector_tiles_from_all(
    city: str,
    data_path: str,
    layer: str,
    city_grid: gpd.GeoDataFrame,
    buffer_m: float = 10.0,
    predicate: str = "intersects",   # "intersects" (safer) or "within" (stricter)
    compression: str = "snappy",
):
    """
    Create per-tile GeoParquet files (layer/layer_<id>.parquet) from a single layer_all.parquet.

    Assumes:
      {data_path}/{city}/{layer}/{layer}_all.parquet exists
    Writes:
      {data_path}/{city}/{layer}/{layer}_{ID}.parquet  for each grid cell ID in city_grid
    """
    layer_dir = os.path.join(data_path, city, layer)
    all_path = os.path.join(layer_dir, f"{layer}_all.parquet")
    if not os.path.exists(all_path):
        raise FileNotFoundError(f"Missing all-file: {all_path}")

    gdf = gpd.read_parquet(all_path)

    # Ensure CRS matches the grid (UTM) so buffer_m is in meters
    if gdf.crs is None:
        raise ValueError(f"{all_path} has no CRS; cannot tile safely.")
    if city_grid.crs is None:
        raise ValueError("city_grid has no CRS; cannot tile safely.")

    if gdf.crs != city_grid.crs:
        gdf = gdf.to_crs(city_grid.crs)
        
    grid_crs = city_grid.crs
    grid_crs_str = grid_crs.srs if hasattr(grid_crs, "srs") else str(grid_crs)

    # Build spatial index (huge speedup)
    sidx = gdf.sindex

    if predicate == "intersects":
        pred_fn = lambda sub, geom: sub.intersects(geom)
    elif predicate == "within":
        pred_fn = lambda sub, geom: sub.within(geom)
    else:
        raise ValueError("predicate must be 'intersects' or 'within'")

    for _, cell in city_grid.iterrows():
        gid = cell["ID"]
        tile_geom_4326 = cell["geometry"]  # shapely polygon in EPSG:4326
    
        # Build a GeoExtent bbox in 4326
        bbox_tile = GeoExtent(bbox=tile_geom_4326.bounds, crs="EPSG:4326")
    
        # Buffer in meters (via UTM internally), then return to 4326 bbox
        if buffer_m and buffer_m > 0:
            bbox_tile = bbox_tile.buffer_utm_bbox(buffer_m)
    
        bbox_4326 = bbox_tile.as_geographic_bbox()   # still a GeoExtent
    
        # Convert GeoExtent bbox -> shapely polygon in EPSG:4326
        clip_geom = box(*bbox_4326.bounds)
    
        # spatial index query in EPSG:4326 bounds (matches gdf if gdf is 4326)
        minx, miny, maxx, maxy = clip_geom.bounds
        cand_idx = list(sidx.intersection((minx, miny, maxx, maxy)))
    
        if not cand_idx:
            out = gdf.iloc[0:0].copy()
        else:
            sub = gdf.iloc[cand_idx]
            if predicate == "intersects":
                mask = sub.geometry.intersects(clip_geom)
            else:  # "within"
                mask = sub.geometry.within(clip_geom)
            out = sub[mask]
    
        out_path = os.path.join(layer_dir, f"{layer}_{gid}.parquet")
        out.to_parquet(out_path, index=False, compression=compression)
        
def _malloc_trim():
    """Return freed heap memory to the OS (Linux/glibc only)."""
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
    except Exception:
        pass


def get_data(city, output_base=".", batch_size=5):

    data_path = os.path.join(output_base, "data")

    # Get city_polygon -------------------------------------------------
    city_polygon = get_city_polygon(city, data_path=data_path, copy_to_s3=True)

    # Create the grid for the city -------------------------------------------------
    # City grid is in 4326
    print("creating city grid")
    city_grid = create_grid_for_city(city, city_polygon, data_path=data_path, copy_to_s3=True)

    # Get city grid in epsg 4326 for OSM data
    # city_grid_4326 = city_grid.to_crs("EPSG:4326")
    minx, miny, maxx, maxy = city_grid.total_bounds
    bbox = GeoExtent(bbox=(minx, miny, maxx, maxy), crs="EPSG:4326")

    print("getting roads from OSM")
    get_roads(city, bbox, grid_cell_id="all", data_path=data_path, copy_to_s3=True)

    print("getting open space from OSM")
    get_open_space(city, bbox, grid_cell_id="all", data_path=data_path, copy_to_s3=True)

    print("getting water from OSM")
    get_water(city, bbox, grid_cell_id="all", data_path=data_path, copy_to_s3=True)

    print("getting parking from OSM")
    get_parking(city, bbox, grid_cell_id="all", data_path=data_path, copy_to_s3=True)

    summarize_average_lanes(city, data_path=data_path, copy_to_s3=True)

    # Start a dask client
    from dask.distributed import Client, LocalCluster
    from dask import delayed
    import dask

    cluster = LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        memory_limit="12GB",
        dashboard_address=":0",
        local_directory=f"/tmp/dask-spill-{os.getuid()}",
    )

    client = Client(cluster)

    print(f"Dask: {client}")
    print(f"Dask dashboard: {client.dashboard_link}")
    
    # ---- precompute everything once (FAST)
    per_cell = []
    grid_crs = city_grid.crs
    grid_crs_str = grid_crs.srs if hasattr(grid_crs, "srs") else str(grid_crs)

    for _, cell in city_grid.iterrows():
        gid = cell["ID"]
        geom = cell["geometry"]

        bbox_tile = GeoExtent(bbox=geom.bounds, crs=grid_crs_str)
        bbox_fetch = bbox_tile.buffer_utm_bbox(10)

        per_cell.append((gid, bbox_fetch))

    print(f"Prepared {len(per_cell)} cells.")
    print("=" * 60)

    try:
        # ---- LIGHT tasks (run all at once; this is where speed comes from)
        light_tasks = []
        for gid, bbox_fetch in per_cell:
            light_tasks.extend([
                delayed(get_buildings)(city, bbox_fetch, gid, data_path=data_path, copy_to_s3=True),
                delayed(get_urban_land_use)(city, bbox_fetch, gid, data_path=data_path, copy_to_s3=False),
                delayed(get_anbh)(city, bbox_fetch, grid_cell_id=gid, data_path=data_path, copy_to_s3=False),
            ])

        print(f"Running LIGHT tasks: {len(light_tasks)}")
        dask.compute(*light_tasks)
        print("LIGHT tasks complete.")
        print("-" * 60)
        
    finally:
        client.close()
        cluster.close()

    # ---- ESA tasks (throttle to avoid worker deaths)
    import gc
    import math
    from dask.distributed import wait
    
    # ---- ESA tasks: parallel, but keep memory under control
    ESA_CONCURRENCY   = 2   # start 2, then 4
    ESA_RESTART_EVERY = 0   # with threads, usually no restart needed
    
    esa_cluster = LocalCluster(
        n_workers=1,
        threads_per_worker=ESA_CONCURRENCY,
        processes=False,
        dashboard_address=":0",
        local_directory=f"/tmp/dask-esa-spill-{os.getuid()}",
    )
    esa_client = Client(esa_cluster)
    
    try:
        esa_total = len(per_cell)
        completed = 0
    
        for i in range(0, esa_total, ESA_CONCURRENCY):
            chunk = per_cell[i : i + ESA_CONCURRENCY]
            chunk_ids = [gid for gid, _ in chunk]
            print(f"ESA batch {i//ESA_CONCURRENCY + 1}/{math.ceil(esa_total/ESA_CONCURRENCY)}: {chunk_ids}")
    
            futures = [
                esa_client.submit(
                    get_esa, city, bbox_fetch,
                    grid_cell_id=gid, data_path=data_path, copy_to_s3=False
                )
                for gid, bbox_fetch in chunk
            ]
    
            wait(futures)
            for f in futures:
                f.result()  # raise if failed
    
            completed += len(chunk)  # <-- FIX: do this before deleting futures
    
            esa_client.cancel(futures)
            del futures
            gc.collect()
            esa_client.run(_malloc_trim)
    
        print("ESA tasks complete.")
    
    finally:
        esa_client.close()
        esa_cluster.close()

    # Create per-tile vector files from all-files
    make_vector_tiles_from_all(city, data_path, "roads", city_grid)
    make_vector_tiles_from_all(city, data_path, "open_space", city_grid)
    make_vector_tiles_from_all(city, data_path, "water", city_grid)
    make_vector_tiles_from_all(city, data_path, "parking", city_grid)

    merge_building_tiles(city, data_path=data_path, copy_to_s3=True)


def _parse_args():
    parser = argparse.ArgumentParser(description="Fetch and process city data.")
    parser.add_argument("city", help="City identifier, e.g. ZAF-Durban")
    parser.add_argument(
        "--output-base",
        default=".",
        help="Base directory for output data (default: current directory).",
    )
    # Optional, but you can ignore it if you want (it defaults to 5)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="How many grid cells to process per batch (default: 5).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    get_data(args.city, output_base=args.output_base, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
