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
        geom = cell["geometry"]

        bbox_tile = GeoExtent(bbox=geom.bounds, crs=grid_crs_str)
            
        if buffer_m and buffer_m > 0:
            geom = bbox_tile.buffer_utm_bbox(10)
            
        geom = geom.as_geographic_bbox()

        minx, miny, maxx, maxy = geom.bounds
        cand_idx = list(sidx.intersection((minx, miny, maxx, maxy)))
        if not cand_idx:
            out = gdf.iloc[0:0].copy()
        else:
            sub = gdf.iloc[cand_idx]
            out = sub[pred_fn(sub, geom)]

        out_path = os.path.join(layer_dir, f"{layer}_{gid}.parquet")
        out.to_parquet(out_path, index=False, compression=compression)


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
        n_workers=4,
        threads_per_worker=1,
        processes=True,
        memory_limit="12GB",
        dashboard_address=":0",
        local_directory=f"/tmp/dask-spill-{os.getuid()}",
    )

    client = Client(cluster)

    print(f"Dask: {client}")
    print(f"Dask dashboard: {client.dashboard_link}")

    try:
        # ---- precompute everything once (FAST)
        per_cell = []
        grid_crs = city_grid.crs
        grid_crs_str = grid_crs.srs if hasattr(grid_crs, "srs") else str(grid_crs)

        for _, cell in city_grid.iterrows():
            gid = cell["ID"]
            geom = cell["geometry"]

            bbox_tile = GeoExtent(bbox=geom.bounds, crs=grid_crs_str)
            bbox_fetch = bbox_tile.buffer_utm_bbox(10)
            bbox_fetch_4326 = bbox_fetch.as_geographic_bbox()

            per_cell.append((gid, bbox_fetch, bbox_fetch_4326))

        print(f"Prepared {len(per_cell)} cells.")
        print("=" * 60)

        # ---- LIGHT tasks (run all at once; this is where speed comes from)
        light_tasks = []
        for gid, bbox_fetch, bbox_fetch_4326 in per_cell:
            light_tasks.extend([
                delayed(get_buildings)(city, bbox_fetch_4326, gid, data_path=data_path, copy_to_s3=True),
                delayed(get_urban_land_use)(city, bbox_fetch, gid, data_path=data_path, copy_to_s3=False),
                delayed(get_anbh)(city, bbox_fetch, grid_cell_id=gid, data_path=data_path, copy_to_s3=False),
            ])

        print(f"Running LIGHT tasks: {len(light_tasks)}")
        dask.compute(*light_tasks)
        print("LIGHT tasks complete.")
        print("-" * 60)

        # ---- ESA tasks (throttle to avoid worker deaths)
        ESA_CONCURRENCY = 1  # set to 2 only if stable

        esa_total = len(per_cell)
        for i in range(0, esa_total, ESA_CONCURRENCY):
            chunk = per_cell[i:i + ESA_CONCURRENCY]
            chunk_ids = [gid for gid, *_ in chunk]
            print(f"ESA chunk {i//ESA_CONCURRENCY + 1}/{int(np.ceil(esa_total/ESA_CONCURRENCY))}: {chunk_ids}")

            esa_tasks = [
                delayed(get_esa)(city, bbox_fetch, grid_cell_id=gid, data_path=data_path, copy_to_s3=False)
                for gid, bbox_fetch, _ in chunk
            ]
            dask.compute(*esa_tasks)

        print("ESA tasks complete.")
        print("=" * 60)

    finally:
        client.close()


    # Create per-tile vector files from all-files
    make_vector_tiles_from_all(city, data_path, "roads", city_grid, buffer_m=10)
    make_vector_tiles_from_all(city, data_path, "open_space", city_grid, buffer_m=10)
    make_vector_tiles_from_all(city, data_path, "water", city_grid, buffer_m=10)
    make_vector_tiles_from_all(city, data_path, "parking", city_grid, buffer_m=10)

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
