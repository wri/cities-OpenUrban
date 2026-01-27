import os
import argparse
import numpy as np

from city_metrix.metrix_model import GeoExtent




import importlib, utils.download as download
importlib.reload(download)

from utils.download import *
from utils.grid import create_grid_for_city

import os, glob
import geopandas as gpd
import pandas as pd


def get_data(city, output_base="."):
    city = "NLD-Rotterdam"
    data_path = os.path.join(output_base, "data")
    # copy_to_s3 = True

    

    # Get city_polygon -------------------------------------------------
    city_polygon = get_city_polygon(city, data_path=data_path, copy_to_s3=True)

    # Get UTM -------------------------------------------------
    # from utils.utm import get_utm
    # utm_info = get_utm(city_polygon)
    # print(f"UTM EPSG: {utm_info['epsg']}, Earth Engine: {utm_info['ee']}")
    # #crs = utm_info['ee']
    # crs = 'EPSG:4326'
    
    # Download vector data
    
    # Create the grid for the city -------------------------------------------------
    # City grid is in UTM
    city_grid = create_grid_for_city(city, city_polygon, data_path=data_path, copy_to_s3=True)
    city_grid_4326 = city_grid.to_crs("EPSG:4326")

    minx, miny, maxx, maxy = city_grid_4326.total_bounds
    bbox = GeoExtent(bbox=(minx, miny, maxx, maxy), crs="EPSG:4326")
    
    get_roads(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    get_open_space(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    get_water(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    # # get_buildings(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    get_parking(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    
    summarize_average_lanes(city, data_path=data_path, copy_to_s3=True)

    # Get existing city grid from file


    # Check file with https://geojson.io/ or https://mapshaper.org/

    # Start a dask client
    from dask.distributed import Client, LocalCluster
    from dask import delayed
    import dask
    
    cluster = LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=True,
        memory_limit="5GB",
        dashboard_address=":0",
        local_directory=f"/tmp/dask-spill-{__import__('os').getuid()}",
    )
    
    client = Client(cluster)


    # Print Dask client information
    print(f"Dask: {client}")
    print(f"Dask dashboard: {client.dashboard_link}")

    try:
        # Create all tasks for all cells and all data types to run in parallel
        print("Creating all data fetching tasks for all grid cells...")
        all_tasks = []
        task_descriptions = []
        
        for idx, cell in city_grid.iterrows():
            grid_cell_id = cell['ID']
            print(f"Preparing tasks for grid cell {grid_cell_id}")
            
            # Get the bounding box of the cell
            grid_crs = city_grid.crs  # UTM CRS, e.g. "EPSG:32631" etc.

            geometry = cell["geometry"]
            
            # tile bbox in UTM (correct)
            bbox = GeoExtent(bbox=geometry.bounds, crs=grid_crs.srs if hasattr(grid_crs, "srs") else str(grid_crs))
            
            # Buffered bbox
            bbox_fetch = bbox.buffer_utm_bbox(10)
            # Buffered bbox back in 4326 for vector APIs that require lon/lat
            bbox_fetch_4326 = bbox_fetch.as_geographic_bbox()
            
            # Create all data fetching tasks for this cell
            buildings_task = delayed(get_buildings)(city, bbox_fetch_4326, grid_cell_id, data_path=data_path, copy_to_s3=True)
            urban_land_use_task = delayed(get_urban_land_use)(city, bbox, bbox_fetch, grid_cell_id, data_path=data_path, copy_to_s3=False)
            esa_task = delayed(get_esa)(city, bbox, bbox_fetch, grid_cell_id=grid_cell_id, data_path=data_path, copy_to_s3=False)
            anbh_task = delayed(get_anbh)(city, bbox, bbox_fetch, grid_cell_id=grid_cell_id, data_path=data_path, copy_to_s3=False)

            # Add all tasks to the master list
            all_tasks.extend([buildings_task, urban_land_use_task, esa_task, anbh_task])

            # Keep track of what each task does for debugging
            task_descriptions.extend([
                f"buildings_cell_{grid_cell_id}",
                f"urban_land_use_cell_{grid_cell_id}",
                f"esa_cell_{grid_cell_id}",
                f"anbh_cell_{grid_cell_id}"
            ])
        
        # Execute ALL tasks in parallel across all cells and all data types
        print(f"Executing ALL {len(all_tasks)} tasks in parallel...")
        print(f"This includes {len(city_grid)} cells × 6 data types = {len(all_tasks)} total tasks")
        print("=" * 60)

        visualize = False
        if visualize:
            print("Visualizing task graph...")
            try:
                dask.visualize(*all_tasks, filename='task_graph.png', optimize_graph=False)
                print("Task graph saved as 'task_graph.png' - check it out!")
            except Exception as e:
                print(f"Visualization failed: {e}")
        else:
        
            # Now execute the actual computation
            print("Starting actual computation...")
            results = dask.compute(*all_tasks)

            print("ALL data fetching completed!")
            print(f"Successfully executed {len(results)} tasks in parallel")
            print("=" * 60)

    finally:
        # Close the Dask client
        client.close()
        
    merge_building_tiles(city, data_path=data_path, keep_tiles=True, copy_to_s3=True)

def _parse_args():
    parser = argparse.ArgumentParser(description="Fetch and process city data.")
    parser.add_argument("city", help="City identifier, e.g. ZAF-Durban")
    parser.add_argument(
        "--output-base",
        default=".",
        help="Base directory for output data (default: current directory).",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    get_data(args.city, output_base=args.output_base)


if __name__ == "__main__":
    main()
