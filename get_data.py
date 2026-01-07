
import os
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
    # city = "ZAF-Durban"
    data_path = os.path.join(output_base, "data")
    # copy_to_s3 = True

    

    # Get city_polygon -------------------------------------------------
    city_polygon = get_city_polygon(city, data_path=data_path, copy_to_s3=True)

    # Get UTM -------------------------------------------------
    from utils.utm import get_utm
    utm_info = get_utm(city_polygon)
    print(f"UTM EPSG: {utm_info['epsg']}, Earth Engine: {utm_info['ee']}")
    #crs = utm_info['ee']
    crs = 'EPSG:4326'
    
    # Download vector data
    minx, miny, maxx, maxy = city_polygon.total_bounds  
    bbox = GeoExtent(bbox=[minx, miny, maxx, maxy])
    
    get_roads(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    get_open_space(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    get_water(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    # # get_buildings(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    get_parking(city, bbox, grid_cell_id = "all",  data_path=data_path, copy_to_s3=True)
    
    summarize_average_lanes(city, data_path=data_path, copy_to_s3=True)

    # Create the grid for the city -------------------------------------------------
    city_grid = create_grid_for_city(city, city_polygon, data_path=data_path, copy_to_s3=True)

    # Get existing city grid from file


    # Check file with https://geojson.io/ or https://mapshaper.org/

    # Start a dask client
    from dask.distributed import Client
    from dask import delayed
    import dask
    client = Client()

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
            geometry = cell['geometry']
            # Get the bounding box of the cell
            bbox = GeoExtent(bbox=geometry.bounds)
            
            geographic_box = bbox.as_geographic_bbox()
            geographic_bbox_str = ','.join(map(str, geographic_box.bounds))
            print(f"Preparing tasks for grid cell {grid_cell_id} with bbox: {geographic_bbox_str}")
            
            # Create all data fetching tasks for this cell
            # roads_task = delayed(get_roads)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            # open_space_task = delayed(get_open_space)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            # water_task = delayed(get_water)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            buildings_task = delayed(get_buildings)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=True)
            urban_land_use_task = delayed(get_urban_land_use)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=False)
            esa_task = delayed(get_esa)(city, bbox, grid_cell_id=grid_cell_id, data_path=data_path, copy_to_s3=False)
            anbh_task = delayed(get_anbh)(city, bbox, grid_cell_id=grid_cell_id, data_path=data_path, copy_to_s3=False)
            # parking_task = delayed(get_parking)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)

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

# if __name__ == '__main__':
#     main()

