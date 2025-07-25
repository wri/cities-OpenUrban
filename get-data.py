import os
import numpy as np

from city_metrix.metrix_model import GeoExtent

from utils.download import *
from utils.grid import create_grid_for_city

def main():
    data_path = 'data'
    copy_to_s3 = True

    city = 'ARG-Buenos_Aires'

    # Get city_polygon -------------------------------------------------
    city_polygon = get_city_polygon(city, data_path=data_path, copy_to_s3=copy_to_s3)

    # Get UTM -------------------------------------------------
    from utils.utm import get_utm
    utm_info = get_utm(city_polygon)
    print(f"UTM EPSG: {utm_info['epsg']}, Earth Engine: {utm_info['ee']}")
    #crs = utm_info['ee']
    crs = 'EPSG:4326'

    # Create the grid for the city -------------------------------------------------
    city_grid = create_grid_for_city(city, city_polygon, data_path=data_path, copy_to_s3=copy_to_s3)

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
            roads_task = delayed(get_roads)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            open_space_task = delayed(get_open_space)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            water_task = delayed(get_water)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            buildings_task = delayed(get_buildings)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            urban_land_use_task = delayed(get_urban_land_use)(city, bbox, grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            esa_task = delayed(get_esa)(city, bbox, grid_cell_id=grid_cell_id, data_path=data_path, copy_to_s3=copy_to_s3)
            
            # Add all tasks to the master list
            all_tasks.extend([roads_task, open_space_task, water_task, buildings_task, urban_land_use_task, esa_task])
            
            # Keep track of what each task does for debugging
            task_descriptions.extend([
                f"roads_cell_{grid_cell_id}",
                f"open_space_cell_{grid_cell_id}",
                f"water_cell_{grid_cell_id}",
                f"buildings_cell_{grid_cell_id}",
                f"urban_land_use_cell_{grid_cell_id}",
                f"esa_cell_{grid_cell_id}"
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

if __name__ == '__main__':
    main()
