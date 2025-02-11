
# Create LULC dataset for city --------------------------------------------

library(here)
library(tidyverse)
library(sf)
library(terra)
library(osmdata)
library(rgee)
library(googledrive)




# Set up GEE --------------------------------------------------------------

# Set environment
# reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley")
ee_Initialize("ejwesley", drive = TRUE, gcs = TRUE)

# LULC raster creation function -------------------------------------------

# city = city of interest
# epsg = state plane EPSG for city

create_LULC <- function(city, epsg){

  # City boundary -----------------------------------------------------------
  
  # Choose city of interest, use spaces
  city_name <- paste0("USA-", str_replace(str_replace(city, " ", "_"), "-", "_"))
  
  print(city_name)
  
  epsg_ee <- paste0("EPSG:", epsg)
  
  geom <- cities %>% 
    filter(MetroName == city) %>% 
    pull(geometry)
  
  # Create local city folder to store data
  # path <- here("cities", city)
  path <- paste0("~/Desktop/LULC-SSC/", city_name)
  
  if (!dir.exists(path)){
    dir.create(path)
  } else{
    print("dir exists")
  }
  
  city_grid <- geom %>% 
    st_make_grid(cellsize = c(0.15, 0.15), what = "polygons") %>% 
    st_sf() %>% 
    st_filter(geom) %>% 
    mutate(ID = row_number())
  
  ggplot() +
    geom_sf(data = geom) +
    geom_sf(data = city_grid, fill = NA) +
    geom_sf_label(data = city_grid, aes(label = ID))
  
  # Road download & avg lane calculation ------------------------------------
  
  
  # Download roads and calculate mean lanes per highway class for the city
  road_path <- paste0(path, "/roads")
  
  if (!dir.exists(road_path)){
    dir.create(road_path)
  } else{
    print("dir exists")
  }
  
  road_lanes <- tibble()

  for (i in 1:length(city_grid$ID)){
    # Buffer grid cell so there will be overlap
    aoi <- city_grid %>%
      filter(ID == i) %>%
      st_as_sf()

    bb <- st_bbox(aoi)

    # Values from https://taginfo.openstreetmap.org/keys/highway#values
    # Chosen based on description, excluding footways, etc.
    # Values w/o descriptions not included

    # get road lanes data from OSM for gridcell
    roads_grid <- opq(bb, timeout = 100) %>%
      add_osm_feature(key = 'highway',
                      value = c("residential",
                                "service",
                                "unclassified",
                                "tertiary",
                                "secondary",
                                "primary",
                                "turning_circle",
                                "living_street",
                                "trunk",
                                "motorway",
                                "motorway_link",
                                "trunk_link",
                                "primary_link",
                                "secondary_link",
                                "tertiary_link",
                                "motorway_junction",
                                "turning_loop",
                                "road",
                                "mini_roundabout",
                                "passing_place",
                                "busway")) %>%
      osmdata_sf() %>%
      # cast polygons as lines
      osm_poly2line() %>%
      # retain only unique geometries (no overlap in hierarchies)
      unique_osmdata()
    
    if(is.null(nrow(roads_grid$osm_lines))){
      next
    }

    # select lines geometries
    roads_grid <- roads_grid$osm_lines %>%
      dplyr::select(any_of(c("highway", "lanes"))) %>%
      # an inline anonymous function to add the needed columns
      (function(.df){
        cls <- c("lanes") # columns I need
        # adding cls columns with NAs if not present in the piped data.frame
        .df[cls[!(cls %in% colnames(.df))]] = NA
        return(.df)
      }) %>%
      mutate(lanes = as.numeric(lanes))
    
    roads_grid <- st_make_valid(roads_grid)

    # Save roads vectors to use later
    roads_grid_file <- paste0(road_path, "/roads_", i, ".geojson")
    st_write(obj = roads_grid,
             dsn = roads_grid_file,
             append = FALSE,
             delete_dsn = TRUE)

  }
  
  road_lanes <- list.files(here(road_path), full.names = TRUE, pattern = "geojson") %>%
    map(\(x) st_read(x) %>%
          st_drop_geometry() %>% 
          mutate(lanes = as.numeric(lanes))) %>%
    reduce(bind_rows)
  
  # Get the avg number of lanes per highway class
  lanes <- road_lanes %>% 
    group_by(highway) %>% 
    summarize(avg.lanes = ceiling(mean(lanes, na.rm = TRUE))) %>% 
    mutate(avg.lanes = case_when(is.na(avg.lanes) ~ 2,
                                 !is.na(avg.lanes) ~ avg.lanes))
  
  rm(road_lanes)
  
  write_csv(lanes, paste0(road_path, "/average_lanes.csv"))
  # lanes <- read_csv(paste0(road_path, "/average_lanes.csv"))

  # Iterate over grid -------------------------------------------------------

  
  for (i in 1:length(city_grid$ID)){
    print(paste0(city_name, " grid ", i))
    # Buffer grid cell so there will be overlap
    aoi <- city_grid %>% 
      filter(ID == i) %>% 
      st_as_sf() %>% 
      st_buffer(dist = 10) 
    
    # st_write(aoi, here("data", "aoi_la.shp"))
    
    bb <- st_bbox(aoi)
    
    bb_ee <- sf_as_ee(st_as_sfc(bb))
    
    # ggplot() +
    #   geom_sf(data = city.geom) +
    #   geom_sf(data = aoi, fill = NA)
    
    # ESA Worldcover ----------------------------------------------------------
    
    # Read ESA land cover (2021)
    esa <- ee$ImageCollection('ESA/WorldCover/v200')
    
    # Clip to city of interest
    esa_city <- esa$filterBounds(bb_ee)
    
    # ImageCollection to image
    esa_city <- esa_city$mosaic()
    
    # Reclassify raster to combine greenspace types
    
    ## [ND, Tree, Shrubland, Grassland, Cropland, Built up, Bare/sparse vegetation,
    ##  Snow/ice, Permanent water bodies, Herbaceous wetland, Mangroves, Moss and lichen]
    FROM <- c(0, 10, 20, 30, 40, 50, 60, 80, 70, 90, 95, 100)
    
    # ND: 0 
    # Green: 1 (Tree, Shrubland, Grassland, Cropland)
    # Built up: 2 (Built up)
    # Barren: 3 (Bare/sparse vegetation, Moss and lichen)
    # Water: 20 (Snow/ice, Permanent water bodies, Herbaceous wetland, Mangroves)
    # water code matches eventual landuse code because the category matches
    TO <- c(0, 110, 110, 110, 110, 120, 130, 300, 300, 300, 300, 130)
    
    esa_city <- esa_city$
      remap(FROM, TO)
    
    # If all pixels in the ESA raster are water, skip this tile
    esa_values <- esa_city$
      reduceRegion(
        reducer = ee$Reducer$frequencyHistogram(),
        geometry = bb_ee,
        scale = 10, 
        maxPixels = 1e13
      )$getInfo()
    
    unique_values_list <- names(esa_values[[1]]) %>% 
      as.numeric()
    
    if (sum(unique_values_list != 300) == 0){
      next
    }
    
    esa_img <- ee_image_to_drive(esa_city,
                                 description = "ESA_1m",
                                 folder = "LULC_grid_SSC",
                                 timePrefix = FALSE,
                                 region = bb_ee,
                                 scale = 1,
                                 crs = epsg_ee,
                                 maxPixels = 10e10)
    
    esa_img$start()
    
    # Move from drive to local
    ee_drive_to_local(task = ee_monitoring(esa_img, max_attempts = 1000), 
                      dsn = here(path, "ESA_1m.tif"))
    
    # Delete from drive
    drive_rm("LULC_grid_SSC/ESA_1m.tif")
    
    print("ESA raster saved")
    
    esa_rast <- rast(here(path, "ESA_1m.tif"))
    
    # ggplot() +
    #   geom_sf(data = city.geom) +
    #   geom_sf(data = test.bb, fill = NA) +
    #   geom_sf(data = open_space)
    
    # Open space --------------------------------------------------------------
    
    
    ## Get OSM open space data -------------------------------------------------
    
    open_space1 <- opq(bb, timeout = 100) %>% 
      add_osm_feature(key = 'leisure',
                      value = c("pitch",
                                "park",
                                "garden",
                                "playground",
                                "nature_reserve",
                                "golf_course",
                                "common",
                                "dog_park",
                                "recreation_ground",
                                "disc_golf_course")) %>% 
      osmdata_sf()
    
    print("open space 1")
    
    open_space2 <- opq(bb, timeout = 100) %>% 
      add_osm_feature(key = 'boundary',
                      value = c('protected_area',
                                'national_park',
                                'forest_compartment',
                                'forest')) %>% 
      osmdata_sf()
    
    print("open space 2")
    
    open_space <- c(open_space1, open_space2)
    
    rm(open_space1)
    rm(open_space2)
    
    ## Tidy --------------------------------------------------------------------
    
    if (!is.null(nrow(open_space$osm_polygons)) & !is.null(nrow(open_space$osm_multipolygons))){
      # if there are polygons & multipolygons
      open_space <- open_space$osm_polygons %>% 
        bind_rows(st_cast(open_space$osm_multipolygons, "POLYGON")) 
      
    } else if (!is.null(nrow(open_space$osm_polygons)) & is.null(nrow(open_space$osm_multipolygons))){
      # if there are polygons but no multipolygons
      open_space <- open_space$osm_polygons
      
    } else if (is.null(nrow(open_space$osm_polygons)) & !is.null(nrow(open_space$osm_multipolygons))){
      # if there are multipolygons but no polygons
      
      open_space <- st_cast(open_space$osm_multipolygons, "POLYGON")
      
    } else {
      # if there are neither keep empty geometry
      open_space <- open_space$osm_polygons
    }
    
    # Add value = 200 as an attribute if there is open space data, 
    # otherwise use the bounding box with value = 0
    if (!is.null(open_space)){
      open_space <- open_space %>% 
        st_transform(epsg) %>% 
        mutate(Value = 200) %>% 
        select(any_of(c("Value", "osm_id", "leisure", "sport", "surface")))
    } else {
      open_space <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(epsg) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    open_space <- st_make_valid(open_space)
    
    print("open space combined polygons")
    
    ## Save open space polygons ####
    openspace_path <- paste0("~/Desktop/LULC-SSC/", city_name, "/openspace/")
    
    if (!dir.exists(openspace_path)){
      dir.create(openspace_path)
    } else{
      print("dir exists")
    }
    
    openspace_file <- paste0(openspace_path, "/openspace_", i, ".geojson")
    
    st_write(obj = open_space,
             dsn = openspace_file,
             append = FALSE,
             delete_dsn = TRUE)
    
    open_space %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here(path, "open_space_1m.tif"),
                overwrite = TRUE)
    
    rm(open_space)
    
    print("Open space raster saved")
    
    # Water -------------------------------------------------------------------
    
    ## Get OSM water data --------------------------------------------------------------
    
    # get water from OSM
    water1 <- opq(bb, timeout = 100) %>% 
      add_osm_feature(key = 'water') %>% 
      osmdata_sf()
    
    print("water osm")
    
    water2 <- opq(bb, timeout = 100) %>% 
      add_osm_feature(key = 'natural', 
                      value = c('water')) %>% 
      osmdata_sf()
    
    print("water 2 osm")
    
    water3 <- opq(bb, timeout = 100) %>% 
      add_osm_feature(key = 'waterway') %>% 
      osmdata_sf()
    
    print("water 3 osm")
    
    water <- c(water1, water2, water3)
    
    rm(water1)
    rm(water2)
    rm(water3)
    
    # if there are polygons but no multipolygons
    if (!is.null(nrow(water$osm_polygons)) & !is.null(nrow(water$osm_multipolygons))){
      # if there are polygons & multipolygons
      water <- water$osm_polygons %>% 
        bind_rows(st_cast(water$osm_multipolygons, "POLYGON")) 
      
    } else if (!is.null(nrow(water$osm_polygons)) & is.null(nrow(water$osm_multipolygons))){
      water <- water$osm_polygons
      
    } else if (is.null(nrow(water$osm_polygons)) & !is.null(nrow(water$osm_multipolygons))){
      # if there are multipolygons but no polygons
      water <- st_cast(water$osm_multipolygons, "POLYGON")
      
    } else {
      # if there are neither keep empty geometries
      water <- water$osm_polygons
      
    }
    
    if (!is.null(water)){
      water <- water %>% 
        st_transform(epsg) %>% 
        mutate(Value = 300) %>% 
        select(any_of(c("Value", "osm_id")))
    } else {
      water <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(epsg) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    print("water combined polygons")
    
    water %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here(path, "water_1m.tif"),
                overwrite = TRUE)
    
    rm(water)
    
    print("water raster saved")
    
    
    
    # Roads -------------------------------------------------------------------
    
    
    ## Load roads data ----------------------------------------------------------------
    
    roads_grid_file <- paste0(road_path, "/roads_", i, ".geojson")
    
    if (file.exists(roads_grid_file)){
      
      roads <- st_read(roads_grid_file)
      
      ## Tidy --------------------------------------------------------------------
      
      
      # Fill lanes with avg lane value when missing
      roads <- roads %>% 
        mutate(lanes = as.numeric(lanes)) %>% 
        left_join(lanes, by = "highway") %>% 
        mutate(lanes = coalesce(lanes, avg.lanes))
      
      # Add value field (20)
      roads <- roads %>% 
        mutate(Value = 500)
      
      # Reproject 
      roads <- roads %>% 
        st_transform(epsg) 
      
      print("roads reproject")
      
      
      ## Buffer ------------------------------------------------------------------
      
      # Buffer roads by lanes * 10 ft (3.048 m) 
      # https://nacto.org/publication/urban-street-design-guide/street-design-elements/lane-width/#:~:text=wider%20lane%20widths.-,Lane%20widths%20of%2010%20feet%20are%20appropriate%20in%20urban%20areas,be%20used%20in%20each%20direction
      # cap is flat to the terminus of the road
      # join style is mitred so intersections are squared
      if (st_crs(roads)$units == "us-ft"){
        width = 10
      } else if (st_crs(roads)$units == "ft"){
        width = 10
      } else if (st_crs(roads)$units == "m"){
        width = 3.048
      } 
      
      roads <- roads %>% 
        st_buffer(dist = roads$lanes * (width / 2),
                  endCapStyle = "FLAT",
                  joinStyle = "MITRE") %>% 
        select(Value)
      
      print("roads buffered")
      
    } else {
      roads <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(epsg) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    ## Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    roads %>% rasterize(esa_rast, 
                        field = "Value",
                        background = 0, 
                        filename = here(path, "roads_1m.tif"),
                        overwrite = TRUE)
    
    rm(roads)
    
    print("Roads raster saved")

    
    # Buildings -------------------------------------------------------------------
    
    ## Get overture buildings data --------------------------------------------------------------
    
    # get bounding box string
    bb_string <- bb %>% 
      as.character() %>% 
      paste(collapse = ',')
    
    building_file <- here(path, "overture.geojson")
    
    # Define the command to run the Python tool
    command <- paste0("/Users/WRI/anaconda3/bin/overturemaps download --bbox=", bb_string, " -f geojson --type=building -o ", building_file)
    
    # Run the Python command
    system(command)
    
    buildings <- st_read(building_file)
    
    print("buildings Overture")
    
    if (nrow(buildings) > 0){
      
      # drop excess fields
      buildings <- buildings %>% 
        select(any_of(c("id", "subtype", "class")))
      
      ## Average building height from the global human settlement layer ----------
      
      
      # ANBH is the average height of the built surfaces, USE THIS
      # AGBH is the amount of built cubic meters per surface unit in the cell
      # https://ghsl.jrc.ec.europa.eu/ghs_buH2023.php
      
      anbh <- ee$ImageCollection("projects/wri-datalab/GHSL/GHS-BUILT-H-ANBH_R2023A")$
        filterBounds(bb_ee)$ 
        select('b1')
      
      anbh_proj <- anbh$first()$projection()
      
      anbh <- anbh$
        mosaic()$
        reproject(crs = anbh_proj, scale = 100)$
        clip(bb_ee)

      anbh_img <- ee_image_to_drive(anbh,
                                    description = "ANBH_100m",
                                    timePrefix = FALSE,
                                    region = bb_ee,
                                    scale = 100,
                                    maxPixels = 10e10)
      
      anbh_img$start()
      
      # Move from GCS to local
      ee_drive_to_local(task = ee_monitoring(anbh_img, max_attempts = 1000), 
                        dsn = here(path, "ANBH_100m.tif"),
                        overwrite = TRUE)
      
      print("ANBH raster saved")
      
      anbh <- rast(here(path, "ANBH_100m.tif"))
      
      drive_rm("rgee_backup/ANBH_100m.tif")
      
      ## Get ULU data ---------------------------------------------------
      
      # Read ULU land cover, filter to city, select lulc band
      ulu <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
        filterBounds(bb_ee)$
        select('lulc')
      
      if (ulu$size()$getInfo() > 1){
        
        ulu_proj <- ulu$first()$projection()
        
        ulu <- ulu$
          reduce(ee$Reducer$firstNonNull())$
          reproject(crs = ulu_proj, scale = 5)$
          rename('lulc')
        
        # Reclassify raster to combine lulc types
        # 0-Open space 
        # 1-Non-residential 
        # 2-Atomistic 
        # 3-Informal subdivision 
        # 4-Formal subdivision
        # 5-Housing project 
        
        FROM = c(0, 1, 2, 3, 4, 5)
        
        # 0-Unclassified: 0 (open space)
        # 1-Residential: 2 (Atomistic), 3 (Informal), 4 (Formal), 5 (Housing project)
        # 2-Non-residential: 1 (non-res)
        
        TO = c(0, 2, 1, 1, 1, 1)
        
        ulu = ulu$remap(FROM, TO)
        
        ulu_img <- ee_image_to_drive(ulu,
                                     description = "ULU_5m",
                                     timePrefix = FALSE,
                                     region = bb_ee,
                                     scale = 5,
                                     maxPixels = 1e13)
        
        ulu_img$start()
        
        # Move from GCS to local
        ee_drive_to_local(task = ee_monitoring(ulu_img, max_attempts = 1000), 
                          dsn = here(path, "ULU_5m.tif"),
                          overwrite = TRUE)
        
        print("ULU raster saved")
        
        ulu <- rast(here(path, "ULU_5m.tif"))
        
        drive_rm("rgee_backup/ULU_5m.tif")
      } else {
        
        ulu <- rast(ext(anbh), 
                    res = res(anbh), 
                    crs = crs(anbh))
        
        # Assign a single value to all cells
        values(ulu) <- 0
        
        # Save the new raster (optional)
        writeRaster(ulu, here(path, "ULU_5m.tif"), overwrite = TRUE) 
        
        ulu <- rast(here(path, "ULU_5m.tif"))
      }

      # Classify as residential or non-residential from WRI ULU
      
      # Reproject buildings to match ULU
      buildings <- buildings %>% 
        st_transform(crs = st_crs(ulu)) 
      
      print("buildings reproject")
      
      # Extract values to buildings using exact_extract, as coverage fractions
      # https://github.com/isciences/exactextractr
      build_ulu <- exactextractr::exact_extract(ulu, 
                                                buildings, 
                                                'mode',
                                                force_df = TRUE) 
      
      print("buildings ulu extract")
      
      buildings <- buildings %>% 
        add_column(ULU = build_ulu$mode) 
      
      # Add value field and transform to epsg
      buildings <- buildings %>% 
        mutate(Value = ULU) %>% 
        st_transform(epsg)
      
      buildings <- st_make_valid(buildings)
      
      # Reproject buildings to match ANBH
      buildings <- buildings %>% 
        st_transform(crs = st_crs(anbh))
      
      # Extract average of pixel values to buildings
      # Mean value of cells that intersect the polygon, 
      # weighted by the fraction of the cell that is covered.
      avg_ht <- exactextractr::exact_extract(anbh, 
                                             buildings, 
                                             'mean')
      
      buildings <- buildings %>% 
        add_column(ANBH = avg_ht)
      
      print("buildings anbh")
      
      ## Tidy ---------------------------------------------------------------
      
      # Reproject to local state plane and calculate area
      ######### This is not correct
      ######### should be Area_m = Area_ft / 10.764
      buildings <- buildings %>% 
        st_transform(epsg) %>% 
        mutate(Area_ft = as.numeric(st_area(.)),
               Area_m = Area_ft / 3.281,
               ULU = as.factor(ULU),
               ULU = droplevels(replace(ULU, ULU == "0", NA)))
      
      print("buildings reproject")

      ## Classification of roof slope --------------------------------------------
      tree <- readRDS("~/Desktop/LULC-SSC/V2-building-class-tree.rds")
      
      buildings <- buildings %>% 
        add_column(Slope = predict(tree, newdata = buildings, type = "class")) 
      
      # For V3 reserve single digit space for classification
      buildings <- buildings %>% 
        mutate(Class = case_when(ULU == 2 ~ "non-residential",
                                 ULU == 1 ~ "residential",
                                 is.na(ULU) ~ "unclassified"),
               Value = case_when(Class == "unclassified" & Slope == "low" ~ 601,
                                 Class == "unclassified" & Slope == "high" ~ 602,
                                 Class == "residential" & Slope == "low" ~ 611,
                                 Class == "residential" & Slope == "high" ~ 612,
                                 Class == "non-residential" & Slope == "low" ~ 621,
                                 Class == "non-residential" & Slope == "high" ~ 622)) %>% 
        select(Value)
      
      print("buildings reclass")
 
      ## Save building vectors ---------------------------------------------------
      
      building_path <- paste0("~/Desktop/LULC-SSC/", city_name, "/buildings/")
      
      if (!dir.exists(building_path)){
        dir.create(building_path)
      } else{
        print("dir exists")
      }
      
      building_grid_file <- paste0(building_path, "/buildings_", i, ".geojson")
      st_write(obj = buildings,
               dsn = building_grid_file,
               append = FALSE,
               delete_dsn = TRUE)
      
    } else {
      buildings <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(epsg) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    
    
    ## Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    buildings %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here(path, "buildings_1m.tif"),
                            overwrite = TRUE)
    
    rm(buildings)
    
    print("Buildings raster saved")

    
    # Parking -----------------------------------------------------------------
    
    
    # get parking from OSM
    parking1 <- opq(bb, timeout = 100) %>% 
      add_osm_feature(key = 'amenity',
                      value = 'parking') %>% 
      osmdata_sf() 
    
    print("parking osm")
    
    parking2 <- opq(bb, timeout = 100) %>% 
      add_osm_feature(key = 'parking') %>% 
      osmdata_sf() 
    
    print("parking osm")
    
    parking <- c(parking1, parking2)
    
    
    # if there are polygons but no multipolygons
    if (!is.null(nrow(parking$osm_polygons)) & !is.null(nrow(parking$osm_multipolygons))){
      # if there are polygons & multipolygons
      parking <- parking$osm_polygons %>% 
        bind_rows(st_cast(parking$osm_multipolygons, "POLYGON"))
      
    } else if (!is.null(nrow(parking$osm_polygons)) & is.null(nrow(parking$osm_multipolygons))){
      
      parking <- parking$osm_polygons
      
    } else if (is.null(nrow(parking$osm_polygons)) & !is.null(nrow(parking$osm_multipolygons))){
      # if there are multipolygons but no polygons
      
      parking <- st_cast(parking$osm_multipolygons, "POLYGON")
      
    } else {
      # if there are neither create raster of 0s
      
      parking <- parking$osm_polygons
    }
    
    if (!is.null(parking)){
      parking <- parking %>% 
        st_transform(epsg) %>% 
        mutate(Value = 400) %>% 
        select(any_of(c("Value", "osm_id", "amenity", "parking", "surface")))
    } else {
      parking <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(epsg) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    parking <- st_make_valid(parking)
    
    ## Save parking vectors ---------------------------------------------------
    
    parking_path <- paste0("~/Desktop/LULC-SSC/", city_name, "/parking/")
    
    if (!dir.exists(parking_path)){
      dir.create(parking_path)
    } else{
      print("dir exists")
    }
    
    parking_grid_file <- paste0(parking_path, "parking_", i, ".geojson")
    
    st_write(obj = parking,
             dsn = parking_grid_file,
             append = FALSE,
             delete_dsn = TRUE)
    
    
    ## Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    parking %>% rasterize(esa_rast, 
                          field = "Value",
                          background = 0,
                          filename = here(path, "parking_1m.tif"),
                          overwrite = TRUE)
    
    print("Parking raster saved")
    
    # Combine rasters ---------------------------------------------------------
    
    open_space_rast <- rast(here(path, "open_space_1m.tif"))
    roads_rast <- rast(here(path, "roads_1m.tif"))
    water_rast <- rast(here(path, "water_1m.tif"))
    buildings_rast <- rast(here(path, "buildings_1m.tif"))
    parking_rast <- rast(here(path, "parking_1m.tif"))
    
    LULC <- max(esa_rast,
                open_space_rast,
                roads_rast,
                water_rast,
                buildings_rast,
                parking_rast)
    
    writeRaster(x = LULC, 
                filename = here(path, "LULC.tif"),
                overwrite = TRUE,
                datatype = 'INT2U',
                gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512"))
    
    # 2. From local to gcs
    gs_uri <- local_to_gcs(
      x = here(path, "LULC.tif"),
      bucket = 'wri-cities-lulc-gee' 
    )
    
    assetID <- paste0("projects/earthengine-legacy/assets/projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC/", city_name, "_", i)
    
    properties <- list(city = city_name, 
                       grid_cell = i, 
                       version = "united states",
                       start_time = format(Sys.Date(), "%F"))
    
    # 3. Create an Image Manifest
    manifest <- ee_utils_create_manifest_image(gs_uri, 
                                               assetID,
                                               properties = properties)
    
    # 4. From GCS to Earth Engine
    gcs_to_ee_image(
      manifest = manifest,
      overwrite = TRUE
    )
    
    print("LULC raster saved")
    
    keep <- c("city", "epsg", "epsg_ee", "country_code", "city_grid", "lanes", "path", "road_path", "aois", "city_name", "cities", "geom")
    rm(list = setdiff(ls(), keep))
    
    gc()
    
    
  }
  
  
}


# Iterate over cities -----------------------------------------------------

cities <- ee$FeatureCollection("projects/wri-datalab/cities/SSC/features/SSC_RegionalBoundaries_2025") %>% 
  ee_as_sf() %>% 
  mutate(MetroName = case_when(MetroName == "Jacksonvillle" ~ "Jacksonville", .default = MetroName))

# Cities and state plane epsg codes to iterate through
# from https://spatialreference.org/
# all NAD83 (ft)

# create_LULC("New_Orleans", 3452)

# city <- "New_Orleans"
# epsg <- 3452

aois <- tribble(~ city, ~ epsg, ~ zone, ~geoid, 
                "New Orleans", 3452, "Louisiana South", "62677",
                "Dallas-Fort Worth", 2276, "Texas North Central", "22042",
                "Columbia", 2273, "South Carolina", "18964",
                "Atlanta", 2240, "Georgia West", "03817",
                "Boston", 2249, "Massachusetts Mainland", "09271",
                "Phoenix", 2223, "Arizona Central", "69184",
                "Portland", 2269, "Oregon North", "71317",
                "Charlotte", 2264, "North Carolina", "15670",
                "Jacksonville", 2236, "Florida East", "42346",
                "San Antonio", 2278, "Texas South Central", "78580")

cities <- cities %>% 
  left_join(aois, by = c("MetroName" = "city"))

cities <- cities %>% 
  slice(9:10)

# Iterate function for LULC creation over aois

walk2(cities$MetroName, cities$epsg, create_LULC)

# city <- cities %>% slice(2) %>% pull(MetroName)
# epsg <- cities %>% slice(2) %>% pull(epsg)
# geom <- cities %>% slice(2) %>% pull(geometry)

