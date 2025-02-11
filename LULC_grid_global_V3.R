
# Create LULC dataset for city --------------------------------------------

library(terra)
library(osmdata)
library(rgee)
library(gfcanalysis)
library(googledrive)
library(sf)
library(here)
library(tidyverse)



# Set up GEE --------------------------------------------------------------

# Set environment
# reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley", drive = TRUE, gcs = TRUE)
ee_Initialize("ejwesley", drive = TRUE, gcs = TRUE)

# LULC raster creation function -------------------------------------------

# city = spatial data for city of interest
# city_name = string with underscores, this will be used for naming files

# city_geom <- newCities %>%
#   slice(1)
# city_geom <- st_read("~/Desktop/LULC-Global/cape_town_admin_boundary.geojson")
# # city <- city_geom$name
# city <- "Cape_Town"
# country_code <- "ZAF"

create_LULC <- function(city, city_geom, country_code){

  # City boundary -----------------------------------------------------------
  city <- str_replace_all(city, " ", "_")
  city_name <- paste0(country_code, "-", city)
  
  # city_geom <- st_read(city_file) %>%
  #   st_transform(4326)
  # st_write(city_geom, here(path, paste0(city_name, ".shp")))
  # transform data to UTM
  city_centroid <- city_geom %>% 
    st_centroid() %>% 
    st_coordinates()
  
  utm <- utm_zone(y = city_centroid[2], x = city_centroid[1], proj4string = TRUE)
  
  utm_ee <- str_split(utm, "=", simplify = TRUE)[2] %>% 
    str_to_upper()
  
  utm <- str_split(utm_ee, ":", simplify = TRUE)[2] %>% 
    as.numeric() %>% 
    st_crs()
    
  
  # Path to temp directory
  path <- here()

  # Create bounding box for urban area
  # buffered by 804.672 (half a mile in meters)
  # divide into grid for processing
  city_bb <- city_geom %>% 
    st_buffer(dist = 804.672) %>% 
    st_bbox() 
  
  # Create grid for LULC tiles
  city_grid <- city_geom %>% 
    st_make_grid(cellsize = c(0.15, 0.15), what = "polygons") %>% 
    st_sf() %>% 
    st_filter(city_geom) %>% 
    mutate(ID = row_number())
  
  ggplot() +
    geom_sf(data = city_geom) +
    geom_sf(data = city_grid, fill = NA) +
    geom_sf_label(data = city_grid, aes(label = ID))

  
  # Road download & avg lane calculation ------------------------------------
  
  
  # Download roads and calculate mean lanes per highway class for the city
  # road_path <- paste0("~/Desktop/LULC-Global/roads/", city_name)
  road_path <- here("roads", city_name)
  
  if (!dir.exists(road_path)){
    dir.create(road_path)
  } else{
    print("dir exists")
  }

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
    
    # if (!is.null(nrow(roads_grid$osm_lines))){
    #   # if there are polygons & multipolygons
    #   roads_grid <- roads_grid$osm_lines 
    #   
    # } else {
    #   # if there are neither keep empty geometry
    #   next
    # }
    if(is.null(nrow(roads_grid$osm_lines))){
      next
    }

    # select lines geometries
    roads_grid <- roads_grid$osm_lines %>%
      dplyr::select(any_of(c("highway", "lanes", "osm_id"))) %>%
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
          mutate(lanes = as.numeric(lanes))) %>%
    reduce(bind_rows) %>%
    st_drop_geometry()
  
  # Get the avg number of lanes per highway class
  lanes <- road_lanes %>% 
    group_by(highway) %>% 
    summarize(avg.lanes = ceiling(mean(lanes, na.rm = TRUE))) %>% 
    mutate(avg.lanes = case_when(is.na(avg.lanes) ~ 2,
                                 !is.na(avg.lanes) ~ avg.lanes))
  
  write_csv(lanes, paste0(road_path, "/average_lanes.csv"))
  lanes <- read_csv(paste0(road_path, "/average_lanes.csv"))
  

  # Iterate over grid -------------------------------------------------------

  
  for (i in 1:length(city_grid$ID)){
    
    print(paste0(city_name, " grid ", i))
    
    # Buffer grid cell by 10-m so there will be overlap
    aoi <- city_grid %>% 
      filter(ID == i) %>% 
      st_as_sf() %>% 
      st_buffer(dist = 10) 
    
    bb <- st_bbox(aoi)
    
    bb_ee <- sf_as_ee(st_as_sfc(bb))
    
    # ESA Worldcover ----------------------------------------------------------
    
    # Read ESA land cover (2021)
    esa <- ee$ImageCollection('ESA/WorldCover/v200')
    
    # Clip to tile 
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
                               folder = "LULC_grid_global_V3",
                               timePrefix = FALSE,
                               region = bb_ee,
                               scale = 1,
                               crs = utm_ee,
                               maxPixels = 10e10)
    
    esa_img$start()
    
    # Move from drive to local
    ee_drive_to_local(task = ee_monitoring(esa_img, max_attempts = 1000), 
                      dsn = here(path, "ESA_1m.tif"))
    
    # drive_download("rgee_backup/ESA_1m.tif",
    #                path = here(path, "ESA_1m.tif"))
    
    # Delete from drive
    drive_rm("LULC_grid_global_V3/ESA_1m.tif")
    
    print("ESA raster saved")
    
    esa_rast <- rast(here(path, "ESA_1m.tif"))
    
    
    
    # Open space --------------------------------------------------------------
    
    set_overpass_url("https://overpass-api.de/api/interpreter")
    
    ## Get OSM open space data -------------------------------------------------
    
    open_space1 <- opq(bb) %>% 
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
    
    open_space2 <- opq(bb) %>% 
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
        st_transform(utm) %>% 
        mutate(Value = 200) %>% 
        select(any_of(c("Value", "osm_id", "leisure", "sport", "surface")))
    } else {
      open_space <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(utm) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    open_space <- st_make_valid(open_space)
    
    print("open space combined polygons")
    
    ## Save open space polygons ####
    openspace_path <- paste0("~/Desktop/LULC-Global/openspace/", city_name)
    
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
    
    open_space <- st_read(openspace_file)
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
    water1 <- opq(bb) %>% 
      add_osm_feature(key = 'water') %>% 
      osmdata_sf()

    print("water osm")
    
    water2 <- opq(bb) %>% 
      add_osm_feature(key = 'natural', 
                      value = c('water')) %>% 
      osmdata_sf()

    print("water 2 osm")
    
    water3 <- opq(bb) %>% 
      add_osm_feature(key = 'waterway') %>% 
      osmdata_sf()
   
    print("water 3 osm")
    
    water <- c(water1, water2, water3)
    
    rm(water1)
    rm(water2)
    
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
        st_transform(utm) %>% 
        mutate(Value = 300) %>% 
        select(any_of(c("Value", "osm_id")))
    } else {
      water <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(utm) %>% 
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
        st_transform(utm) 
      
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
        st_transform(utm) %>% 
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
      
      ## Get ULU data ---------------------------------------------------
      
      # Read ULU land cover, filter to city, select lulc band
      ulu <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
        filterBounds(bb_ee)$
        select('lulc')
      
      ulu_proj <- ulu$first()$projection()
      
      ulu <- ulu$
        reduce(ee$Reducer$firstNonNull())$
        reproject(crs = ulu_proj, scale = 5)$
        rename('lulc')
      
      # If there is no ULU data for a gridcell, create a raster of 0s (unclassified)
      tryCatch(ee_print(ulu),
               error = function(e)
               {ulu <<- ee$Image(0)$
                 clip(bb_ee)$
                 rename('lulc')
               return(ulu)})
      
      # Calculate the maximum value
      max_val <- ulu$reduceRegion(
        reducer = ee$Reducer$max(),
        geometry = bb_ee,
        scale = 5,        # Specify resolution in meters
        maxPixels = 1e9      # Maximum pixels to process
      )$getInfo()
      
      if (is.null(max_val$lulc)){
        ulu <- ee$Image(0)$ 
          clip(bb_ee)$
          rename('lulc')
      }
      
      # Reclassify raster to combine lulc types
      # 0-Open space 
      # 1-Non-residential 
      # 2-Atomistic 
      # 3-Informal subdivision 
      # 4-Formal subdivision
      # 5-Housing project 
      
      FROM = c(0, 1, 2, 3, 4, 5)
      
      # 0-Unclassified: 0 (open space)
      # 1-Non-residential: 1 (non-res)
      # 2-Residential: 2 (Atomistic), 3 (Informal), 4 (Formal), 5 (Housing project)
      
      TO = c(600, 620, 610, 610, 610, 610)
      
      ulu = ulu$remap(FROM, TO)$rename('lulc_remap')$select('lulc_remap')
      
      ulu_img <- ee_image_to_drive(ulu,
                                   description = "ULU_5m",
                                   folder = "LULC_grid_global_V3",
                                   timePrefix = FALSE,
                                   region = bb_ee,
                                   scale = 5,
                                   maxPixels = 10e10)
      
      ulu_img$start()
      
      # Move from GCS to local
      ee_drive_to_local(task = ee_monitoring(ulu_img, max_attempts = 1000), 
                        dsn = here("ULU_5m.tif"),
                        overwrite = TRUE)
      
      print("ULU raster saved")
      
      ulu <- rast(here("ULU_5m.tif"))
      
      ulu <- ulu %>% 
        subst(0, 600)
      
      drive_rm("LULC_grid_global_V3/ULU_5m.tif")
      
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
      
      # Add value field and transform to utm
      buildings <- buildings %>% 
        mutate(Value = ULU) %>% 
        st_transform(utm)
      
      buildings <- st_make_valid(buildings)
      
      
      ## Save building vectors ---------------------------------------------------
      
      building_path <- paste0("~/Desktop/LULC-Global/buildings/", city_name)
      
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
        st_transform(utm) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    buildings <- st_read(building_grid_file)
       
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
    parking1 <- opq(bb) %>% 
      add_osm_feature(key = 'amenity',
                      value = 'parking') %>% 
      osmdata_sf() 
    
    print("parking osm")
    
    parking2 <- opq(bb) %>% 
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
        st_transform(utm) %>% 
        mutate(Value = 400) %>% 
        select(any_of(c("Value", "osm_id", "amenity", "parking", "surface")))
    } else {
      parking <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(utm) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    parking <- st_make_valid(parking)
    
    ## Save parking vectors ---------------------------------------------------
    
    parking_path <- paste0("~/Desktop/LULC-Global/parking/", city_name)
    
    if (!dir.exists(parking_path)){
      dir.create(parking_path)
    } else{
      print("dir exists")
    }
    
    parking_grid_file <- paste0(parking_path, "/parking_", i, ".geojson")
    
    st_write(obj = parking,
             dsn = parking_grid_file,
             append = FALSE,
             delete_dsn = TRUE)
    
    parking <- st_read(parking_grid_file)
    
    ## Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    parking %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here(path, "parking_1m.tif"),
                            overwrite = TRUE)
    
    print("Parking raster saved")
    
    # Combine rasters ---------------------------------------------------------
    
    # esa_rast <- rast(here(path, "ESA_1m.tif"))
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
                       version = "global",
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

    keep <- c("city", "utm", "utm_ee", "country_code", "city_grid", "lanes", "path", "road_path", "aois", "city_name")
    rm(list = setdiff(ls(), keep))
    
    gc()
    
    
    
  }
  
  # Delete files in folder
  file.list <- list.files(here(path), full.names = TRUE, pattern = c(".tif", ".geojson"))
  unlink(file.list, recursive = TRUE)
  
}



# pmap(list(newCities$name, newCities$geometry, newCities$country), create_LULC)
# 
# create_LULC(city = city, city_geom = city_geom,
#             country_code = country_code)

# Cities ------------------------------------------------------------------
city_geom <- ee$FeatureCollection("projects/rthngl/assets/campinas_fullarea") %>% 
  ee_as_sf()

create_LULC(city = "Campinas", 
            city_geom = city_geom,
            country_code = "BRA")


# mexico <- tribble(
#   ~ City, ~ File, ~ Code,
# #   "Cancun", here("cities", "boundaries", "MEX-Cancun-EJW.shp"), "MEX",
# #   "Hermosillo", here("cities", "boundaries", "MEX-Hermosillo-EJW.shp"), "MEX",
#   "Monterrey", "/Users/WRI/Library/CloudStorage/OneDrive-WorldResourcesInstitute/Documents/SSC/Code/cities-heat/cities/boundaries/MEX-Monterrey.geojson", "MEX")
# #   "Mexico_City", here("cities", "boundaries", "MEX-Mexico_City.geojson"), "MEX"
# # )
# # 
# pmap(list(mexico$City, mexico$File, mexico$Code), create_LULC)
# 
# city_geoms <- tribble(
#   ~ City, ~ File, ~ Code,
#   "Busan", "/Users/WRI/Desktop/Global/Busan/Busan.shp", "KOR",
#   "Cabimas", "/Users/WRI/Desktop/Global/Cabimas/Cabimas.shp", "VEN",
#   "Gombe", "/Users/WRI/Desktop/Global/Gombe/Gombe.shp", "NGA",
#   "Kaiping", "/Users/WRI/Desktop/Global/Kaiping/Kaiping.shp", "CHN",
#   "Karachi", "/Users/WRI/Desktop/Global/Karachi/Karachi.shp", "PAK",
#   "Kinshasa", "/Users/WRI/Desktop/Global/Kinshasa/Kinshasa.shp", "COD",
#   "Manila", "/Users/WRI/Desktop/Global/Manila/Manila.shp", "PHL",
#   "Marrakech", "/Users/WRI/Desktop/Global/Marrakesh", "MAR",
#   "Okayama", "/Users/WRI/Desktop/Global/Okayama/Okayama.shp", "JPN",
#   "Palembang", "/Users/WRI/Desktop/Global/Palembang", "IDN",
#   "Qom", "/Users/WRI/Desktop/Global/Qom/Qom.shp", "IRN",
#   "Sao Paulo", "/Users/WRI/Desktop/Global/SaoPaulo/SaoPaulo.shp", "BRA",
#   "Tashkent", "/Users/WRI/Desktop/Global/Tashkent/Tashkent.shp", "UZB",
#   "Warsaw", "/Users/WRI/Desktop/Global/Warsaw/Warsaw.shp", "POL"
# )
# 
# pmap(list(city_geoms$City, city_geoms$File, city_geoms$Code), create_LULC)
# 
# 
