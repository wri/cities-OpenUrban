
# Create LULC dataset for city --------------------------------------------

library(here)
library(tidyverse)
library(sf)
library(terra)
library(osmdata)
library(rgee)
library(googledrive)
library(retry)


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
  
  # Load metro area boundaries
  cities <- st_read(here("data", "SSC_CensusUrbanAreas2020.shp")) 
  
  # Choose city of interest, use spaces
  # city_name <- "Los Angeles"
  city_name <- city %>% 
    str_replace("_", " ")
  
  # Create local city folder to store data
  path <- here("cities", city)
  
  if (!dir.exists(path)){
    dir.create(path)
  } else{
    print("dir exists")
  }
  
  # Get city geometry
  city.geom <- cities %>%
    filter(str_detect(NAME20, city_name))

  # Create bounding box for urban area
  # buffered by 804.672 (half a mile in meters)
  # divide into grid for processing
  city_bb <- city.geom %>% 
    st_buffer(dist = 804.672) %>% 
    st_bbox() 
  
  city_grid <- city.geom %>% 
    st_make_grid(cellsize = c(0.15, 0.15), what = "polygons") %>% 
    st_sf() %>% 
    st_filter(city.geom) %>% 
    mutate(ID = row_number())
  
  # ggplot() +
  #   geom_sf(data = city.geom) +
  #   geom_sf(data = city_grid, fill = NA)
  
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
    roads_grid <- opq(bb) %>% 
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
    
    # Save roads vectors to use later
    roads_grid_file <- paste0(road_path, "/roads_", i, ".geojson")
    st_write(obj = roads_grid, 
             dsn = roads_grid_file, 
             append = FALSE,
             delete_dsn = TRUE)
    
    road_lanes <- road_lanes %>% 
      bind_rows(st_drop_geometry(roads_grid))
    
  }
  
  # road_lanes <- list.files(here(road_path), full.names = TRUE) %>%
  #   map(\(x) st_read(x) %>% 
  #         mutate(lanes = as.numeric(lanes))) %>%
  #   reduce(bind_rows) %>% 
  #   st_drop_geometry()
  
  # Get the avg number of lanes per highway class
  lanes <- road_lanes %>% 
    group_by(highway) %>% 
    summarize(avg.lanes = ceiling(mean(lanes, na.rm = TRUE))) %>% 
    mutate(avg.lanes = case_when(is.na(avg.lanes) ~ 2,
                                 !is.na(avg.lanes) ~ avg.lanes))
  

  # Iterate over grid -------------------------------------------------------

  
  for (i in 15:length(city_grid$ID)){
    # Buffer grid cell so there will be overlap
    aoi <- city_grid %>% 
      filter(ID == i) %>% 
      st_as_sf() %>% 
      st_buffer(dist = 100) 
    
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
    TO <- c(0, 1, 1, 1, 1, 2, 3, 20, 20, 20, 20, 3)
    
    esa_city <- esa_city$
      remap(FROM, TO)$
      reproject(paste0("EPSG:", epsg))
    
    # Download ESA to local via Google Drive
    esa_img <- ee_as_rast(esa_city,
                          region = bb_ee,
                          via = "drive",
                          scale = 1,
                          maxPixels = 10e10,
                          lazy = TRUE)
    
    # esa_filename <- paste0("ESA_1m_", i, ".tif")
    
    esa_rast <- esa_img %>%
      ee_utils_future_value() %>%
      writeRaster(here(path, "ESA_1m.tif"),
                  overwrite = TRUE)
    
    print("ESA raster saved")
    
    # esa_rast <- rast(here(path, "ESA_1m.tif"))
    
    # ggplot() +
    #   geom_sf(data = city.geom) +
    #   geom_sf(data = test.bb, fill = NA) +
    #   geom_sf(data = open_space)
    
    # Open space --------------------------------------------------------------
    
    
    ## Get OSM open space data -------------------------------------------------
    
    get_open_space1 <- function(bb){
      opq(bb) %>% 
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
    } 
    
    open_space1 <- retry(get_open_space1(bb), when = "runtime error")
    print("open space 1")
    
    get_open_space2 <- function(bb){
      opq(bb) %>% 
        add_osm_feature(key = 'boundary',
                        value = c('protected_area',
                                  'national_park',
                                  'forest_compartment',
                                  'forest')) %>% 
        osmdata_sf()
    }
    
    open_space2 <- retry(get_open_space2(bb), when = "runtime error")
    print("open space 2")
    
    open_space <- c(open_space1, open_space2)
    
    rm(open_space1)
    rm(open_space2)
    
    unloadNamespace("osmdata")
    library(osmdata)
    
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
    
    if (!is.null(open_space)){
      open_space <- open_space %>% 
        st_transform(epsg) %>% 
        mutate(Value = 10) %>% 
        select(Value)
    } else {
      open_space <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(epsg) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    print("open space combined polygons")
    
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
    get_water1 <- function(bb){
      opq(bb) %>% 
        add_osm_feature(key = 'water') %>% 
        osmdata_sf()
    } 
    
    water1 <- retry(get_water1(bb), when = "runtime error")
    print("water osm")
    
    get_water2 <- function(bb){
      opq(bb) %>% 
        add_osm_feature(key = 'natural', 
                        value = c('water')) %>% 
        osmdata_sf()
    }
    
    water2 <- retry(get_water2(bb), when = "runtime error")
    print("water 2 osm")
    
    get_water3 <- function(bb){
      opq(bb) %>% 
        add_osm_feature(key = 'waterway') %>% 
        osmdata_sf()
    }
    
    water3 <- retry(get_water3(bb), when = "runtime error")
    print("water 3 osm")
    
    water <- c(water1, water2, water3)
    
    rm(water1)
    rm(water2)
    
    unloadNamespace("osmdata")
    library(osmdata)
    
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
        mutate(Value = 20) %>% 
        select(Value)
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
    
    roads <- st_read(roads_grid_file)
    
    ## Tidy --------------------------------------------------------------------
    
    
    # Fill lanes with avg lane value when missing
    roads <- roads %>% 
      mutate(lanes = as.numeric(lanes)) %>% 
      left_join(lanes, by = "highway") %>% 
      mutate(lanes = coalesce(lanes, avg.lanes))
    
    # Add value field (20)
    roads <- roads %>% 
      mutate(Value = 30)
    
    # Reproject to local state plane 
    roads <- roads %>% 
      st_transform(epsg) 
    
    print("roads reproject")
    
    
    ## Buffer ------------------------------------------------------------------
    
    # Buffer roads by lanes * 10 ft (3.048 m) 
    # https://nacto.org/publication/urban-street-design-guide/street-design-elements/lane-width/#:~:text=wider%20lane%20widths.-,Lane%20widths%20of%2010%20feet%20are%20appropriate%20in%20urban%20areas,be%20used%20in%20each%20direction
    # cap is flat to the terminus of the road
    # join style is mitred so intersections are squared
    roads <- roads %>% 
      st_buffer(dist = roads$lanes * 3.048,
                endCapStyle = "FLAT",
                joinStyle = "MITRE") %>% 
      select(Value)
    
    print("roads buffered")
    
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
    
    
    ## Get ULU data ---------------------------------------------------
    
    
    # Read ULU land cover, filter to city, select lulc band
    ulu <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
      filterBounds(bb_ee)$
      select('lulc')$
      reduce(ee$Reducer$firstNonNull())$
      rename('lulc')
    
    # If there is no ULU data for a gridcell, create a raster of 0s
    
    tryCatch(ee_print(ulu), 
             error = function(e) 
               {ulu <<- ee$Image(0)$ 
                 clip(bb_ee)$
                 rename('lulc')
             return(ulu)})
    
    # Reclassify raster to combine lulc types
    # 0-Open space 
    # 1-Non-residential 
    # 2-Atomistic 
    # 3-Informal subdivision 
    # 4-Formal subdivision
    # 5-Housing project 
    # 6-Roads
    
    FROM = c(0, 1, 2, 3, 4, 5)
    
    # 0-Unclassified: 0 (open space)
    # 1-Non-residential: 1 (non-res)
    # 2-Residential: 2 (Atomistic), 3 (Informal), 4 (Formal), 5 (Housing project)
    
    TO = c(0, 1, 2, 2, 2, 2)
    
    ulu = ulu$remap(FROM, TO)
    
    # Download from Google Drive to city folder
    ulu_img <- ee_as_rast(ulu,
                          region = bb_ee,
                          via = "drive",
                          scale = 5,
                          maxPixels = 10e10,
                          lazy = TRUE) 
    
    
    ulu <- ulu_img %>% 
      ee_utils_future_value() %>% 
      writeRaster(here(path, "ULU_5m.tif"),
                  overwrite = TRUE)
    
    rm(ulu_img)
    
    print("ULU raster saved")
    
    # ulu <- rast(here(path, "ULU_5m.tif"))
    
    ## Average building height from the global human settlement layer ----------
    
    
    # ANBH is the average height of the built surfaces, USE THIS
    # AGBH is the amount of built cubic meters per surface unit in the cell
    # https://ghsl.jrc.ec.europa.eu/ghs_buH2023.php
    
    anbh <- ee$ImageCollection("projects/wri-datalab/GHSL/GHS-BUILT-H-ANBH_R2023A")$
      filterBounds(bb_ee)$ 
      select('b1')$
      mosaic()
    
    # Download from Google Drive to city folder
    anbh_img <- ee_as_rast(anbh,
                           region = bb_ee,
                           via = "drive",
                           scale = 100,
                           maxPixels = 10e10,
                           lazy = TRUE) 
    
    anbh <- anbh_img %>% 
      ee_utils_future_value() %>% 
      writeRaster(here(path, "anbh.tif"),
                  overwrite = TRUE)
    
    print("ANBH raster saved")
    
    # anbh <- rast(here(path, "anbh.tif"))
    
    ## Get OSM buildings data --------------------------------------------------------------
    
    # get buildings from OSM
    get_buildings <- function(bb){
      opq(bb) %>% 
        add_osm_feature(key = 'building') %>% 
        osmdata_sf()
    }
    
    buildings <- retry(get_buildings(bb), when = "runtime error")
    print("buildings OSM")
    
    unloadNamespace("osmdata")
    library(osmdata)
    
    # if there are polygons & multipolygons
    if (!is.null(nrow(buildings$osm_polygons)) & !is.null(nrow(buildings$osm_multipolygons))){
      buildings <- buildings$osm_polygons %>% 
        bind_rows(st_cast(buildings$osm_multipolygons, "POLYGON")) 
    
    # if there are polygons but no multipolygons
    } else if (!is.null(nrow(buildings$osm_polygons)) & is.null(nrow(buildings$osm_multipolygons))){
      buildings <- buildings$osm_polygons 
    
    # if there are multipolygons but no polygons
    } else if (is.null(nrow(buildings$osm_polygons)) & !is.null(nrow(buildings$osm_multipolygons))){
      buildings <- st_cast(buildings$osm_multipolygons, "POLYGON")
    
    # if there are no buildings use empty vector dataset
    # else {buildings <- esa_rast * 0}
    } else {
      buildings <- buildings$osm_polygons
    }
    
    if (is.null(buildings)){
      buildings <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        mutate(osm_id = NULL)
    }
      
    # drop excess fields
    buildings <- buildings %>% 
      select(osm_id)
      
    ## Extract ULU to buildings ------------------------------------------------
    
    # Reproject buildings to match ULU
    buildings <- buildings %>% 
      st_transform(crs = st_crs(ulu))
    
    print("buildings reproject")
    
    # Extract values to buildings using exact_extract, as coverage fractions
    # https://github.com/isciences/exactextractr
    build_ulu <- exactextractr::exact_extract(ulu, 
                                              buildings, 
                                              'frac',
                                              force_df = TRUE) 
    
    print("buildings ulu extract")
      
    # Assign the max coverage class to the buildings
    build_ulu <- build_ulu %>% 
      mutate(ID = row_number()) %>% 
      pivot_longer(cols = starts_with("frac"), 
                   names_to = "ULU", 
                   values_to = "coverage") %>% 
      mutate(ULU = as.integer(str_sub(ULU, start = -1))) %>% 
      group_by(ID) %>% 
      arrange(desc(coverage), .by_group = TRUE) %>% 
      # Needs to be an if statement
      # if coverage is equal between groups 
      # building should be unclassified (0)
      slice_head() %>% 
      ungroup() %>% 
      arrange(ID)
      
    buildings <- buildings %>% 
      add_column(ULU = build_ulu$ULU)
    
    print("buildings ulu class")
    
      
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
    buildings <- buildings %>% 
      st_transform(epsg) %>% 
      mutate(Area_m = as.numeric(st_area(.)))
    
    print("buildings reproject")
    
    
    ## Classification of roof slope --------------------------------------------
    tree <- readRDS(here("data", "building-class-tree.rds"))
    
    pred <- predict(tree, newdata = buildings, type = "class")
    
    buildings <- buildings %>% 
      add_column(Slope = pred) 
    
    buildings <- buildings %>% 
      mutate(Class = case_when(ULU == 0 ~ "unclassified",
                               ULU == 1 ~ "non-residential",
                               ULU == 2 ~ "residential",
                               is.na(ULU) ~ "unclassified"),
             Value = case_when(Class == "unclassified" ~ 44,
                               Class == "non-residential" & Slope == "low" ~ 43,
                               Class == "residential" & Slope == "low" ~ 42,
                               Class == "non-residential" & Slope == "high" ~ 41,
                               Class == "residential" & Slope == "high" ~ 40)) %>% 
      select(Value)
    
    print("buildings reclass")
    
    
    ## Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    buildings %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here(path, "buildings_1m.tif"),
                            overwrite = TRUE)
    
    # rm(buildings)
    
    print("Buildings raster saved")

    
    # Parking -----------------------------------------------------------------
    
    
    # get parking from OSM
    get_parking1 <- function(bb){
      opq(bb) %>% 
        add_osm_feature(key = 'amenity',
                        value = 'parking') %>% 
        osmdata_sf() 
    }
    
    parking1 <- retry(get_parking1(bb), when = "runtime error")
    print("parking osm")
    
    get_parking2 <- function(bb){
      opq(bb) %>% 
        add_osm_feature(key = 'parking') %>% 
        osmdata_sf() 
    }
    
    parking2 <- retry(get_parking2(bb), when = "runtime error")
    print("parking osm")
    
    parking <- c(parking1, parking2)
    
    unloadNamespace("osmdata")
    library(osmdata)
    
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
        mutate(Value = 50) %>% 
        select(Value)
    } else {
      parking <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(epsg) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    
    
    # Rasterize ---------------------------------------------------------------
    
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
    
    # Reclass ESA water (4) to 20
    # reclass <- cbind(from = c(1, 2, 3, 10, 20, 30, 41, 42, 50),
    #                  to = c(1, 2, 3, 10, 20, 30, 41, 42, 50))
    
    # LULC <- classify(LULC, reclass)
    
    lulc.name <- paste0(path, "/", city, "_LULC_1m_V2-", i, ".tif")
    
    writeRaster(x = LULC, 
                filename = here(path, "LULC.tif"),
                # lulc.name,
                overwrite = TRUE,
                datatype = 'INT1U',
                gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512"))
    
    # 2. From local to gcs
    gs_uri <- local_to_gcs(
      x = here(path, "LULC.tif"),
      bucket = 'wri-cities-lulc-gee' 
    )

    assetID <- paste0("projects/earthengine-legacy/assets/projects/wri-datalab/cities/SSC/LULC_V2/", city, "_LULC_1m_", i)

    # 3. Create an Image Manifest
    manifest <- ee_utils_create_manifest_image(gs_uri, assetID)

    # 4. From GCS to Earth Engine
    gcs_to_ee_image(
      manifest = manifest,
      overwrite = TRUE
    )
    
    print("LULC raster saved")

    
    rm(esa_rast, open_space_rast, roads_rast, water_rast, buildings_rast,
       parking_rast, LULC)
    
    gc()
    
  }
  
  
}


# Iterate over cities -----------------------------------------------------


# Cities and state plane epsg codes to iterate through
# from https://spatialreference.org/
# all NAD83 (ft)

create_LULC("New_Orleans", 3452)

city <- "Dallas"
epsg <- 2276

aois <- tribble(~ city, ~ epsg, ~ zone, ~geoid, 
                # "New_Orleans", 3452, "Louisiana South", "62677",
                "Dallas", 2276, "Texas North Central", "22042", 
                "Columbia", 2273, "South Carolina", "18964", 
                "Atlanta", 2240, "Georgia West", "03817", 
                "Boston", 2249, "Massachusetts Mainland", "09271", 
                "Phoenix", 2223, "Arizona Central", "69184", 
                "Portland", 2269, "Oregon North", "71317",
                "Charlotte", 2264, "North Carolina", "15670", 
                "Jacksonville", 2236, "Florida East", "42346",
                "San_Antonio", 2278, "Texas South Central", "78580")

# Iterate function for LULC creation over aois

walk2(aois$city, aois$epsg, create_LULC)



