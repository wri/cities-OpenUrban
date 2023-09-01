
# Create LULC dataset for city --------------------------------------------
library(tictoc)
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
reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley")
ee_Initialize("ejwesley", drive = TRUE)

# LULC raster creation function -------------------------------------------

# city = city of interest, use underscores instead of spaces
# epsg = state plane EPSG for city
# fold = EE asset folder

create_LULC(city = "Los_Angeles", 
            epsg = 26945, 
            GEE_folder = "projects/wri-datalab/cities/SSC")

create_LULC <- function(city, epsg, GEE_folder){
  tic("Total function run time:")
  # City boundary -----------------------------------------------------------
  
  # Load metro area boundaries
  cities <- st_read("data/Smart_Surfaces_metro_areas/smart_surfaces_urban_areas.shp")
  
  # Choose city of interest, use underscores instead of spaces
  # city_name <- "Los_Angeles"
  city_name <- city
  
  # Use LA AOI for testing purposes
  # city <- st_read("data/AOI.geojson")
  
  # Get city geometry
  city <- cities %>%
    filter(str_detect(NAME10, city))
  
  # Create bounding box for urban area
  # buffered by 804.672 (half a mile in meters)
  # divide into grid for processing
  bb <- city %>% 
    st_buffer(dist = 804.672) %>% 
    st_bbox() %>% 
    st_make_grid(cellsize = c(0.5, 0.5), what = "polygons")
  
  bb_ee <-  sf_as_ee(st_as_sfc(bb))
  
  # Lookup local state plane projection 
  # epsg <- 26945
  epsg <- epsg
  
  
  # ESA Worldcover ----------------------------------------------------------
  
  tic("ESA land cover")
  
  # Read ESA land cover (2021)
  esa <- ee$ImageCollection('ESA/WorldCover/v200')
  
  # Clip to city of interest
  esa_city <- esa$filterBounds(bb_ee)
  
  # ImageCollection to image
  esa_city <- esa_city$first()
  
  # Reclassify raster to combine greenspace types
  
  ## [ND, Tree, Shrubland, Grassland, Cropland, Built up, Bare/sparse vegetation,
  ##  Snow/ice, Permanent water bodies, Herbaceous wetland, Mangroves, Moss and lichen]
  FROM = c(0, 10, 20, 30, 40, 50, 60, 80, 70, 90, 95, 100)
  
  # ND: 0 
  # Green: 1 (Tree, Shrubland, Grassland, Cropland)
  # Built up: 2 (Built up)
  # Barren: 3 (Bare/sparse vegetation, Moss and lichen)
  # Water: 4 (Snow/ice, Permanent water bodies, Herbaceous wetland, Mangroves)
  TO = c(0, 1, 1, 1, 1, 2, 3, 4, 4, 4, 4, 3)
  
  esa_city = esa_city$remap(FROM, TO)
  
  # Resample raster from 10-m to 1-m
  # GEE performs nearest neighbor resampling by default
  # https://developers.google.com/earth-engine/guides/resample
  # esa_city = esa_city$reproject(crs = paste0("EPSG:", epsg), scale = 1)
  
  
  # 
  # user <- ee_get_assethome()
  # addm <- function(x) sprintf("%s/%s",user, x)
  # path <- paste0("projects/wri-datalab/cities/SSC/", city_name)
  # 
  # ee_manage_create(path_asset = addm(path),
  #                  asset_type = "Folder")
  
  # Create local city folder to store data
  path <- here("data", city_name)

  if (!dir.exists(path)){
    dir.create(path)
  } else{
    print("dir exists")
  }

  # Download ESA to Google Drive
  esa_img <- ee_as_rast(esa_city,
                        region = bb_ee,
                        via = "drive",
                        scale = 1,
                        maxPixels = 10e10,
                        lazy = TRUE)

  
  
  # Resampled to 1m in local state plane
  # Save as EE asset
  # esa_img <- ee_image_to_asset(image = esa_city,
  #                              region = bb_ee,
  #                              assetId = assetID,
  #                              scale = 1,
  #                              crs = paste0("EPSG:", epsg),
  #                              maxPixels = 10e10)
  # esa_img$start()
 
  # esa_rast <- esa_img %>%
  #   ee_utils_future_value() %>%
  #   writeRaster(paste0(path, "/ESA_1m.tif"))
  
  
  print("ESA raster saved")
  toc()
  
  # esa_rast <- rast(here(path, "ESA_1m.tif"))
  
  # esa <- ee$Image(assetID) 
  
  # esa_rast <- esa_img %>%
  #   ee_utils_future_value() %>%
  #   writeRaster(paste0(path, "/ESA_1m.tif"))
  
  # List ESA Images in drive folder
  drive_folder <- "rgee_backup"
  imgs <- drive_ls(drive_folder,
                   pattern = "ESA")
    
  ids <- imgs$id
  
  for (i in 1:length(ids)){
    
    index <- i
    img_id <- ids[i]
    
    print(img_id)
    LULC_byIMG(img_id, index)
    
  }
  
  
}
  
# Create rasters for each ESA image
# This effectively manages memory constraints
LULC_byIMG <- function(img_id, index){
  
  
  # download ESA image ------------------------------------------------------
  drive_download(img_id,
                 path = here(path, "ESA_1m.tif"),
                 overwrite = TRUE)
  
  # delete image from Google Drive
  drive_rm(img_id)
  
  esa_rast <- rast(here(path, "ESA_1m.tif"))
  
  # Get bb of esa_rast
  esa_bb <- esa_rast %>% 
    st_bbox() %>% 
    st_as_sfc() %>%
    st_transform(crs = st_crs(city)) %>%
    st_bbox()
  
  esa_bb_ee <- esa_bb %>%  
    st_as_sfc() %>% 
    sf_as_ee()
  
  # Open space --------------------------------------------------------------
  
  tic("Open space")
  
  ## Get OSM open space data -------------------------------------------------
  ## May need a try statement ####
  # Error in check_for_error(paste0(doc)) : 
  #   General overpass server error; returned:
  #   The data included in this document is from www.openstreetmap.org. 
  # The data is made available under ODbL. runtime error: Query timed out in "query" at line 4 after 26 seconds.
  
  get_open_space1 <- function(esa_bb){
    opq(esa_bb) %>% 
      add_osm_feature(key = 'leisure',
                      value = c('park', 'nature_reserve', 'common', 
                                'playground', 'pitch', 'track')) %>% 
      osmdata_sf()
  } 
  
  open_space1 <- retry(get_open_space1(esa_bb), when = "runtime error")
  print("open space 1")
  
  get_open_space2 <- function(esa_bb){
    opq(esa_bb) %>% 
      add_osm_feature(key = 'boundary',
                      value = c('protected_area','national_park')) %>% 
      osmdata_sf()
  }
  
  open_space2 <- retry(get_open_space2(esa_bb), when = "runtime error")
  print("open space 2")
  
  open_space <- c(open_space1, open_space2)
  
  rm(open_space1)
  rm(open_space2)
  
  ## Tidy --------------------------------------------------------------------
  
  # Combine polygons and multipolygons
  open_space <- open_space$osm_polygons %>% 
    bind_rows(st_cast(open_space$osm_multipolygons, "POLYGON")) 
  print("open space combined polygons")
  
  # Reproject to local state plane and add value field (10)
  open_space <- open_space %>% 
    st_transform(epsg) %>% 
    mutate(Value = 10)
  print("open space reproject")
  
  ## Rasterize ---------------------------------------------------------------
  
  # Rasterize to match grid of esa and save raster
  open_space %>% 
    rasterize(esa_rast, 
              field = "Value",
              background = 0,
              filename = here(path, "open_space_1m.tif"),
              overwrite = TRUE)
  
  rm(open_space)
  
  print("Open space raster saved")
  toc()
  
  # Roads -------------------------------------------------------------------
  
  tic("Roads")
  
  ## Get OSM roads data ----------------------------------------------------------------
  
  # get roads from OSM
  get_roads <- function(esa_bb){
    opq(esa_bb) %>% 
      add_osm_feature(key = 'highway',
                      value = c('motorway', 'trunk', 'primary', 'secondary', 
                                'tertiary', 'residential', 'unclassified', 
                                'motorway_link', 'trunk_link', 'primary_link', 
                                'secondary_link', 'tertiary_link', 'living_street')) %>% 
      osmdata_sf() 
  }
  
  roads <- retry(get_roads(esa_bb), when = "runtime error")
  print("roads osm")
  
  # select lines geometries
  roads <- roads$osm_lines %>% 
    dplyr::select(highway, lanes) %>% 
    mutate(lanes = as.numeric(lanes))
  
  
  ## Tidy --------------------------------------------------------------------
  
  # Get the avg number of lanes per highway class
  lanes <- roads %>% 
    st_drop_geometry() %>% 
    group_by(highway) %>% 
    summarize(avg.lanes = ceiling(mean(lanes, na.rm = TRUE)))
  
  # Fill lanes with avg lane value when missing
  roads <- roads %>% 
    left_join(lanes, by = "highway") %>% 
    mutate(lanes = coalesce(lanes, avg.lanes))
  
  # Add value field (20)
  roads <- roads %>% 
    mutate(Value = 20)
  
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
              joinStyle = "MITRE")
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
  toc()
  
  # Water -------------------------------------------------------------------
  
  tic("Water")
  
  ## Get OSM water data --------------------------------------------------------------
  
  # get water from OSM
  get_water1 <- function(esa_bb){
    opq(esa_bb) %>% 
      add_osm_feature(key = 'water') %>% 
      osmdata_sf()
  } 
  
  water1 <- retry(get_water1(esa_bb), when = "runtime error")
  print("water osm")
  
  get_water2 <- function(esa_bb){
    opq(esa_bb) %>% 
      add_osm_feature(key = 'natural', value = 'water') %>% 
      osmdata_sf()
  }
  
  water2 <- retry(get_water2(esa_bb), when = "runtime error")
  print("water 2 osm")
  
  water <- c(water1, water2)
  
  rm(water1)
  rm(water2)
  
  # Combine polygons and multipolygons
  water <- water$osm_polygons %>% 
    bind_rows(st_cast(water$osm_multipolygons, "POLYGON"))
  
  ## Tidy ---------------------------------------------------------------
  
  # Reproject to local state plane and add value field (10)
  water <- water %>% 
    st_transform(epsg) %>% 
    mutate(Value = 30)
  print("water reproject")
  
  ## Rasterize ---------------------------------------------------------------
  
  # Rasterize to match grid of esa and save raster
  water %>% rasterize(esa_rast, 
                      field = "Value",
                      background = 0,
                      filename = here(path, "water_1m.tif"),
                      overwrite = TRUE)
  
  rm(water)
  
  print("Water raster saved")
  toc()
  
  # Buildings -------------------------------------------------------------------
  
  tic("Buildings")
  
  ## Get ULU data ---------------------------------------------------
  
  tic("ULU")
  
  ############# Check that first is okay here
  # firstNonNull is used as reducer in GEE script
  # Read ULU land cover, filter to city, select lulc band
  ulu <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
    filterBounds(esa_bb_ee)$
    select('lulc')$
    first()$
    rename('lulc')
  
  roads <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
    filterBounds(esa_bb_ee)$
    select('road')$
    first()$
    rename('lulc')
  
  # Create road mask
  # Typical threshold for creating road mask
  road_prob <- 50
  road_mask <- roads$
    updateMask(roads$gte(road_prob))$
    where(roads, 1)
  
  ulu <- ulu$
    where(road_mask, 6)
  
  # Reclassify raster to combine lulc types
  # 0-Open space 
  # 1-Non-residential 
  # 2-Atomistic 
  # 3-Informal subdivision 
  # 4-Formal subdivision
  # 5-Housing project 
  # 6-Roads
  
  FROM = c(0, 1, 2, 3, 4, 5, 6)
  
  # 1-Non-residential: 0 (open space), 1 (non-res)
  # 2-Residential: 2 (Atomistic), 3 (Informal), 4 (Formal), 5 (Housing project)
  # 3-Roads: 6 (Roads)
  
  TO = c(1, 1, 2, 2, 2, 2, 3)
  
  ulu = ulu$remap(FROM, TO)
  
  # Download from Google Drive to city folder
  ulu_img <- ee_as_rast(ulu,
                        region = esa_bb_ee,
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
  
  ## Get OSM buildings data --------------------------------------------------------------
  
  # get buildings from OSM
  get_buildings <- function(esa_bb){
    opq(esa_bb) %>% 
      add_osm_feature(key = 'building') %>% 
      osmdata_sf()
  }
  
  buildings <- retry(get_buildings(esa_bb), when = "runtime error")
  print("buildings OSM")
  
  # Combine polygons and multipolygons
  buildings <- buildings$osm_polygons %>% 
    bind_rows(st_cast(buildings$osm_multipolygons, "POLYGON"))
  
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
  build_ulu <- exactextractr::exact_extract(ulu, buildings, 'frac') 
  print("buildings ulu extract")
  
  # Assign the max coverage class to the building if it is not roads,
  # If the max class is roads assign the minority class
  # If the max class is roads, and there is no minority class, assign non-residential
  build_ulu <- build_ulu %>% 
    mutate(ID = row_number()) %>% 
    pivot_longer(cols = starts_with("frac"), 
                 names_to = "ULU", 
                 values_to = "coverage") %>% 
    mutate(ULU = as.integer(str_sub(ULU, start = -1))) %>% 
    group_by(ID) %>% 
    arrange(coverage, .by_group = TRUE) %>% 
    summarise(ULU = case_when(
      # When the majority class is not roads (3), return the majority class
      last(ULU) != 3 ~ last(ULU),
      # When the majority class is roads, return the second majority class
      last(ULU) == 3 & nth(coverage, -2L) != 0 ~ nth(ULU, -2L),
      # When the majority ULU is roads (3) & there is no minority class, use ULU 1 (non-res)
      last(ULU) == 3 & nth(coverage, -2L) == 0 ~ 1)) %>% 
    ungroup() 
  
  buildings <- buildings %>% 
    add_column(ULU = build_ulu$ULU)
  print("buildings ulu class")
  
  toc()
  
  ## Average building height from the global human settlement layer ----------
  
  tic("ANBH")
  
  # ANBH is the average height of the built surfaces, USE THIS
  # AGBH is the amount of built cubic meters per surface unit in the cell
  # https://ghsl.jrc.ec.europa.eu/ghs_buH2023.php
  
  anbh <- ee$ImageCollection("projects/wri-datalab/GHSL/GHS-BUILT-H-ANBH_R2023A")$
    filterBounds(esa_bb_ee)$ 
    select('b1')$
    first()
  
  # Download from Google Drive to city folder
  anbh_img <- ee_as_rast(anbh,
                         region = esa_bb_ee,
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
  
  # Reproject buildings to match ANBH
  buildings <- buildings %>% 
    st_transform(crs = st_crs(anbh))
  
  # Extract average of pixel values to buildings
  # Mean value of cells that intersect the polygon, 
  # weighted by the fraction of the cell that is covered.
  avg_ht <- exactextractr::exact_extract(anbh, buildings, 'mean')
  
  buildings <- buildings %>% 
    add_column(ANBH = avg_ht)
  print("buildings anbh")
  
  toc()
  
  ## Tidy ---------------------------------------------------------------
  
  # Reproject to local state plane and calculate area
  buildings <- buildings %>% 
    st_transform(epsg) %>% 
    mutate(Area_m = as.numeric(st_area(.)))
  print("buildings reproject")
  
  
  ## Classification of roof slope --------------------------------------------
  tree <- readRDS(here("data", "tree.rds"))
  
  pred <- predict(tree, newdata = buildings, type = "class")
  
  buildings <- buildings %>% 
    add_column(Slope = pred) 
  
  # add value field, low slope = 41, high slope = 42
  buildings <- buildings %>% 
    mutate(Value = case_when(Slope == "low" ~ 41,
                             Slope == "high" ~ 42))
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
  toc()
  
  # Parking -----------------------------------------------------------------
  
  tic("Parking")
  
  # get open space from OSM
  get_parking <- function(esa_bb){
    opq(esa_bb) %>% 
    add_osm_feature(key = 'amenity',
                    value = 'parking') %>% 
    osmdata_sf() 
  }
  
  parking <- retry(get_parking(esa_bb), when = "runtime error")
  print("parking osm")
  
  # Combine polygons and multipolygons
  parking <- parking$osm_polygons %>% 
    bind_rows(st_cast(parking$osm_multipolygons, "POLYGON")) 
  
  # Reproject to local state plane and add value field (50)
  parking <- parking %>% 
    st_transform(epsg) %>% 
    mutate(Value = 50)
  
  # Rasterize to match grid of esa and save raster
  parking %>% rasterize(esa_rast, 
                        field = "Value",
                        background = 0,
                        filename = here(path, "parking_1m.tif"),
                        overwrite = TRUE)
  
  rm(parking)
  
  print("Parking raster saved")
  toc()
  
  # Combine rasters ---------------------------------------------------------
  
  tic("Combine rasters")
  
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
  
  LULC <- app(LULC, max)
  
  # Reclass ESA water (4) to 30
  reclass <- cbind(from = c(1, 2, 3, 4, 10, 20, 30, 41, 42, 50),
                   to = c(1, 2, 3, 30, 10, 20, 30, 41, 42, 50))
  
  LULC <- classify(LULC, reclass)
  
  filename <- paste0("LULC_1m_", index, ".tif")
  
  writeRaster(LULC, 
              here(path, filename), 
              overwrite = TRUE)
  
  print("LULC raster saved")
  toc()
  toc()
}


