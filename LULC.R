
# Create LULC dataset for city --------------------------------------------
library(tictoc)
library(here)
library(tidyverse)
library(sf)
library(terra)
library(osmdata)
library(rgee)


# Set up GEE --------------------------------------------------------------

# Set environment
reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley")
ee_Initialize("ejwesley", drive = TRUE)


# LULC raster creation function -------------------------------------------

# city = city of interest, use underscores instead of spaces
# epsg = state plane EPSG for city

create_LULC <- function(city, epsg){
  tic("Total function run time:")
  # City boundary -----------------------------------------------------------
  
  # Load metro area boundaries
  cities <- st_read("data/Smart_Surfaces_metro_areas/smart_surfaces_urban_areas.shp")
  
  # Choose city of interest, use underscores instead of spaces
  # city_name <- "Los_Angeles"
  city_name <- city
  
  # Use LA AOI for testing purposes
  #city <- st_read("data/AOI.geojson")
  
  # Get city geometry
  city <- cities %>%
    filter(str_detect(NAME10, city))
  
  # Create bounding box for urban area
  # buffered by 804.672 (half a mile in meters)
  bb <- city %>% 
    st_buffer(dist = 804.672) %>% 
    st_bbox() 
  
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
  esa_city = esa_city$reproject(crs = paste0("EPSG:", epsg), scale = 1)
  
  # Create city folder to store data
  path <- paste0("data/", city_name)
  
  if (!dir.exists(path)){
    dir.create(path)
  } else{
    print("dir exists")
  }
  
  # Download from Google Drive to city folder
  esa_img <- ee_as_rast(esa_city,
                        region = bb_ee,
                        via = "drive",
                        scale = 1,
                        maxPixels = 10e10,
                        lazy = TRUE) 
  
  esa_rast <- esa_img %>% 
    ee_utils_future_value() %>% 
    writeRaster(paste0(path, "/ESA_1m.tif"))
  
  print("ESA raster saved")
  toc()
  
  # esa_rast <- rast(here(path, "ESA_1m.tif"))
  
  # Open space --------------------------------------------------------------
  
  tic("Open space")
  
  ## Get OSM open space data -------------------------------------------------
  
  open_space1 <- opq(bb) %>% 
    add_osm_feature(key = 'leisure',
                    value = c('park', 'nature_reserve', 'common', 
                    'playground', 'pitch', 'track')) %>% 
    osmdata_sf() 
  
  open_space2 <- opq(bb) %>% 
    add_osm_feature(key = 'boundary',
                    value = c('protected_area','national_park')) %>% 
    osmdata_sf() 
  
  open_space <- c(open_space1, open_space2)
  
  rm(open_space1)
  rm(open_space2)
  
  ## Tidy --------------------------------------------------------------------
  
  # Combine polygons and multipolygons
  open_space <- open_space$osm_polygons %>% 
    bind_rows(st_cast(open_space$osm_multipolygons, "POLYGON")) 
  
  # Reproject to local state plane and add value field (10)
  open_space <- open_space %>% 
    st_transform(epsg) %>% 
    mutate(Value = 10)
  
  
  ## Rasterize ---------------------------------------------------------------
  
  # Rasterize to match grid of esa and save raster
  open_space_rast <- open_space %>% 
    rasterize(esa, 
              field = "Value",
              background = 0,
              filename = paste0(path, "/open_space_1m.tif"),
              overwrite = TRUE)
  
  rm(open_space)
  
  print("Open space raster saved")
  toc()
  
  # open_space_rast <- rast(here(path, "open_space_1m.tif"))
  
  
  # Roads -------------------------------------------------------------------
  
  tic("Roads")
  
  ## Get OSM roads data ----------------------------------------------------------------
  
  # get roads from OSM
  roads <- opq(bb) %>% 
    add_osm_feature(key = 'highway',
                    value = c('motorway', 'trunk', 'primary', 'secondary', 
                              'tertiary', 'residential', 'unclassified', 
                              'motorway_link', 'trunk_link', 'primary_link', 
                              'secondary_link', 'tertiary_link', 'living_street')) %>% 
    osmdata_sf() 
  
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
  
  
  ## Buffer ------------------------------------------------------------------
  
  # Buffer roads by lanes * 10 ft (3.048 m) 
  # https://nacto.org/publication/urban-street-design-guide/street-design-elements/lane-width/#:~:text=wider%20lane%20widths.-,Lane%20widths%20of%2010%20feet%20are%20appropriate%20in%20urban%20areas,be%20used%20in%20each%20direction
  # cap is flat to the terminus of the road
  # join style is mitred so intersections are squared
  roads <- roads %>% 
    st_buffer(dist = roads$lanes * 3.048,
              endCapStyle = "FLAT",
              joinStyle = "MITRE")
  
  
  ## Rasterize ---------------------------------------------------------------
  
  # Rasterize to match grid of esa and save raster
  roads_rast <- roads %>% 
    rasterize(esa, 
              field = "Value",
              background = 0, 
              filename = paste0(path, "/roads_1m.tif"),
              overwrite = TRUE)
  
  rm(roads)
  
  print("Roads raster saved")
  toc()
  
  # roads_rast <- rast(here(path, "roads_1m.tif"))
  
  # Water -------------------------------------------------------------------
  
  tic("Water")
  
  ## Get OSM water data --------------------------------------------------------------
  
  # get water from OSM
  water1 <- opq(bb) %>% 
    add_osm_feature(key = 'water') %>% 
    osmdata_sf() 
  
  water2 <- opq(bb) %>% 
    add_osm_feature(key = 'natural', value = 'water') %>% 
    osmdata_sf() 
  
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
  
  ## Rasterize ---------------------------------------------------------------
  
  # Rasterize to match grid of esa and save raster
  water_rast <- water %>% 
    rasterize(esa, 
              field = "Value",
              background = 0,
              filename = paste0(path, "/water_1m.tif"),
              overwrite = TRUE)
  
  rm(water)
  
  print("Water raster saved")
  toc()
  
  # water_rast <- rast(here(path, "water_1m.tif"))
  
  # Buildings -------------------------------------------------------------------
  
  tic("Buildings")
  
  ## Get ULU data ---------------------------------------------------
  
  tic("ULU")
  
  ############# Check that first is okay here
  # firstNonNull is used as reducer in GEE script
  # Read ULU land cover, filter to city, select lulc band
  ulu <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
    filterBounds(bb_ee)$
    select('lulc')$
    first()$
    rename('lulc')
  
  roads <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
    filterBounds(bb_ee)$
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
                        region = bb_ee,
                        via = "drive",
                        scale = 5,
                        maxPixels = 10e10,
                        lazy = TRUE) 
  
  ulu <- ulu_img %>% 
    ee_utils_future_value() %>% 
    writeRaster(paste0(path, "/ULU_5m.tif"),
                overwrite = TRUE)
  
  rm(ulu_img)
  
  print("ULU raster saved")
  
  # ulu <- rast(here(path, "ULU_5m.tif"))
  
  ## Get OSM buildings data --------------------------------------------------------------
  
  # get buildings from OSM
  buildings <- opq(bb) %>% 
    add_osm_feature(key = 'building') %>% 
    osmdata_sf() 
  
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
  
  # Extract values to buildings using exact_extract, as coverage fractions
  # https://github.com/isciences/exactextractr
  build_ulu <- exactextractr::exact_extract(ulu, buildings, 'frac') 
  
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
  
  toc()
  
  ## Average building height from the global human settlement layer ----------
  
  tic("ANBH")
  
  # ANBH is the average height of the built surfaces, USE THIS
  # AGBH is the amount of built cubic meters per surface unit in the cell
  # https://ghsl.jrc.ec.europa.eu/ghs_buH2023.php
  
  anbh <- ee$ImageCollection("projects/wri-datalab/GHSL/GHS-BUILT-H-ANBH_R2023A")$
    filterBounds(bb_ee)$ 
    select('b1')$
    first()
  
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
  
  # Reproject buildings to match ANBH
  buildings <- buildings %>% 
    st_transform(crs = st_crs(anbh))
  
  # Extract average of pixel values to buildings
  # Mean value of cells that intersect the polygon, 
  # weighted by the fraction of the cell that is covered.
  avg_ht <- exactextractr::exact_extract(anbh, buildings, 'mean')
  
  buildings <- buildings %>% 
    add_column(ANBH = avg_ht)
  
  toc()
  
  ## Tidy ---------------------------------------------------------------
  
  # Reproject to local state plane and calculate area
  buildings <- buildings %>% 
    st_transform(epsg) %>% 
    mutate(Area_m = as.numeric(st_area(.)))
  
  
  ## Classification of roof slope --------------------------------------------
  tree <- readRDS(here("data", "tree.rds"))
  
  pred <- predict(tree, newdata = buildings, type = "class")
  
  buildings <- buildings %>% 
    add_column(Slope = pred) 
  
  # add value field, low slope = 41, high slope = 42
  buildings <- buildings %>% 
    mutate(Value = case_when(Slope == "low" ~ 41,
                             Slope == "high" ~ 42))
  
  
  ## Rasterize ---------------------------------------------------------------
  
  # Rasterize to match grid of esa and save raster
  buildings_rast <- buildings %>% 
    rasterize(esa, 
              field = "Value",
              background = 0,
              filename = paste0(path, "/buildings_1m.tif"),
              overwrite = TRUE)
  
  # rm(buildings)
  
  print("Buildings raster saved")
  toc()
  
  # buildings_rast <- rast(here(path, "buildings_1m.tif"))
  
  # Parking -----------------------------------------------------------------
  
  tic("Parking")
  
  # get open space from OSM
  parking <- opq(bb) %>% 
    add_osm_feature(key = 'amenity',
                    value = 'parking') %>% 
    osmdata_sf() 
  
  # Combine polygons and multipolygons
  parking <- parking$osm_polygons %>% 
    bind_rows(st_cast(parking$osm_multipolygons, "POLYGON")) 
  
  # Reproject to local state plane and add value field (50)
  parking <- parking %>% 
    st_transform(epsg) %>% 
    mutate(Value = 50)
  
  # Rasterize to match grid of esa and save raster
  parking_rast <- parking %>% 
    rasterize(esa, 
              field = "Value",
              background = 0,
              filename = paste0(path, "/parking_1m.tif"),
              overwrite = TRUE)
  
  rm(parking)
  
  print("Parking raster saved")
  toc()
  
  # parking_rast <- rast(here(path, "parking_1m.tif"))
  
  # Combine rasters ---------------------------------------------------------
  
  tic("Combine rasters")
  
  LULC <- sds(esa,
            open_space_rast,
            roads_rast,
            water_rast,
            buildings_rast,
            parking_rast)
  
  LULC <- app(LULC, max)
  
  # Reclass ESA water (4) to 30
  LULC <- classify(LULC, cbind(4, 30))
  
  writeRaster(LULC, 
              here(path, "LULC_1m.tif"), 
              overwrite = TRUE)
  
  print("LULC raster saved")
  toc()
  toc()
}
  
create_LULC("Los_Angeles", 26945)


