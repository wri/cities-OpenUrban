library(terra)
# library(osmdata)
# library(rgee)
library(gfcanalysis)
library(googledrive)
library(sf)
library(here)
library(tidyverse)
library(aws.s3)
library(glue)


# Helper functions --------------------------------------------------------

list_s3_keys <- function(bucket, prefix, region = NULL, max = Inf) {
  objs <- aws.s3::get_bucket(bucket = bucket, prefix = prefix, region = region, max = max)
  if (length(objs) == 0) return(tibble(Key = character()))
  purrr::map_dfr(objs, ~ tibble(Key = .x[["Key"]]))
}

####

city_name <- "ARG-Buenos_Aires"
temp_path <- here("data", city_name)
if (!dir.exists(temp_path)) {dir.create(temp_path)}

bucket <- "wri-cities-heat"
region <- "us-east-1"
prefix_base <- glue("OpenUrban/{city_name}")
aws_path <- glue("https://{bucket}.s3.{region}.amazonaws.com/{prefix_base}")

grid <- st_read(glue("{aws_path}/city_grid/city_grid.geojson"))

# Get UTM ####
# city_centroid <- grid %>% 
#   st_union() %>% 
#   st_centroid() %>% 
#   st_coordinates()
# 
# utm <- utm_zone(y = city_centroid[2], x = city_centroid[1], proj4string = TRUE) %>% 
#   str_remove("\\+init=epsg:") %>% 
#   as.numeric() %>% 
#   st_crs()

# Create average lanes ####
roads_prefix <- glue("{prefix_base}/roads/")
road_keys <- list_s3_keys(bucket, roads_prefix, region) %>% 
  filter(str_ends(Key, regex("\\.geojson$", ignore_case = TRUE))) %>%
  mutate(url = glue("https://{bucket}.s3.{region}.amazonaws.com/{Key}")) %>%
  pull(url)
  
lanes <- road_keys %>%
  map(\(x) {
    g <- st_read(x, quiet = TRUE) %>% st_drop_geometry() 
    if (nrow(g) == 0) return(NULL)
    if (!"lanes" %in% names(g)) g$lanes <- NA_real_
    g %>% mutate(lanes = as.numeric(lanes))
  }) %>% 
  reduce(bind_rows) %>%
  st_drop_geometry() %>% 
  group_by(highway) %>% 
  summarize(avg.lanes = ceiling(mean(lanes, na.rm = TRUE))) %>% 
  mutate(avg.lanes = case_when(is.na(avg.lanes) ~ 2,
                               !is.na(avg.lanes) ~ avg.lanes))

# TODO: write to s3
write_csv(lanes, here(temp_path, "/average_lanes.csv"))

create_lulc_tile <- function(gridcell_id, city_name, temp_path){
  
  bb <- grid %>% 
    filter(ID == gridcell_id)
  
  # ESA ####
  # Load
  esa_rast <- rast(glue("{aws_path}/esa/esa_{gridcell_id}.tif"))
  
  utm <- st_crs(esa)
  
  # Reclassify
  # Define reclass table
  esa_rcl <- matrix(c(
    0,   0,   # ND → 0
    10, 110,  # Tree → Green
    20, 110,  # Shrubland → Green
    30, 110,  # Grassland → Green
    40, 110,  # Cropland → Green
    50, 120,  # Built up → Built
    60, 130,  # Bare/sparse vegetation → Barren
    80, 300,  # Snow/ice → Water
    70, 300,  # Permanent water bodies → Water
    90, 300,  # Herbaceous wetland → Water
    95, 300,  # Mangroves → Water
    100,130   # Moss/lichen → Barren
  ), ncol = 2, byrow = TRUE)
  
  # Apply classification
  esa_rast <- classify(esa_rast, esa_rcl)
  
  # Open space ####
  # Load
  open_space <- st_read(glue("{aws_path}/open_space/open_space_{gridcell_id}.geojson")) %>% 
    st_make_valid() %>%
    st_collection_extract("POLYGON") %>%
    st_cast("POLYGON", warn = FALSE)
  
  if (nrow(open_space) > 0){
    open_space <- open_space %>% 
      st_transform(utm) %>% 
      mutate(Value = 200) 
    
    # Create raster
    open_space %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here(temp_path, "open_space_1m.tif"),
                overwrite = TRUE)
  } 
  
  
  # Water ####
  # Load
  water <- st_read(glue("{aws_path}/water/water_{gridcell_id}.geojson")) %>% 
    st_make_valid() %>%
    st_collection_extract("POLYGON") %>%
    st_cast("POLYGON", warn = FALSE)
  
  if (nrow(water) > 0){
    water <- water %>% 
      st_transform(utm) %>% 
      mutate(Value = 300) %>% 
      select(any_of(c("Value", "osm_id")))
    
    # Create raster
    water %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here(temp_path, "water_1m.tif"),
                overwrite = TRUE)
  } 
  
  
  
  # Roads ####
  # Load
  roads <- st_read(glue("{aws_path}/roads/roads_{gridcell_id}.geojson")) %>% 
    st_make_valid() %>%
    st_collection_extract("LINESTRING")
  
  if (nrow(roads) > 0){
    # Fill lanes with avg lane value when missing
    roads <- roads %>% 
      mutate(lanes = as.numeric(lanes)) %>% 
      left_join(lanes, by = "highway") %>% 
      mutate(lanes = coalesce(lanes, avg.lanes))
    
    # Add value field 
    roads <- roads %>% 
      mutate(Value = 500) %>% 
      st_transform(utm) 
    
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
    
    
    # Create raster
    roads %>% rasterize(esa_rast, 
                        field = "Value",
                        background = 0, 
                        filename = here(temp_path, "roads_1m.tif"),
                        overwrite = TRUE)
  } 
  
  
  # Buildings ####
  # Load
  buildings <- st_read(glue("{aws_path}/buildings/buildings_{gridcell_id}.tif")) %>% 
    st_make_valid() %>%
    st_collection_extract("POLYGON") %>%
    st_cast("POLYGON", warn = FALSE)
  
  ulu <- rast(glue("{aws_path}/urban_land_use/urban_land_use_{gridcell_id}.tif"))
  
  # Build reclassification matrix
  ulu_rcl <- matrix(c(
    0, 600,  # Open space → 600
    1, 620,  # Non-residential → 620
    2, 610,  # Atomistic → Residential
    3, 610,  # Informal subdivision → Residential
    4, 610,  # Formal subdivision → Residential
    5, 610   # Housing project → Residential
  ), ncol = 2, byrow = TRUE)
  
  # Apply reclassification
  ulu <- classify(ulu, ulu_rcl)
  
  if (nrow(buildings) > 0){
    # If the city is in the U.S. apply the slope classification model
    if (str_detect(city_name, "USA")){
      
      version <- "USA"
      
      # reproject buildings to match ULU
      buildings <- buildings %>% 
        st_transform(st_crs(ulu))
      
      # Extract values to buildings using exact_extract, as coverage fractions
      # https://github.com/isciences/exactextractr
      build_ulu <- exactextractr::exact_extract(ulu, 
                                                buildings, 
                                                'mode',
                                                force_df = TRUE) 
      
      buildings <- buildings %>% 
        add_column(ULU = build_ulu$mode) 
      
      anbh <- rast(glue("{aws_path}/anbh/anbh_{gridcell_id}.tif"))
      
      # reproject buildings to match ANBH
      buildings <- buildings %>% 
        st_transform(st_crs(anbh))
      
      # Extract average of pixel values to buildings
      # Mean value of cells that intersect the polygon, 
      # weighted by the fraction of the cell that is covered.
      avg_ht <- exactextractr::exact_extract(anbh, 
                                             buildings, 
                                             'mean')
      
      buildings <- buildings %>% 
        add_column(ANBH = avg_ht)
      
      # TODO
      # Reproject to local state plane and calculate area
      ######### This is not correct
      ######### should be Area_m = Area_ft / 10.764
      buildings <- buildings %>% 
        st_transform(utm) %>% 
        mutate(Area_ft = as.numeric(st_area(.)),
               Area_m = Area_ft / 3.281,
               ULU = as.factor(ULU),
               ULU = droplevels(replace(ULU, ULU == "0", NA)))
      
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
                                 Class == "non-residential" & Slope == "high" ~ 622)) 
      
      # TODO: Overwrite building file in AWS with new data
      st_write(obj = buildings,
               dsn = glue("{aws_path}/buildings/buildings_{gridcell_id}.tif"),
               append = FALSE,
               delete_dsn = TRUE)
      
    } else {
      
      version <- "global"
      
      # reproject buildings to match ULU
      buildings <- buildings %>% 
        st_transform(st_crs(ulu))
      
      # Extract values to buildings using exact_extract, as coverage fractions
      # https://github.com/isciences/exactextractr
      build_ulu <- exactextractr::exact_extract(ulu, 
                                                buildings, 
                                                'mode',
                                                force_df = TRUE) 
      
      buildings <- buildings %>% 
        add_column(ULU = build_ulu$mode) 
      
      # Add value field and transform to utm
      buildings <- buildings %>% 
        mutate(Value = ULU) %>% 
        st_transform(utm)
      
      # TODO: Overwrite building file in AWS with new data
      st_write(obj = buildings,
               dsn = glue("{aws_path}/buildings/buildings_{gridcell_id}.tif"),
               append = FALSE,
               delete_dsn = TRUE)
    }
    
    # Create raster
    buildings %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here(temp_path, "buildings_1m.tif"),
                            overwrite = TRUE)
  }
  
  # Parking ####
  # Load
  parking <- st_read(glue("{aws_path}/parking/parking_{gridcell_id}.geojson"))
  
  if (nrow(parking) > 0){
    parking <- parking %>% 
      st_transform(utm) %>% 
      mutate(Value = 400) 
    
    # Create raster
    parking %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here(temp_path, "parking_1m.tif"),
                overwrite = TRUE)
  } 
  
  # Combine rasters ####
  open_space_rast <- tryCatch(
    rast(here(temp_path, "open_space_1m.tif")),
    error = function(e) NULL
  )
  roads_rast <- tryCatch(
    rast(here(temp_path, "roads_1m.tif")),
    error = function(e) NULL
  )
  water_rast <- tryCatch(
    rast(here(temp_path, "water_1m.tif")),
    error = function(e) NULL
  )
  buildings_rast <- tryCatch(
    rast(here(temp_path, "buildings_1m.tif")),
    error = function(e) NULL
  )
  parking_rast <- tryCatch(
    rast(here(temp_path, "parking_1m.tif")),
    error = function(e) NULL
  )
  
  LULC <- max(esa_rast,
              open_space_rast,
              roads_rast,
              water_rast,
              buildings_rast,
              parking_rast, na.rm = TRUE)
  
  # TODO: write raster to s3
  writeRaster(x = LULC, 
              filename = here(temp_path, "LULC.tif"),
              overwrite = TRUE,
              datatype = 'INT2U',
              gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512"))
  
  # 2. From local to gcs
  gs_uri <- local_to_gcs(
    x = here(temp_path, "LULC.tif"),
    bucket = 'wri-cities-lulc-gee' 
  )
  
  assetID <- glue("projects/earthengine-legacy/assets/projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC/ 
                    {city_name}_{gridcell_id}")
  
  properties <- list(city = city_name, 
                     grid_cell = gridcell_id, 
                     version = version,
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
  
}