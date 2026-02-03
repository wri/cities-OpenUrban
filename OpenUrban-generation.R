# ============================================================
# Libraries ####
# ============================================================

library(terra)
library(sf)
library(here)
library(tidyverse)
library(glue)
library(geoarrow)
library(sfarrow)
library(processx)
library(future)
library(future.apply)
library(parallel)



# ============================================================
# Setup ####
# ============================================================

Sys.setenv(PYTHONUNBUFFERED = "1")

s3 <- paws::s3()
bucket <- "wri-cities-tcm"
aws_http <- "https://wri-cities-tcm.s3.us-east-1.amazonaws.com"

source(here("utils", "write-s3.R"))
source(here("utils", "open-urban-helpers.R"))

# ============================================================
# create_lulc_tile ####
# ============================================================

create_lulc_tile <- function(
    gridcell_id,
    city,
    city_path,
    upload_gee = FALSE,          # build phase defaults to FALSE (serial EE upload happens later)
    env_name = "open-urban"
) {
  
  open_urban_aws_http <- glue("{aws_http}/OpenUrban/{city}")
  
  # ---- per-tile temp directory (PARALLEL SAFE)
  tmp_dir <- here("tmp", city, as.character(gridcell_id))
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
  
  # ESA ####
  esa_rast <- rast(glue("{city_path}/esa/esa_{gridcell_id}.tif"))
  utm <- st_crs(esa_rast)
  
  bb <- st_read(glue("{city_path}/city_grid/city_grid.geojson"), quiet = TRUE) %>%
    filter(ID == gridcell_id) %>%
    st_transform(utm)
  
  # Reclassify ESA
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
  
  esa_rast <- classify(esa_rast, esa_rcl)
  
  # Open space ####
  open_space <- sfarrow::st_read_parquet(
    glue("{city_path}/open_space/open_space_{gridcell_id}.parquet"),
    quiet = TRUE
  ) %>%
    st_transform(utm) %>%
    st_filter(bb)
  
  if (nrow(open_space) > 0) {
    open_space <- open_space %>%
      mutate(Value = 200)
    
    open_space %>%
      rasterize(
        esa_rast,
        field = "Value",
        background = 0,
        filename = file.path(tmp_dir, "open_space_1m.tif"),
        overwrite = TRUE
      )
  }
  
  # Water ####
  water <- sfarrow::st_read_parquet(
    glue("{city_path}/water/water_{gridcell_id}.parquet"),
    wkt_filter = st_as_text(bb),
    quiet = TRUE
  ) %>%
    st_transform(utm) %>%
    st_filter(bb)
  
  if (nrow(water) > 0) {
    water <- water %>%
      mutate(Value = 300)
    
    water %>%
      rasterize(
        esa_rast,
        field = "Value",
        background = 0,
        filename = file.path(tmp_dir, "water_1m.tif"),
        overwrite = TRUE
      )
  }
  
  # Roads ####
  roads <- sfarrow::st_read_parquet(
    glue("{city_path}/roads/roads_{gridcell_id}.parquet"),
    quiet = TRUE
  ) %>%
    st_transform(utm) %>%
    st_filter(bb)
  
  avg_lanes <- read_csv(
    glue("{open_urban_aws_http}/roads/average_lanes.csv"),
    show_col_types = FALSE
  )
  
  if (nrow(roads) > 0) {
    
    roads <- roads %>%
      mutate(lanes = as.numeric(lanes)) %>%
      left_join(avg_lanes, by = "highway") %>%
      mutate(lanes = coalesce(lanes, avg_lanes)) %>%
      mutate(Value = 500)
    
    # Buffer roads by lanes * lane_width/2
    # If CRS units are feet use 10 ft, else 3.048 m (10 ft)
    units <- st_crs(roads)$units %||% NA_character_
    if (!is.na(units) && units %in% c("us-ft", "ft")) {
      width <- 10
    } else {
      width <- 3.048
    }
    
    roads <- roads %>%
      st_buffer(
        dist = roads$lanes * (width / 2),
        endCapStyle = "FLAT",
        joinStyle = "MITRE"
      )
    
    roads %>%
      rasterize(
        esa_rast,
        field = "Value",
        background = 0,
        filename = file.path(tmp_dir, "roads_1m.tif"),
        overwrite = TRUE
      )
  }
  
  # Buildings ####
  buildings <- sfarrow::st_read_parquet(
    glue("{city_path}/buildings/buildings_{gridcell_id}.parquet"),
    quiet = TRUE
  ) %>%
    select(id)
  
  ulu <- rast(glue("{city_path}/urban_land_use/urban_land_use_{gridcell_id}.tif"))
  
  # Build reclassification matrix (ULU → building codes)
  ulu_rcl <- matrix(c(
    0, 0,  # Open space → Unclassified
    1, 1,  # Non-residential → Non-residential
    2, 2,  # Atomistic → Residential
    3, 2,  # Informal subdivision → Residential
    4, 2,  # Formal subdivision → Residential
    5, 2   # Housing project → Residential
  ), ncol = 2, byrow = TRUE)
  
  ulu <- classify(ulu, ulu_rcl)
  
  if (nrow(buildings) > 0) {
      
    # reproject buildings to match ULU
    buildings <- buildings %>%
      st_transform(st_crs(ulu))
    
    build_ulu <- exactextractr::exact_extract(
      ulu,
      buildings,
      'mode'
    )
    
    buildings <- buildings %>%
      add_column(ULU = build_ulu)
    
    anbh <- rast(glue("{city_path}/anbh/anbh_{gridcell_id}.tif"))
    
    # reproject buildings to match ANBH
    buildings <- buildings %>%
      st_transform(st_crs(anbh))
    
    avg_ht <- exactextractr::exact_extract(
      anbh,
      buildings,
      'mean'
    )
    
    buildings <- buildings %>%
      add_column(ANBH = avg_ht) 
    
    buildings <- buildings %>%
      st_transform(utm) %>%
      mutate(
        Area_m = as.numeric(st_area(.)),
        ULU = replace_na(ULU, 0)
      )
      
    if (str_detect(city, "USA")) {
      
      version <- "USA"
      
      buildings <- buildings %>%
        mutate(
          Slope = case_when(
            ULU == 2 ~ "high",
            
            # Only use ANBH threshold when ANBH is present
            ULU %in% c(0, 1) & Area_m < 1034 & !is.na(ANBH) ~ if_else(ANBH < 11, "high", "low"),
            ULU %in% c(0, 1) & Area_m >= 1034 ~ "low",
            
            # If ANBH missing, classify by area
            ULU %in% c(0, 1) & is.na(ANBH) & Area_m <= 821 ~ "high",
            ULU %in% c(0, 1) & is.na(ANBH) & Area_m > 821 ~ "low",
            
            TRUE ~ NA_character_
          )
        )
      
      
      buildings <- buildings %>%
        mutate(
          Class = case_when(
            ULU == 1 ~ "non-residential",
            ULU == 2 ~ "residential",
            is.na(ULU) ~ "unclassified",
            TRUE ~ "unclassified"
          ),
          Value = case_when(
            Class == "unclassified"     & Slope == "low"  ~ 601,
            Class == "unclassified"     & Slope == "high" ~ 602,
            Class == "residential"      & Slope == "low"  ~ 611,
            Class == "residential"      & Slope == "high" ~ 612,
            Class == "non-residential"  & Slope == "low"  ~ 621,
            Class == "non-residential"  & Slope == "high" ~ 622,
            TRUE ~ NA_real_
          )
        )
      
    } else {
      
      version <- "global"
      
      buildings <- buildings %>%
        mutate(
          Class = case_when(
            ULU == 1 ~ "non-residential",
            ULU == 2 ~ "residential",
            is.na(ULU) ~ "unclassified",
            TRUE ~ "unclassified"
          ),
          Value = case_when(
            Class == "unclassified" ~ 600,
            Class == "residential" ~ 610,
            Class == "non-residential" ~ 620
          )
        )
  
    }

    write_s3(buildings, glue("{bucket}/OpenUrban/{city}/buildings/buildings_{gridcell_id}.parquet"))
    
    # Rasterize buildings
    buildings %>%
      rasterize(
        esa_rast,
        field = "Value",
        background = 0,
        filename = file.path(tmp_dir, "buildings_1m.tif"),
        overwrite = TRUE
      )
  }
  
  # Parking ####
  parking <- sfarrow::st_read_parquet(
    glue("{city_path}/parking/parking_{gridcell_id}.parquet"),
    quiet = TRUE
  ) %>%
    st_transform(utm) %>%
    st_filter(bb)
  
  if (nrow(parking) > 0) {
    parking <- parking %>%
      st_transform(utm) %>%
      mutate(Value = 400)
    
    parking %>%
      rasterize(
        esa_rast,
        field = "Value",
        background = 0,
        filename = file.path(tmp_dir, "parking_1m.tif"),
        overwrite = TRUE
      )
  }
  
  # Combine rasters ####
  open_space_rast <- tryCatch(rast(file.path(tmp_dir, "open_space_1m.tif")), error = function(e) NULL)
  roads_rast      <- tryCatch(rast(file.path(tmp_dir, "roads_1m.tif")),      error = function(e) NULL)
  water_rast      <- tryCatch(rast(file.path(tmp_dir, "water_1m.tif")),      error = function(e) NULL)
  buildings_rast  <- tryCatch(rast(file.path(tmp_dir, "buildings_1m.tif")),  error = function(e) NULL)
  parking_rast    <- tryCatch(rast(file.path(tmp_dir, "parking_1m.tif")),    error = function(e) NULL)
  
  LULC <- max(
    esa_rast,
    open_space_rast,
    roads_rast,
    water_rast,
    buildings_rast,
    parking_rast,
    na.rm = TRUE
  )
  
  # Save per-tile local file (same name within per-tile tmp_dir is fine)
  local_file <- file.path(tmp_dir, "LULC.tif")
  
  writeRaster(
    x = LULC,
    filename = local_file,
    overwrite = TRUE,
    datatype = 'INT2U',
    gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512")
  )
  
  # Save to S3 (unique key per tile)
  write_s3(LULC, glue("wri-cities-tcm/OpenUrban/{city}/OpenUrban/{city}_{gridcell_id}.tif"))
  
  print(glue("LULC raster {gridcell_id} saved to S3"))
  
  # Optional upload to GEE (disabled during parallel build by default)
  if (isTRUE(upload_gee)) {
    conda <- "/home/ubuntu/miniconda3/condabin/conda"
    script_path <- normalizePath("upload_gee.py")
    
    gee_args <- c(
      "--gcs-bucket", "wri-cities-gee-imports",
      "--local-file", local_file,
      "--collection-id", "projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC",
      "--city-name", city,
      "--gridcell-id", gridcell_id,
      "--version", version,
      "--overwrite"
    )
    
    args <- c(
      "run", "--no-capture-output",
      "-n", env_name,
      "python", "-u", script_path, gee_args
    )
    
    run_python_live(args)
  }
  
  invisible(list(
    gridcell_id = gridcell_id,
    city = city,
    tmp_dir = tmp_dir,
    local_file = local_file,
    s3_key = glue("OpenUrban/{city}/OpenUrban/{city}_{gridcell_id}.tif"),
    version = version
  ))
}


# ============================================================
# City wrapper ####
# ============================================================

generate_openurban_city <- function(city) {
  
  city_path <- here("data", city)
  dir.create(city_path, recursive = TRUE, showWarnings = FALSE)
  
  args <- c(
    "run","--no-capture-output",
    "-n","open-urban",
    "python","-u","get_data.py", city
  )
  run_python_live(args)
  
  grid <- st_read(glue("{city_path}/city_grid/city_grid.geojson"),
                  quiet = TRUE)
  
  if (str_detect(city, "USA")) {
    version <- "USA"} else {version <- "global"}
  
  # Create LULC tiles in parallel
  # workers <- max(1L, min(6L, parallel::detectCores() - 2L))
  # options(warn = 1)          # print warnings as they happen
  # options(error = recover)   # interactive stack trace in RStudio (optional)
  # 
  # future::plan(future::multisession, workers = workers)
  # message(glue("Building tiles in parallel with {workers} workers"))
  # 
  # future.apply::future_lapply(
  #   grid$ID,
  #   function(id) create_lulc_tile(id, city = city, city_path = city_path),
  #   future.seed = TRUE
  # )
  map(grid$ID, create_lulc_tile, city = city, city_path = city_path)
  
  
  # Check that all files are in s3
  
  expected_keys <- openurban_expected_s3_keys(city, grid$ID)
  s3_check <- s3_keys_exist_by_prefix(s3, bucket, expected_keys)
  
  if (!s3_check$ok) {
    message("S3 incomplete; keeping local data.")
    print(head(s3_check$missing, 50))
    return(invisible(list(city=city, s3=s3_check)))
  }
  
  message("S3 complete. Deleting tmp and local city folder...")
  
  unlink(here("tmp", city), recursive = TRUE, force = TRUE)
  unlink(here("data", city), recursive = TRUE, force = TRUE)
  
  # Upload serially to GEE
  conda <- "/home/ubuntu/miniconda3/condabin/conda"
  env_name <- "open-urban"
  script_path <- normalizePath("upload_gee.py")
  
  for (id in grid$ID) {
    s3_key <- glue("OpenUrban/{city}/OpenUrban/{city}_{id}.tif")
    
    gee_args <- c(
      "--gcs-bucket", "wri-cities-gee-imports",
      "--gcs-blob-name", "OpenUrban_LULC/LULC.tif",     # reused each time
      "--s3-bucket", "wri-cities-tcm",
      "--s3-key", s3_key,
      "--collection-id", "projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC",
      "--city-name", city,
      "--gridcell-id", id,
      "--version", if (str_detect(city, "USA")) "USA" else "global",
      "--overwrite",
      "--wait", "600"   # pick a number that actually blocks long enough
    )
    
    args <- c("run","--no-capture-output","-n", env_name, "python","-u", script_path, gee_args)
    run_python_live(args)
  }
  
  
}

# ============================================================
# RUN
# ============================================================

generate_openurban_city("NLD-Rotterdam")








# library(terra)
# library(sf)
# library(here)
# library(tidyverse)
# library(glue)
# library(geoarrow)
# library(sfarrow)
# 
# 
# 
# 
# 
# 
# 
# 
# 
# Sys.setenv(PYTHONUNBUFFERED = "1")
# 
# 
# 
# # S3 setup ----------------------------------------------------------------
# 
# 
# # 1) Log in via AWS SSO for that profile (opens browser)
# # system("aws sso login --profile cities-data-dev")
# # 
# # # 2) Point this R/reticulate session at that profile & region
# # Sys.setenv(
# #   AWS_SDK_LOAD_CONFIG = "1",      # make boto3 read ~/.aws/config
# #   AWS_PROFILE = "cities-data-dev",
# #   AWS_DEFAULT_REGION = "us-east-1"
# # )
# 
# s3 <- paws::s3()
# bucket <- "wri-cities-tcm"
# aws_http <- "https://wri-cities-tcm.s3.us-east-1.amazonaws.com"
# 
# # Load write_s3
# source(here("utils", "write-s3.R"))
# 
# ####
# city <- "NLD-Rotterdam"
# city_path <- here("data", city)
# if (!dir.exists(city_path)) {dir.create(city_path)}
# 
# open_urban_aws_http <- glue("{aws_http}/OpenUrban/{city}")
# 
# # Get extent from CIF bucket
# # extent <- st_read("https://wri-cities-indicators.s3.us-east-1.amazonaws.com/data/published/layers/UrbanExtents/geojson/{city}__urban_extent__UrbanExtents__StartYear_2020_EndYear_2020.geojson")
# # if (!dir.exists(here(city_path, "boundaries"))) {dir.create(here(city_path, "boundaries"))}
# # boundary_path <- here(city_path, "boundaries", "city_polygon.geojson")
# # if (!file.exists(boundary_path)) {st_write(extent, boundary_path)}
# 
# # Get data
# 
# # Define your parameters
# env_name <- "open-urban"
# script_path <- "get_data.py"
# script_args <- city
# 
# # Combine 'run', the environment name, 'python', the script, and its arguments
# args <- c(
#   "run", "--no-capture-output",
#   "-n", env_name,
#   "python", "-u", script_path, script_args
# )
# 
# # Execute via system2
# # system2(
# #   command = "/home/ubuntu/miniconda3/condabin/conda", 
# #   args = args,
# #   stdout  = "",
# #   stderr  = ""
# # )
# 
# run_python_live(args)
# 
# # Print output
# # print(response)
# 
# # Load the grid
# grid <- st_read(glue("{open_urban_aws_http}/city_grid/city_grid.geojson"))
# 
# create_lulc_tile <- function(gridcell_id, city, city_path){
#   
#   # ESA ####
#   # Load
#   esa_rast <- rast(glue("{city_path}/esa/esa_{gridcell_id}.tif"))
#   
#   utm <- st_crs(esa_rast)
#   
#   bb <- grid %>% 
#     filter(ID == gridcell_id) %>% 
#     st_transform(utm)
#   
#   # Reclassify
#   # Define reclass table
#   esa_rcl <- matrix(c(
#     0,   0,   # ND → 0
#     10, 110,  # Tree → Green
#     20, 110,  # Shrubland → Green
#     30, 110,  # Grassland → Green
#     40, 110,  # Cropland → Green
#     50, 120,  # Built up → Built
#     60, 130,  # Bare/sparse vegetation → Barren
#     80, 300,  # Snow/ice → Water
#     70, 300,  # Permanent water bodies → Water
#     90, 300,  # Herbaceous wetland → Water
#     95, 300,  # Mangroves → Water
#     100,130   # Moss/lichen → Barren
#   ), ncol = 2, byrow = TRUE)
#   
#   # Apply classification
#   esa_rast <- classify(esa_rast, esa_rcl)
#   
#   # Open space ####
#   # Load
#   open_space <- sfarrow::st_read_parquet(
#       glue("{open_urban_aws_http}/open_space/open_space_all.parquet"),
#       quiet = TRUE) %>% 
#     st_transform(utm) %>% 
#     st_filter(bb)
#   
#   if (nrow(open_space) > 0){
#     open_space <- open_space %>% 
#       mutate(Value = 200) 
#     
#     # Create raster
#     open_space %>% 
#       rasterize(esa_rast, 
#                 field = "Value",
#                 background = 0,
#                 filename = here("tmp", "open_space_1m.tif"),
#                 overwrite = TRUE)
#   } 
#   
#   
#   # Water ####
#   # Load
#   water <- sfarrow::st_read_parquet(
#       glue("{open_urban_aws_http}/water/water_all.parquet"),
#       wkt_filter = st_as_text(bb),   
#       quiet = TRUE) %>% 
#     st_transform(utm) %>% 
#     st_filter(bb)
#   
#   if (nrow(water) > 0){
#     water <- water %>% 
#       mutate(Value = 300) 
#     
#     # Create raster
#     water %>% 
#       rasterize(esa_rast, 
#                 field = "Value",
#                 background = 0,
#                 filename = here("tmp", "water_1m.tif"),
#                 overwrite = TRUE)
#   } 
#   
#   
#   
#   # Roads ####
#   roads <- sfarrow::st_read_parquet(
#       glue("{open_urban_aws_http}/roads/roads_all.parquet"),
#       wkt_filter = st_as_text(bb),   
#       quiet = TRUE) %>% 
#     st_transform(utm) %>% 
#     st_filter(bb)
#   
#   avg_lanes <- read_csv(glue("{open_urban_aws_http}/roads/average_lanes.csv"))
#   
#   if (nrow(roads) > 0){
#     # Fill lanes with avg lane value when missing
#     roads <- roads %>% 
#       mutate(lanes = as.numeric(lanes)) %>% 
#       left_join(avg_lanes, by = "highway") %>% 
#       mutate(lanes = coalesce(lanes, avg_lanes))
#     
#     # Add value field 
#     roads <- roads %>% 
#       mutate(Value = 500) 
#     
#     # Buffer roads by lanes * 10 ft (3.048 m) 
#     # https://nacto.org/publication/urban-street-design-guide/street-design-elements/lane-width/#:~:text=wider%20lane%20widths.-,Lane%20widths%20of%2010%20feet%20are%20appropriate%20in%20urban%20areas,be%20used%20in%20each%20direction
#     # cap is flat to the terminus of the road
#     # join style is mitred so intersections are squared
#     if (st_crs(roads)$units == "us-ft"){
#       width = 10
#     } else if (st_crs(roads)$units == "ft"){
#       width = 10
#     } else if (st_crs(roads)$units == "m"){
#       width = 3.048
#     } 
#     
#     roads <- roads %>% 
#       st_buffer(dist = roads$lanes * (width / 2),
#                 endCapStyle = "FLAT",
#                 joinStyle = "MITRE") 
#     
#     # Create raster
#     roads %>% rasterize(esa_rast, 
#                         field = "Value",
#                         background = 0, 
#                         filename = here("tmp", "roads_1m.tif"),
#                         overwrite = TRUE)
#   } 
#   
#   
#   # Buildings ####
#   # Load
#   buildings <- sfarrow::st_read_parquet(
#     glue("{open_urban_aws_http}/buildings/buildings_{gridcell_id}.parquet"),
#     quiet = TRUE) %>% 
#     select(id, version)
#   
#   ulu <- rast(glue("{city_path}/urban_land_use/urban_land_use_{gridcell_id}.tif"))
#   
#   # Build reclassification matrix
#   ulu_rcl <- matrix(c(
#     0, 600,  # Open space → 600
#     1, 620,  # Non-residential → 620
#     2, 610,  # Atomistic → Residential
#     3, 610,  # Informal subdivision → Residential
#     4, 610,  # Formal subdivision → Residential
#     5, 610   # Housing project → Residential
#   ), ncol = 2, byrow = TRUE)
#   
#   # Apply reclassification
#   ulu <- classify(ulu, ulu_rcl)
#   
#   if (nrow(buildings) > 0){
#     # If the city is in the U.S. apply the slope classification model
#     if (str_detect(city, "USA")){
#       
#       version <- "USA"
#       
#       # reproject buildings to match ULU
#       buildings <- buildings %>% 
#         st_transform(st_crs(ulu))
#       
#       # Extract values to buildings using exact_extract, as coverage fractions
#       # https://github.com/isciences/exactextractr
#       build_ulu <- exactextractr::exact_extract(ulu, 
#                                                 buildings, 
#                                                 'mode',
#                                                 force_df = TRUE) 
#       
#       buildings <- buildings %>% 
#         add_column(ULU = build_ulu$mode) 
#       
#       anbh <- rast(glue("{city_path}/anbh/anbh_{gridcell_id}.tif"))
#       
#       # reproject buildings to match ANBH
#       buildings <- buildings %>% 
#         st_transform(st_crs(anbh))
#       
#       # Extract average of pixel values to buildings
#       # Mean value of cells that intersect the polygon, 
#       # weighted by the fraction of the cell that is covered.
#       avg_ht <- exactextractr::exact_extract(anbh, 
#                                              buildings, 
#                                              'mean')
#       
#       buildings <- buildings %>% 
#         add_column(ANBH = avg_ht)
#       
#       # Reproject to UTM and calculate area
#       buildings <- buildings %>% 
#         st_transform(utm) %>% 
#         mutate(Area_m = as.numeric(st_area(.)),
#                ULU = as.factor(ULU),
#                ULU = droplevels(replace(ULU, ULU == "0", NA)))
#       
#       classify_slope <- function(buildings) {
#         buildings %>%
#           mutate(
#             Slope = case_when(
#               # Residential -> High slope
#               ULU == 2 ~ "high",
#               
#               # Non-residential: split on area
#               ULU == 1 & Area_m < 1034 ~ if_else(ANBH < 11, "high", "low"),
#               ULU == 1 & Area_m >= 1034 ~ "low",
#               
#               # Anything else / missing
#               TRUE ~ NA_character_
#             )
#           )
#       }
#       
#       buildings <- classify_slope(buildings)
#       
#       # For V3 reserve single digit space for classification
#       buildings <- buildings %>% 
#         mutate(Class = case_when(ULU == 2 ~ "non-residential",
#                                  ULU == 1 ~ "residential",
#                                  is.na(ULU) ~ "unclassified"),
#                Value = case_when(Class == "unclassified" & Slope == "low" ~ 601,
#                                  Class == "unclassified" & Slope == "high" ~ 602,
#                                  Class == "residential" & Slope == "low" ~ 611,
#                                  Class == "residential" & Slope == "high" ~ 612,
#                                  Class == "non-residential" & Slope == "low" ~ 621,
#                                  Class == "non-residential" & Slope == "high" ~ 622)) 
#       
#       # Overwrite building file in with new data
#       sfarrow::st_write_parquet(
#         obj = buildings,
#         dsn = here("data", city, glue("buildings/buildings_{gridcell_id}.parquet"))
#       )
#       
#       write_s3(buildings, glue("{bucket}/OpenUrban/{city}/buildings/buildings_{gridcell_id}.parquet"))
#       
#     } else {
#       
#       version <- "global"
#       
#       # reproject buildings to match ULU
#       buildings <- buildings %>% 
#         st_transform(st_crs(ulu))
#       
#       # Extract values to buildings using exact_extract, as coverage fractions
#       # https://github.com/isciences/exactextractr
#       build_ulu <- exactextractr::exact_extract(ulu, 
#                                                 buildings, 
#                                                 'mode',
#                                                 force_df = TRUE) 
#       
#       buildings <- buildings %>% 
#         add_column(ULU = build_ulu$mode) 
#       
#       # Add value field and transform to utm
#       buildings <- buildings %>% 
#         mutate(Value = ULU) %>% 
#         st_transform(utm)
#       
#       # Overwrite building file in with new data
#       sfarrow::st_write_parquet(
#         obj = buildings,
#         dsn = here("data", city, glue("buildings/buildings_{gridcell_id}.parquet"))
#       )
#       
#       write_s3(buildings, glue("{bucket}/OpenUrban/{city}/buildings/buildings_{gridcell_id}.parquet"))
#     }
#     
#     # Create raster
#     buildings %>% rasterize(esa_rast, 
#                             field = "Value",
#                             background = 0,
#                             filename = here("tmp", "buildings_1m.tif"),
#                             overwrite = TRUE)
#   }
#   
#   # Parking ####
#   # Load
#   parking <- sfarrow::st_read_parquet(
#     glue("{open_urban_aws_http}/parking/parking_all.parquet"),
#     quiet = TRUE) %>% 
#     st_filter(bb)
#   
#   if (nrow(parking) > 0){
#     parking <- parking %>% 
#       st_transform(utm) %>% 
#       mutate(Value = 400) 
#     
#     # Create raster
#     parking %>% 
#       rasterize(esa_rast, 
#                 field = "Value",
#                 background = 0,
#                 filename = here("tmp", "parking_1m.tif"),
#                 overwrite = TRUE)
#   } 
#   
#   # Combine rasters ####
#   open_space_rast <- tryCatch(
#     rast(here("tmp", "open_space_1m.tif")),
#     error = function(e) NULL
#   )
#   roads_rast <- tryCatch(
#     rast(here("tmp", "roads_1m.tif")),
#     error = function(e) NULL
#   )
#   water_rast <- tryCatch(
#     rast(here("tmp", "water_1m.tif")),
#     error = function(e) NULL
#   )
#   buildings_rast <- tryCatch(
#     rast(here("tmp", "buildings_1m.tif")),
#     error = function(e) NULL
#   )
#   parking_rast <- tryCatch(
#     rast(here("tmp", "parking_1m.tif")),
#     error = function(e) NULL
#   )
#   
#   LULC <- max(esa_rast,
#               open_space_rast,
#               roads_rast,
#               water_rast,
#               buildings_rast,
#               parking_rast, na.rm = TRUE)
#   
#   # Save local tmp file
#   writeRaster(x = LULC, 
#               filename = here("tmp", "LULC.tif"),
#               overwrite = TRUE,
#               datatype = 'INT2U',
#               gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512"))
#   
#   local_file <- here("tmp", "LULC.tif")
#   
#   # Save to GEE
#   conda <- "/home/ubuntu/miniconda3/condabin/conda"
#   script_path <- normalizePath("upload_gee.py")
#   
#   gee_args <- c(
#     "--gcs-bucket", "wri-cities-gee-imports",
#     "--local-file", local_file,
#     "--collection-id", "projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC",
#     "--city-name", city,
#     "--gridcell-id", gridcell_id,
#     "--version", version,
#     "--overwrite"
#   )
#   
#   args <- c(
#     "run", "--no-capture-output",
#     "-n", env_name,
#     "python", "-u", script_path, gee_args
#   )
#   
#   run_python_live(args)
#   
#   # Save to s3
#   write_s3(LULC, glue("wri-cities-tcm/OpenUrban/{city}/OpenUrban/{city}_{gridcell_id}.tif"))
#   
#   print(glue("LULC raster {gridcell_id} of {nrow(grid)} saved"))
#   
# }
# 
# map(
#   grid$ID,
#   create_lulc_tile,
#   city = city,
#   city_path = city_path
# )
# 
# # Delete local data
# # Check to see that all tiles are in GEE
# # Check to see that all files are in S3
# # if they are, delete city_path folder
