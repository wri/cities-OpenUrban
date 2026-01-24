library(terra)
# library(osmdata)
library(rgee)
library(gfcanalysis)
library(googledrive)
library(sf)
library(here)
library(tidyverse)
# library(aws.s3)
library(glue)
library(geoarrow)
library(sfarrow)
library(googleCloudStorageR)

library(reticulate)
use_condaenv("chri", required = TRUE)

# S3 setup ----------------------------------------------------------------


# 1) Log in via AWS SSO for that profile (opens browser)
system("aws sso login --profile cities-data-dev")

# 2) Point this R/reticulate session at that profile & region
Sys.setenv(
  AWS_SDK_LOAD_CONFIG = "1",      # make boto3 read ~/.aws/config
  AWS_PROFILE = "cities-data-dev",
  AWS_DEFAULT_REGION = "us-east-1"
)

s3 <- paws::s3()
bucket <- "wri-cities-heat"
aws_http <- "https://wri-cities-heat.s3.us-east-1.amazonaws.com"

write_s3 <- function(obj, file_path) {
  s3_uri <- glue::glue("s3://{file_path}")
  
  # parse bucket/key
  p   <- sub("^s3://", "", s3_uri)
  bkt <- sub("/.*", "", p)
  key <- sub("^[^/]+/", "", p)
  ext <- tolower(tools::file_ext(key))
  tmp <- tempfile(fileext = paste0(".", ext))
  
  # choose writer + content-type
  if (ext %in% c("parquet", "geoparquet", "pq")) {
    # write Parquet / GeoParquet
    if (inherits(obj, "sf") || inherits(obj, "sfc")) {
      if (inherits(obj, "sfc")) obj <- sf::st_as_sf(obj)
      if (!requireNamespace("sfarrow", quietly = TRUE))
        stop("Package 'sfarrow' is required to write GeoParquet.")
      sfarrow::st_write_parquet(obj, tmp)
    } else {
      if (!requireNamespace("arrow", quietly = TRUE))
        stop("Package 'arrow' is required to write Parquet.")
      arrow::write_parquet(as.data.frame(obj), tmp)
    }
    ctype <- "application/vnd.apache.parquet"
    
  } else if (inherits(obj, "sf") || inherits(obj, "sfc")) {
    # vector data (e.g., GeoJSON, GPKG, etc.)
    if (inherits(obj, "sfc")) obj <- sf::st_as_sf(obj)
    sf::st_write(obj, tmp, delete_dsn = TRUE, quiet = TRUE)
    ctype <- if (ext %in% c("geojson","json")) "application/geo+json" else "application/octet-stream"
    
  } else if (inherits(obj, "SpatRaster")) {
    # raster data
    terra::writeRaster(obj, tmp, overwrite = TRUE)
    ctype <- if (ext %in% c("tif","tiff")) "image/tiff" else "application/octet-stream"
    
  } else if (ext == "csv") {
    # tabular CSV
    readr::write_csv(obj, file = tmp)
    ctype <- "text/csv"
    
  } else {
    stop("Unsupported extension: ", ext)
  }
  
  # upload
  raw <- readBin(tmp, "raw", file.info(tmp)$size)
  s3$put_object(Bucket = bkt, Key = key, Body = raw, ContentType = ctype)
  unlink(tmp)
  invisible(s3_uri)
}


# Rgee setup -------------------------------------------------------------
py_run_file(here("utils", "rgee.py"))
ee_Initialize(project = "ee-ssc-lulc", drive = TRUE, quiet = TRUE)
Sys.setenv(EARTHENGINE_PROJECT = "ee-ssc-lulc")

gcs_auth("/Users/elizabeth.wesley/Documents/keys/nifty-memory-399115-7aa7f647b0b0.json")  
Sys.setenv(GCS_DEFAULT_BUCKET = "wri-cities-lulc-gee")
gcs_global_bucket("wri-cities-lulc-gee")  
googleCloudStorageR::gcs_upload_set_limit(as.integer(2.5e+7))  

gcs_upload_until_success <- function(file,
                                     bucket,
                                     name = basename(file),
                                     type = "image/tiff",
                                     verbose = TRUE) {
  stopifnot(file.exists(file))
  
  # make sure package is loaded
  if (!"googleCloudStorageR" %in% .packages()) {
    suppressPackageStartupMessages(require(googleCloudStorageR))
  }
  
  # 1) raise the resumable limit ABOVE your file size
  #    50 MB is fine; must be integer
  googleCloudStorageR::gcs_upload_set_limit(as.integer(50 * 1024^2))
  
  attempt <- 1L
  wait <- 2
  
  repeat {
    res <- tryCatch(
      googleCloudStorageR::gcs_upload(
        file = file,
        bucket = bucket,
        name = name,
        type = type,
        upload_type = "simple"   # <-- force simple here
      ),
      error = function(e) e
    )
    
    if (!inherits(res, "error")) {
      if (verbose) message("✅ upload succeeded on attempt ", attempt)
      return(res)
    }
    
    # otherwise, we retry forever
    if (verbose) {
      message("❌ attempt ", attempt, " failed: ", conditionMessage(res))
      message("⏳ retrying in ", wait, "s (Ctrl+C to stop)")
    }
    Sys.sleep(wait)
    wait <- min(wait * 1.5, 30)
    attempt <- attempt + 1L
  }
}

####


city_path <- here("data", city_name)
if (!dir.exists(city_path)) {dir.create(city_path)}

open_urban_aws_http <- glue("{aws_http}/OpenUrban/{city_name}")

source_python(here("get_data.py"))

# Add the Python script folder to sys.path
script_dir <- here()  
py_run_string(sprintf("import sys; sys.path.append('%s')", script_dir))

grid <- st_read(glue("{open_urban_aws_http}/city_grid/city_grid.geojson"))

create_lulc_tile <- function(gridcell_id, city_name, city_path){
  
  # ESA ####
  # Load
  esa_rast <- rast(glue("{city_path}/esa/esa_{gridcell_id}.tif"))
  
  utm <- st_crs(esa_rast)
  
  bb <- grid %>% 
    filter(ID == gridcell_id) %>% 
    st_transform(utm)
  
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
  # open_space <- st_read(glue("{city_path}/open_space/open_space_{gridcell_id}.geojson")) %>% 
  #   st_make_valid() %>%
  #   st_collection_extract("POLYGON") %>%
  #   st_cast("POLYGON", warn = FALSE)
  # open_space_path <- glue("{city_path}/open_space/open_space_all.geojson")
  # open_space <- vect(open_space_path, extent = bb) %>% 
  #   st_as_sf() 
  open_space <- sfarrow::st_read_parquet(
    glue("{open_urban_aws_http}/open_space/open_space_all.parquet"),
    wkt_filter = st_as_text(bb),   
    quiet = TRUE) %>% 
    st_transform(utm) %>% 
    st_filter(bb)
  
  if (nrow(open_space) > 0){
    open_space <- open_space %>% 
      mutate(Value = 200) 
    
    # Create raster
    open_space %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here("temp", "open_space_1m.tif"),
                overwrite = TRUE)
  } 
  
  
  # Water ####
  # Load
  # water_path <- glue("{city_path}/water/water_all.geojson")
  # water <- vect(water_path, extent = bb) %>% 
  #   st_as_sf() 
  
  water <- sfarrow::st_read_parquet(
    glue("{open_urban_aws_http}/water/water_all.parquet"),
    wkt_filter = st_as_text(bb),   
    quiet = TRUE) %>% 
    st_transform(utm) %>% 
    st_filter(bb)
  
  if (nrow(water) > 0){
    water <- water %>% 
      mutate(Value = 300) 
    
    # Create raster
    water %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here("temp", "water_1m.tif"),
                overwrite = TRUE)
  } 
  
  
  
  # Roads ####
  # Load
  # roads_path <- glue("{open_urban_aws_http}/roads/roads_all.parquet")
  # # roads_path <- glue("{city_path}/roads/roads_all.geojson")
  # roads <- vect(roads_path, extent = bb) %>% 
  #   st_as_sf() 
  roads <- sfarrow::st_read_parquet(
    glue("{open_urban_aws_http}/roads/roads_all.parquet"),
    wkt_filter = st_as_text(bb),   
    quiet = TRUE) %>% 
    st_transform(utm) %>% 
    st_filter(bb)
  
  avg_lanes <- read_csv(glue("{open_urban_aws_http}/roads/average_lanes.csv"))
  
  if (nrow(roads) > 0){
    # Fill lanes with avg lane value when missing
    roads <- roads %>% 
      mutate(lanes = as.numeric(lanes)) %>% 
      left_join(avg_lanes, by = "highway") %>% 
      mutate(lanes = coalesce(lanes, avg_lanes))
    
    # Add value field 
    roads <- roads %>% 
      mutate(Value = 500) 
    
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
                joinStyle = "MITRE") 
    
    # Create raster
    roads %>% rasterize(esa_rast, 
                        field = "Value",
                        background = 0, 
                        filename = here("temp", "roads_1m.tif"),
                        overwrite = TRUE)
  } 
  
  
  # Buildings ####
  # Load
  # buildings_path <- glue("{city_path}/buildings/buildings_all.geojson")
  # buildings <- vect(buildings_path, extent = (bb %>% st_transform(4326))) %>% 
  #   st_as_sf() 
  # buildings <- st_read(buildings_path) 
  
  buildings <- sfarrow::st_read_parquet(
    glue("{open_urban_aws_http}/buildings/buildings_{gridcell_id}.parquet"),
    quiet = TRUE) 
  
  ulu <- rast(glue("{city_path}/urban_land_use/urban_land_use_{gridcell_id}.tif"))
  
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
      
      anbh <- rast(glue("{city_path}/anbh/anbh_{gridcell_id}.tif"))
      
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
        mutate(Area_m = as.numeric(st_area(.)),
               ULU = as.factor(ULU),
               ULU = droplevels(replace(ULU, ULU == "0", NA)))
      
      classify_slope <- function(buildings) {
        buildings %>%
          mutate(
            Slope = case_when(
              # Residential -> High slope
              ULU == 2 ~ "high",
              
              # Non-residential: split on area
              ULU == 1 & Area_m < 1034 ~ if_else(ANBH < 11, "high", "low"),
              ULU == 1 & Area_m >= 1034 ~ "low",
              
              # Anything else / missing
              TRUE ~ NA_character_
            )
          )
      }
      
      buildings <- classify_slope(buildings)
      
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
      
      # Overwrite building file in with new data
      st_write_parquet(
        obj = buildings,
        dsn = here("data", city_name, glue("buildings/buildings_{gridcell_id}.parquet"))
      )
      
      write_s3(buildings, glue("{bucket}/OpenUrban/{city_name}/buildings/buildings_{gridcell_id}.parquet"))
      
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
      
      # Overwrite building file in with new data
      st_write_parquet(
        obj = buildings,
        dsn = here("data", city_name, glue("buildings/buildings_{gridcell_id}.parquet"))
      )
      
      write_s3(buildings, glue("{bucket}/OpenUrban/{city_name}/buildings/buildings_{gridcell_id}.parquet"))
    }
    
    # Create raster
    buildings %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here("temp", "buildings_1m.tif"),
                            overwrite = TRUE)
  }
  
  # Parking ####
  # Load
  parking <- sfarrow::st_read_parquet(
    glue("{open_urban_aws_http}/parking/parking_all.parquet"),
    quiet = TRUE) %>% 
    st_filter(bb)
  
  if (nrow(parking) > 0){
    parking <- parking %>% 
      st_transform(utm) %>% 
      mutate(Value = 400) 
    
    # Create raster
    parking %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here("temp", "parking_1m.tif"),
                overwrite = TRUE)
  } 
  
  # Combine rasters ####
  open_space_rast <- tryCatch(
    rast(here("temp", "open_space_1m.tif")),
    error = function(e) NULL
  )
  roads_rast <- tryCatch(
    rast(here("temp", "roads_1m.tif")),
    error = function(e) NULL
  )
  water_rast <- tryCatch(
    rast(here("temp", "water_1m.tif")),
    error = function(e) NULL
  )
  buildings_rast <- tryCatch(
    rast(here("temp", "buildings_1m.tif")),
    error = function(e) NULL
  )
  parking_rast <- tryCatch(
    rast(here("temp", "parking_1m.tif")),
    error = function(e) NULL
  )
  
  LULC <- max(esa_rast,
              open_space_rast,
              roads_rast,
              water_rast,
              buildings_rast,
              parking_rast, na.rm = TRUE)
  
  writeRaster(x = LULC, 
              filename = here("temp", "LULC.tif"),
              overwrite = TRUE,
              datatype = 'INT2U',
              gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512"))
  
  local_file <- here("temp", "LULC.tif")
  
  obj <- gcs_upload_until_success(
    file = local_file,
    bucket = "wri-cities-lulc-gee",
    name = basename(local_file)
  )
  
  gs_uri <- sprintf("gs://%s/%s", gcs_get_global_bucket(), obj$name)
  
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
  
  
  library(here)
  Sys.getenv(c("GOOGLE_APPLICATION_USER","GOOGLE_APPLICATION_CREDENTIALS"))
  
  gee_args <- c(
    "--local-file", local_file,
    "--collection-id", "projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC",
    "--city_name", city_name,
    "--gridcell_id", gridcell_id,
    "--version", version,
    "--overwrite"
  )

  system2(here("./upload_gee.py"), args = gee_args)
          
          
  print("LULC raster saved")
  
}

map(
  grid$ID,
  create_lulc_tile,
  city_name = city_name,
  city_path = city_path
)
