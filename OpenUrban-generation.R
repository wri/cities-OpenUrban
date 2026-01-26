library(terra)
library(sf)
library(here)
library(tidyverse)
library(glue)
library(geoarrow)
library(sfarrow)



# S3 setup ----------------------------------------------------------------


# 1) Log in via AWS SSO for that profile (opens browser)
# system("aws sso login --profile cities-data-dev")
# 
# # 2) Point this R/reticulate session at that profile & region
# Sys.setenv(
#   AWS_SDK_LOAD_CONFIG = "1",      # make boto3 read ~/.aws/config
#   AWS_PROFILE = "cities-data-dev",
#   AWS_DEFAULT_REGION = "us-east-1"
# )

s3 <- paws::s3()
bucket <- "wri-cities-heat"
aws_http <- "https://wri-cities-heat.s3.us-east-1.amazonaws.com"

# Load write_s3
source(here("utils", "write-s3.R"))

####
city <- "NLD-Rotterdam"
city_path <- here("data", city)
if (!dir.exists(city_path)) {dir.create(city_path)}

open_urban_aws_http <- glue("{aws_http}/OpenUrban/{city}")

extent <- st_read("https://wri-cities-indicators.s3.us-east-1.amazonaws.com/data/published/layers/UrbanExtents/geojson/NLD-Rotterdam__urban_extent__UrbanExtents__StartYear_2020_EndYear_2020.geojson")
if (!dir.exists(here(city_path, "boundaries"))) {dir.create(here(city_path, "boundaries"))}
boundary_path <- here(city_path, "boundaries", "city_polygon.geojson")
if (!file.exists(boundary_path)) {st_write(extent, boundary_path)}

# Get data

# Define your parameters
env_name <- "open-urban"
script_path <- "get_data.py"
script_args <- city

# Combine 'run', the environment name, 'python', the script, and its arguments
args <- c("run", "-n", env_name, "python", script_path, script_args)

# Execute via system2
response <- system2(
  command = "/home/ubuntu/miniconda3/condabin/conda", 
  args = args, 
  stdout = TRUE,   
  stderr = TRUE    
)

# Print output
print(response)

# Load the grid
grid <- st_read(glue("{open_urban_aws_http}/city_grid/city_grid.geojson"))

create_lulc_tile <- function(gridcell_id, city, city_path){
  
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
  open_space <- sfarrow::st_read_parquet(
      glue("{open_urban_aws_http}/open_space/open_space_all.parquet"),
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
                filename = here("tmp", "open_space_1m.tif"),
                overwrite = TRUE)
  } 
  
  
  # Water ####
  # Load
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
                filename = here("tmp", "water_1m.tif"),
                overwrite = TRUE)
  } 
  
  
  
  # Roads ####
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
                        filename = here("tmp", "roads_1m.tif"),
                        overwrite = TRUE)
  } 
  
  
  # Buildings ####
  # Load
  buildings <- sfarrow::st_read_parquet(
    glue("{open_urban_aws_http}/buildings/buildings_{gridcell_id}.parquet"),
    quiet = TRUE) %>% 
    select(id, version)
  
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
    if (str_detect(city, "USA")){
      
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
      
      # Reproject to UTM and calculate area
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
        dsn = here("data", city, glue("buildings/buildings_{gridcell_id}.parquet"))
      )
      
      write_s3(buildings, glue("{bucket}/OpenUrban/{city}/buildings/buildings_{gridcell_id}.parquet"))
      
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
        dsn = here("data", city, glue("buildings/buildings_{gridcell_id}.parquet"))
      )
      
      write_s3(buildings, glue("{bucket}/OpenUrban/{city}/buildings/buildings_{gridcell_id}.parquet"))
    }
    
    # Create raster
    buildings %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here("tmp", "buildings_1m.tif"),
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
                filename = here("tmp", "parking_1m.tif"),
                overwrite = TRUE)
  } 
  
  # Combine rasters ####
  open_space_rast <- tryCatch(
    rast(here("tmp", "open_space_1m.tif")),
    error = function(e) NULL
  )
  roads_rast <- tryCatch(
    rast(here("tmp", "roads_1m.tif")),
    error = function(e) NULL
  )
  water_rast <- tryCatch(
    rast(here("tmp", "water_1m.tif")),
    error = function(e) NULL
  )
  buildings_rast <- tryCatch(
    rast(here("tmp", "buildings_1m.tif")),
    error = function(e) NULL
  )
  parking_rast <- tryCatch(
    rast(here("tmp", "parking_1m.tif")),
    error = function(e) NULL
  )
  
  LULC <- max(esa_rast,
              open_space_rast,
              roads_rast,
              water_rast,
              buildings_rast,
              parking_rast, na.rm = TRUE)
  
  # Save local tmp file
  writeRaster(x = LULC, 
              filename = here("tmp", "LULC.tif"),
              overwrite = TRUE,
              datatype = 'INT2U',
              gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512"))
  
  local_file <- here("tmp", "LULC.tif")
  
  # Save to GEE
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
  
  args <- c("run", "-n", "open-urban", "python", script_path, gee_args)
  
  response <- system2(conda, args, stdout = TRUE, stderr = TRUE)
  cat(response, sep = "\n")
  
  # Save to s3
  write_s3(LULC, glue("wri-cities-heat/OpenUrban/{city}/OpenUrban/{city}_{gridcell_id}.tif"))
  
  print(glue("LULC raster {gridcell_id} of {nrow(grid)} saved"))
  
}

map(
  grid$ID,
  create_lulc_tile,
  city = city,
  city_path = city_path
)
