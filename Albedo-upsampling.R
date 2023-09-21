# README ------------------------------------------------------------------

# This code is to upsample albedo from S2 using NAIP imagery
# Upsampled albedo is saved as a GEE asset


# Libraries ---------------------------------------------------------------

library(here)
library(tidyverse)
library(sf)
library(terra)
library(rgee)

# Set up GEE --------------------------------------------------------------

# Set environment
reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley")
ee_Initialize("ejwesley", drive = TRUE, gcs = TRUE)


# Load metro area boundaries
cities <- st_read(here("data", "Smart_Surfaces_metro_areas", "smart_surfaces_urban_areas.shp")) 

city_name <- "New_Orleans"
# Get city geometry
city <- cities %>%
  filter(str_detect(NAME10, city_name))

# Create bounding box for urban area
# buffered by 804.672 (half a mile in meters)
# divide into grid for processing
city_bb <- city %>% 
  st_buffer(dist = 804.672) %>% 
  st_bbox() 

city_grid <- city_bb %>% 
  st_make_grid(cellsize = c(0.1, 0.1), what = "polygons") %>% 
  st_sf() %>% 
  st_filter(city) %>% 
  mutate(ID = row_number())

# start and end dates
# NAIP coverage is 2021-2023
start <- '2021-01-01'
end <- '2023-09-20'

# Cloud mask function
#### Have someone check this ####
maskS2clouds <- function(image){
  qa <- image$select('QA60')
  
  # Bits 10 and 11 are clouds and cirrus, respectively.
  cloudBitMask <- bitwShiftL(1, 10)
  cirrusBitMask <- bitwShiftL(1, 11)
  
  # Both flags should be set to zero, indicating clear conditions.
  mask <- qa$bitwiseAnd(cloudBitMask)$eq(0)
  mask2 <- qa$bitwiseAnd(cirrusBitMask)$eq(0)
  
  image$updateMask(mask)$updateMask(mask2)$divide(10000)
}

# Calculate S2 albedo using Bonafoni coefficients

add.S2.alb <- function(image){
  
  albedocalc <- image$expression(
    expression = '((B*Bw)+(G*Gw)+(R*Rw)+(NIR*NIRw)+(SWIR1*SWIR1w)+(SWIR2*SWIR2w))',
    opt_map = list(
      'B' = image$select('B2'),
      'G' = image$select('B3'),
      'R' = image$select('B4'),
      'NIR' = image$select('B8'),
      'SWIR1' = image$select('B11'),
      'SWIR2' = image$select('B12'),
      'Bw' = 0.2266,
      'Gw' = 0.1236,
      'Rw' = 0.1573,
      'NIRw' = 0.3417,
      'SWIR1w' = 0.1170,
      'SWIR2w' = 0.0338)
    )
  
  return(image$addBands(albedocalc$rename('albedo'))) 
  
}

# Calculate NAIP "albedo"
add.NAIP.alb <- function(image){
  
  BGRN_TOTAL <- 0.8492
  
  albCalc <- image$expression(
    expression = '((B*Bw)+(G*Gw)+(R*Rw)+(N*Nw))',
    opt_map =  list(
      'B' = image$select('B')$divide(255),
      'G' = image$select('G')$divide(255),
      'R' = image$select('R')$divide(255),
      'N' = image$select('N')$divide(255),
      'Bw' = 0.2266/BGRN_TOTAL,
      'Gw' = 0.1236/BGRN_TOTAL,
      'Rw' = 0.1573/BGRN_TOTAL,
      'Nw' = 0.3417/BGRN_TOTAL
      )
    
    )
  
  return(image$addBands(albCalc$rename('albedo')))
  
}

# Iterate over city grid
for (i in 1:length(city_grid$ID)){
  # Buffer grid cell so there will be overlap
  aoi <- city_grid %>% 
    filter(ID == i) %>% 
    st_as_sf() %>% 
    st_buffer(dist = 1000) 
 
  bb <- st_bbox(aoi)
  
  bb_ee <- sf_as_ee(st_as_sfc(bb))
  
  ggplot() +
    geom_sf(data = city) +
    geom_sf(data = aoi, fill = NA)
  
  # S2 albedo ---------------------------------------------------------------

  # Read S2 data
  s2 <- ee$ImageCollection("COPERNICUS/S2_SR")
  
  # Filter to city of interest
  s2_city <- s2$filterBounds(bb_ee)$
    filter(ee$Filter$date(start, end))$
    filter(ee$Filter$lt('CLOUDY_PIXEL_PERCENTAGE', 20))$
    map(maskS2clouds)
  
  Map$addLayer(s2_city$first()$select('B2'), list(min = 0, max = 0.1))
  
  # Add albedo to S2 images 
  s2_city_alb <- s2_city$map(add.S2.alb)
  Map$addLayer(s2_city_alb$first()$select('albedo'), list(min = 0, max = 1))
}