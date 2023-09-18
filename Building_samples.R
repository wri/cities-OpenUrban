# Building classification & model building
# Stratified random sample
# include: all 10 cities, both residential and non-residential areas, areas
# with tall buildings and short buildings, small and large buildings

library(here)
library(tidyverse)
library(sf)
library(terra)
library(osmdata)
library(rgee)
library(googledrive)

# Set up GEE --------------------------------------------------------------

# Set environment
reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley")
ee_Initialize("ejwesley", drive = TRUE, gcs = TRUE)

# Load metro area boundaries
cities <- st_read(here("data", "Smart_Surfaces_metro_areas", "smart_surfaces_urban_areas.shp")) 

# SSC cities
aois <- tribble(~ city, ~ epsg, ~ zone, ~geoid, 
                "New_Orleans", 3452, "Louisiana South", "62677",
                "Dallas", 2276, "Texas North Central", "22042", 
                "Columbia", 2273, "South Carolina", "18964", 
                "Atlanta", 2240, "Georgia West", "03817", 
                "Boston", 2249, "Massachusetts Mainland", "09271", 
                "Phoenix", 2223, "Arizona Central", "69184", 
                "Portland", 2269, "Oregon North", "71317",
                "Charlotte", 2264, "North Carolina", "15670", 
                "Jacksonville", 2236, "Florida East", "42346",
                "San Antonio", 2278, "Texas South Central", "78580")

# Filter to only SSC cities
cities <- cities %>% 
  filter(GEOID10 %in% aois$geoid)

buildings.samp <- tibble()

# Sample per city
for (city in cities){
  
  city_bb <- city %>% 
    st_buffer(dist = 804.672) %>% 
    st_bbox() 
  
  # Get buildings from OSM & take a random sample of 1000
  buildings <- opq(city_bb) %>% 
    add_osm_feature(key = 'building') %>% 
    osmdata_sf() 
  
  buildings <- buildings$osm_polygons %>% 
    bind_rows(st_cast(buildings$osm_multipolygons, "POLYGON")) 
  
  # drop excess fields
  buildings <- buildings %>% 
    select(osm_id) %>% 
    slice_sample(n = 1000) %>% 
    mutate(city = city$GEOID10)
  
  buildings.samp <- bind_rows(buildings.samp, buildings)
}

# Create bounding box for urban area
# buffered by 804.672 (half a mile in meters)
# divide into grid for processing
city_bb <- city %>% 
  st_buffer(dist = 804.672) %>% 
  st_bbox() 
