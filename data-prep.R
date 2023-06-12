# Data mockup for SSC LULC layer creation---Los Angeles, CA
# updates

library(here)
library(tidyverse)
library(sf)
library(osmdata)

# Urban area boundary
# city <- st_read(here("data", "Smart_Surfaces_urban_areas", "Smart_Surfaces_urban_areas.shp")) %>% 
#   filter(str_detect(NAMELSAD10, "Los Angeles"))

aoi <- st_read(here("data", "LA", "aoi.shp"))

# Create bounding box for urban area
bb <- st_bbox(aoi)

# Extract data from Open Street Maps using the Overpass API
# https://wiki.openstreetmap.org/wiki/Overpass_API 
roads <- opq(bb) %>% 
  add_osm_feature(key = 'highway') %>%
  osmdata_sf() 

roads$osm_lines %>% 
  select(highway) %>% 
  st_write(here("data", "LA", "roads.shp"))

ggplot(roads$osm_lines) + 
  geom_sf(aes(color = highway))

# Road types from Ludwig et al. 2021
road.types <- c("motorway",
                "trunk",
                "primary",
                "secondary",
                "tertiary",
                "residential",
                "unclassified",
                "motorway_link", 
                "trunk_link",
                "primary_link",
                "secondary_link",
                "tertiary_link",
                "living_street")

roads.sub <- opq(bb) %>% 
  add_osm_feature(key = 'highway', value = road.types) %>%
  osmdata_sf() 

ggplot(roads.sub$osm_lines) + 
  geom_sf(aes(color = highway))

parking <- opq(bb) %>% 
  add_osm_feature(key = 'highway') %>%
  osmdata_sf() 

ggplot(test$osm_lines) +
  geom_sf() +
  geom_sf(data = st_as_sfc(bb))
