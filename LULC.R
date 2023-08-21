
# Create LULC dataset for city --------------------------------------------

library(here)
library(tidyverse)
library(sf)
library(terra)
library(osmdata)
library(rgee)
# library(reticulate)


# Set up GEE --------------------------------------------------------------

# Set environment
reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley")
ee_Initialize("ejwesley", drive = TRUE)

# City boundary -----------------------------------------------------------

# Load metro area boundaries
# cities <- st_read("data/Smart_Surfaces_metro_areas/smart_surfaces_urban_areas.shp")

# Choose city of interest, use underscores instead of spaces
# city_name <- "Los_Angeles"

# Use LA AOI for testing purposes
city <- st_read("data/AOI.geojson")

# Get city geometry
# city <- cities %>%
#   filter(str_detect(NAME10, city))

# Create bounding box for urban area
# buffered by 804.672 (half a mile in meters)
bb <- city %>% 
  st_buffer(dist = 804.672) %>% 
  st_bbox() 

bb_ee <-  sf_as_ee(st_as_sfc(bb))

# ggplot() +
#   geom_sf(data = bb) +
#   geom_sf(data = city)

########## Need a look up method
# Lookup local state plane projection 
epsg <- 26945


# ESA Worldcover ----------------------------------------------------------

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

## [ND, Green, Green, Green, Green, Built up, Barren, Water, Water, Water, Water, Barren]
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

esa <- esa_img %>% 
  ee_utils_future_value() %>% 
  writeRaster(paste0(path, "/ESA_1m.tif"))

# Open space --------------------------------------------------------------

# get open space from OSM
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

# Combine polygons and multipolygons
open_space <- open_space$osm_polygons %>% 
  bind_rows(st_cast(open_space$osm_multipolygons, "POLYGON")) 

# Reproject to local state plane and add value field (10)
open_space <- open_space %>% 
  st_transform(epsg) %>% 
  mutate(Value = 10)

# Rasterize to match grid of esa and save raster
open_space_rast <- open_space %>% 
  rasterize(esa, 
            field = "Value",
            filename = paste0(path, "/open_space_1m.tif"))


# Roads -------------------------------------------------------------------

## Get data ----------------------------------------------------------------

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
            filename = paste0(path, "/roads_1m.tif"))


# Water -------------------------------------------------------------------

## Get data --------------------------------------------------------------

# get water from OSM
water1 <- opq(bb) %>% 
  add_osm_feature(key = 'water') %>% 
  osmdata_sf() 

water2 <- opq(bb) %>% 
  add_osm_feature(key = 'natural', value = 'water') %>% 
  osmdata_sf() 

water <- c(water1, water2)

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
            filename = paste0(path, "/water_1m.tif"))


# Roofs -------------------------------------------------------------------

## Get data --------------------------------------------------------------

# get water from OSM
buildings <- opq(bb) %>% 
  add_osm_feature(key = 'building') %>% 
  osmdata_sf() 


