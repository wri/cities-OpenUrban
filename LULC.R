
# Create LULC dataset for city --------------------------------------------

library(here)
library(tidyverse)
library(sf)
library(osmdata)
library(rgee)
# library(reticulate)


# Set up GEE --------------------------------------------------------------


ee_install(py_env = "rgee")
# Initialize Earth Engine and GD
ee_Initialize(user = 'ejwesley@gmail.com', drive = TRUE)

ee_Initialize()
ee_Authenticate()

srtm <- ee$Image("USGS/SRTMGL1_003")


# City boundary -----------------------------------------------------------

# Load metro area boundaries
cities <- st_read("data/Smart_Surfaces_metro_areas/smart_surfaces_urban_areas.shp")

# Choose city of interest, use underscores instead of spaces
city <- "New_Orleans"

# Use LA AOI for testing purposes
city <- st_read("data/AOI.geojson")

# Get city geometry
city <- cities %>%
  filter(str_detect(NAME10, city))

# Create bounding box for urban area
# buffered by 804.672 (half a mile in meters)
bb <- city %>% 
  st_buffer(dist = 804.672) %>% 
  st_bbox() %>% 
  st_as_sfc()

# ggplot() +
#   geom_sf(data = bb) +
#   geom_sf(data = city)

########## Need a look up method
# Lookup local state plane projection 
epsg <- 26945


# ESA Worldcover ----------------------------------------------------------

# Read ESA land cover (2021)
esa <- ee_ImageCollection('ESA/WorldCover/v200')

# Clip to city of interest
esa_city <- esa$filterBounds(bb_geom)

# ImageCollection to image
esa_city <- esa_city$first()

# Reclassify raster to combine greenspace types

## [ND, Tree, Shrubland, Grassland, Cropland, Built up, Bare/sparse vegetation,
##  Snow/ice, Permanent water bodies, Herbaceous wetland, Mangroves, Moss and lichen]
FROM = [0, 10, 20, 30, 40, 50, 60, 80, 70, 90, 95, 100]

## [ND, Green, Green, Green, Green, Built up, Barren, Water, Water, Water, Water, Barren]
TO = [0, 1, 1, 1, 1, 2, 3, 4, 4, 4, 4, 3]

esa_city_rm = esa_city$remap(FROM, TO)


