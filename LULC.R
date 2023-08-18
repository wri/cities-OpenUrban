
# Create LULC dataset for city --------------------------------------------

library(here)
library(tidyverse)
library(sf)
library(osmdata)
library(rgee)
# library(reticulate)


# Set up GEE --------------------------------------------------------------

# Set environment
reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD

# City boundary -----------------------------------------------------------

# Load metro area boundaries
# cities <- st_read("data/Smart_Surfaces_metro_areas/smart_surfaces_urban_areas.shp")

# Choose city of interest, use underscores instead of spaces
# city <- "New_Orleans"

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

# Resample raster from 10-m to 1-m
esa_1m = esa_city.reproject(crs = "EPSG:"+proj, scale = 1)

# Export to Google Drive
geemap.ee_export_image_to_drive(
  esa_1m.toByte(), # use toByte() to reduce file size 
  scale = 1, # 10 for native resolution, 50 for smaller file size 
  description = 'ESA_1m',
  region = bb_geom,
  maxPixels = 5000000000
)

# Create city folder
out_dir = os.getcwd()
city_folder = out_dir + "/data/" + city
os.makedirs(city_folder)

################
## Download from Google Drive to city folder
################

################
## Need help with this loop, create variables for unknown # of tifs
# Read local raster

img_list = glob.glob(city_folder + '/*ESA_1m*')
# print(img_list)

esa_1 = rasterio.open(img_list[0])
# esa_2 = rasterio.open(img_list[1])

print(esa_1.crs)


# Open space --------------------------------------------------------------

# get roads from OSM
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

ggplot() +
  geom_sf(data = st_as_sfc(bb)) +
  geom_sf(data = open_space4)
