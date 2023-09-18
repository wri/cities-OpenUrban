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

build.samp <- tibble()

# Sample per city
for (i in 1:nrow(cities)){
  
  city <- cities %>% 
    slice(i)
  
  city_bb <- city %>% 
    st_bbox() 
  
  # create 10x10 grid over city
  city_grid <- city_bb %>% 
    st_make_grid(n = 10, what = "polygons") %>% 
    st_sf() %>% 
    st_filter(city) %>% 
    mutate(ID = row_number())
  
  city_samp <- tibble()
  
  for (x in 1:length(city_grid$ID)){
    # Buffer grid cell so there will be overlap
    aoi <- city_grid %>% 
      filter(ID == x) %>% 
      st_as_sf() 
    
    bb <- st_bbox(aoi)
    
    # Get buildings from OSM & take a random sample of 10 buildings from each
    # grid cell for a total of 1000 per city
    buildings <- opq(bb) %>% 
      add_osm_feature(key = 'building') %>% 
      osmdata_sf() 
    
    if (!is.null(nrow(buildings$osm_polygons)) & !is.null(nrow(buildings$osm_multipolygons))){
      # if there are polygons and multipolygons
      buildings <- buildings$osm_polygons %>% 
        bind_rows(st_cast(buildings$osm_multipolygons, "POLYGON")) 
      
    } else if (!is.null(nrow(buildings$osm_polygons)) & is.null(nrow(buildings$osm_multipolygons))){
      # if there are polygons but no multipolygons
      
      buildings <- buildings$osm_polygons
      
    } else if (is.null(nrow(buildings$osm_polygons)) & !is.null(nrow(buildings$osm_multipolygons))){
      # if there are multipolygons but no polygons
      
      buildings <- st_cast(buildings$osm_multipolygons, "POLYGON")
      
    } else {
      # if there are neither
      print(paste0("Skipped ", city$NAME10, " grid ", i))
      next
    }
    
    # drop excess fields
    buildings <- buildings %>% 
      select(osm_id) %>% 
      mutate(city = city$GEOID10)
    
    # Sample 10% of buildings. If the area has so few buildings that 10% < 1, 
    # sample 1 building.
    if(nrow(buildings) * 0.1 < 1){
      
      buildings2 <- buildings %>% 
        slice_sample(n = 1)
      
    } else {
      
      buildings2 <- buildings %>% 
        slice_sample(prop = 0.1)
      
    }

    city.samp <- rbind(city.samp, buildings)
    print(paste0(city$NAME10, " grid ", i))
    
  }
  
  build.samp <- rbind(city.samp, build.samp)
  
  
}

walk(cities, samp)

