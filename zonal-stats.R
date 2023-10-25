library(here)
library(tidyverse)
library(sf)
library(rgee)

# Set up GEE --------------------------------------------------------------

# Set environment
# reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley")
ee_Initialize("ejwesley", drive = TRUE, gcs = TRUE)


# Load metro area boundaries
cities <- st_read(here("data", "Smart_Surfaces_metro_areas", "smart_surfaces_urban_areas.shp")) 

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
                "San_Antonio", 2278, "Texas South Central", "78580")

city_names <- aois %>% 
  pull(city) 


# Datasets ----------------------------------------------------------------

# Time period
date_start  <- '2021-01-01'
date_end <- '2022-12-31'

# Summer data range for LST
DATE_RANGE <- ee$Filter$dayOfYear(152, 243)
YEAR_RANGE <-  ee$Filter$calendarRange(2021, 2022,'year')

# Sentinel-2
S2 <- ee$ImageCollection("COPERNICUS/S2_SR")

# Tree cover
TreeCover <- ee$ImageCollection("projects/wri-datalab/cities/tree-cover/TreeCover2020-NonTropicalCitiesAug2023batch")  
TreeCover <- TreeCover$reduce(ee$Reducer$mean())$rename('b1')$multiply(.01)

# Landsat 8
L8 <- ee$ImageCollection('LANDSAT/LC08/C02/T1_L2')$
  select('ST_B10', 'QA_PIXEL')

# # UHI, Urban heat islands from https://yceo.yale.edu/research/global-surface-uhi-explorer
# UHI_yearly_1km <- ee$Image('projects/ee-yceo/assets/UHI_yearly_300_v5')

# LULC
lulc <- ee$ImageCollection("projects/wri-datalab/cities/SSC/LULC")$select('b1')


# Functions ---------------------------------------------------------------

# Sentinel-2 cloud mask
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

# Function to add albedo to Sentinel images
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

# Function to add NDVI to Sentinel images
addNDVI <- function(image){
  ndvi = image$normalizedDifference(c('B8', 'B4'))$rename('NDVI')
  return(image$addBands(ndvi))
}

# CLOUD MASK FUNCTION for Landsat 8
# Create a function to mask clouds and cloud shadows based on the QA_PIXEL band of Landsat 8 & 9
# For information on bit values for the QA_PIXEL band refer to: 
# https://developers.google.com/earth-engine/datasets/catalog/LANDSAT_LC08_C02_T1_L2#bands
cloudMaskL08 <- function(image){
  qa <- image$select('QA_PIXEL')
  
  # Bits 10 and 11 are clouds and cirrus, respectively.
  mask10 <- bitwShiftL(1, 10)
  mask10 <- bitwShiftL(1, 11)
  
  # Both flags should be set to zero, indicating clear conditions.
  mask <- qa$bitwiseAnd(mask10)$eq(0)
  mask2 <- qa$bitwiseAnd(mask10)$eq(0)
  
  image$updateMask(mask)$updateMask(mask2)
}

# Create a funtion using Landsat scale factors for deriving ST in Kelvin and Celsius.
# For more information on ST scale factors, refer to:
# https://www.usgs.gov/landsat-missions/landsat-collection-2-level-2-science-products 
applyScaleFactors <- function(image){
  thermalBands <- image$select('ST_B10')$multiply(0.00341802)$add(149.0)$ # Scale factors for Kelvin
    subtract(273.15) # Scale factor for degrees Celsius
  return(image$addBands(thermalBands))
}

# Function to calculate the sum and count of 
# surface characteristics for a gridcell
griderate <- function(gridcell){
  
  lst <- landsatST$select("ST_B10")$
    mean()$
    # clip(gridcell)$
    reproject(lulcProj)$
    rename('LST')
  
  ndvi <- greenest$
    # clip(gridcell)$
    reproject(lulcProj)$
    rename('NDVI')
  
  pct_tree <- TreeCover$
    # clip(gridcell)$
    reproject(lulcProj)$
    rename('Pct-tree')
  
  alb <- S2alb$mean()$
    # clip(gridcell)$
    reproject(lulcProj)$
    rename('Albedo')
  
  newBands <- ee$Image(list(lst, ndvi, pct_tree, alb))
  
  combo <- newBands$addBands(lulc_city$rename('lulc'))
  
  albCount <- combo$select('Albedo', 'lulc')$
    reduceRegion(
      reducer = ee$Reducer$count()$group(groupField = 1, groupName = 'lulc'),
      geometry = gridcell$geometry(),
      scale = 1,
      maxPixels = 10e10)$
    get('groups')
  
  albSum <- combo$select('Albedo', 'lulc')$
    reduceRegion(
      reducer = ee$Reducer$sum()$group(groupField = 1, groupName = 'lulc'),
      geometry = gridcell$geometry(),
      scale = 1,
      maxPixels = 10e10)$
    get('groups')
  
  treeSum <- combo$select('Pct-tree', 'lulc')$
    reduceRegion(
      reducer = ee$Reducer$sum()$group(groupField = 1, groupName = 'lulc'),
      geometry = gridcell$geometry(),
      scale = 1,
      maxPixels = 10e10)$
    get('groups')
  
  ndviSum <- combo$select('NDVI', 'lulc')$
    reduceRegion(
      reducer = ee$Reducer$sum()$group(groupField = 1, groupName = 'lulc'),
      geometry = gridcell$geometry(),
      scale = 1,
      maxPixels = 10e10)$
    get('groups')
  
  lstSum <- combo$select('LST', 'lulc')$
    reduceRegion(
      reducer = ee$Reducer$sum()$group(groupField = 1, groupName = 'lulc'),
      geometry = gridcell$geometry(),
      scale = 1,
      maxPixels = 10e10)$
    get('groups')
  
  gridcell <- gridcell$set('alb-count', albCount)
  gridcell <- gridcell$set('alb-sum', albSum)
  gridcell <- gridcell$set('tree-sum', treeSum)
  gridcell <- gridcell$set('ndvi-sum', ndviSum)
  gridcell <- gridcell$set('lst-sum', lstSum)
  
  return(gridcell)
}


# Iterate zonal statistics over cities ------------------------------------

# Starting with Boston
# Also need Atlanta 10 and Dallas 14,
# Phoenix 5

for (x in city_names){
  
  city_name <- x
  
  city <- cities %>%
    filter(str_detect(NAME10, city_name))

  city_ee <- sf_as_ee(city)
  
  # Sentinel-2 NDVI & albedo
  S2_PROJ <- S2$filterBounds(city_ee)$first()$select('B4')$projection()

  # Filter S2 by dates and less than 20% cloudy, apply cloud mask
  S2filtered  <- S2$filter(ee$Filter$date(date_start, date_end))$
    filter(ee$Filter$lt('CLOUDY_PIXEL_PERCENTAGE', 20))$
    map(maskS2clouds)$
    filterBounds(city_ee)
  
  # Calculate NDVI and albedo
  S2ndvi <- S2filtered$map(addNDVI)
  S2alb  <- S2filtered$map(add.S2.alb)$
    select('albedo')
  
  # Make a "greenest" pixel composite.
  greenest  <- S2ndvi$qualityMosaic('NDVI')$
    select('NDVI');
  
  
  # LST
  # From: https://code.earthengine.google.com/8f8a363aa18fa9d16c1fe84991aa4154
  # Filter the collections by the CLOUD_COVER property so each image contains less than 20% cloud cover.	
  L8_filtered <- L8$filterBounds(city_ee)$
    filter(DATE_RANGE)$
    filter(YEAR_RANGE)$
    filter(ee$Filter$lt('CLOUD_COVER', 20))$
    map(cloudMaskL08)

  # Define a variable to apply scale factors to the filtered image collection.
  landsatST <- L8_filtered$map(applyScaleFactors)
  
  mean_LandsatST <- landsatST$mean()
  
  # LULC
  from <- c(1, 2, 3, 10, 20, 30, 41, 42, 50)
  to <- c(1, 2, 3, 4, 5, 6, 7, 8, 9)
  
  lulc_city <- lulc$filterBounds(city_ee)$max()$int()
  lulc_city <- lulc_city$remap(from, to)$rename('lulc-v1')
  lulcProj <- lulc_city$projection()

  # Zonal statistics  -------------------------------------------------------

  # Make a grid
  grid <- city_ee$geometry()$coveringGrid(lulcProj, 5000)
  # Intersect grid with city geometry to keep urban boundary
  gridClip <- grid$map(function(f){
    return(f$intersection(city_ee$geometry(), 1))
  });
  # Filter out null geometries
  gridClip <- gridClip$map(function(f) {
    return(f$set(list(points = ee$List(ee$List(ee$Feature(f)$geometry()$coordinates()))$length())))
  })
  gridClip <- gridClip$filterMetadata('points', 'greater_than', 0)

  # Get a list of gridcell IDs to iterate over
  # Number of grid cells
  gridSize <- gridClip$size()$getInfo()
  # Get a list of the grid cells
  gridList <- gridClip$toList(gridSize)
  
  # Unlist the list to manipulate in R
  gridList <- unlist(gridList$getInfo())
  # Create list of IDS
  gridList <- stack(gridList) %>% 
    filter(ind == "id") %>% 
    pull(values)
  # Split the list into sublists of 25 elements for iteration
  subGrids <- split(gridList, ceiling(seq_along(gridList) / 25))
  
  # Boston 1:17 
  # for (i in 1:length(subGrids)){
  for (i in 1:17){
    # name <- paste0("grid", i)
    # assign(name, grid$filter(ee$Filter$inList("system:index", ee$List(subGrids[[i]]))))
    # subList <- append(subList, name)
    
    aoi <- gridClip$filter(ee$Filter$inList("system:index", ee$List(subGrids[[i]])))
    zonalStats <- aoi$map(griderate)
    
    zs_task <- ee_table_to_drive(
      collection = zonalStats,
      description = paste0(city_name, '-zonalStats-', i),
      fileFormat = 'geojson')
    
    zs_task$start()
    
    print(paste0(city_name, '-zonalStats-', i))
  }
}  
  

