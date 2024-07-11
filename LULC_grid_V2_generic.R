
# Create LULC dataset for city --------------------------------------------

library(terra)
library(osmdata)
library(rgee)
library(gfcanalysis)
library(googledrive)
library(sf)
library(here)
library(tidyverse)



# Set up GEE --------------------------------------------------------------

# Set environment
# reticulate::use_condaenv("rgee")

# Initialize Earth Engine and GD
# ee_Authenticate("ejwesley", drive = TRUE, gcs = TRUE)
ee_Initialize("ejwesley", drive = TRUE, gcs = TRUE)

# LULC raster creation function -------------------------------------------

# city = spatial data for city of interest
# city_name = string with underscores, this will be used for naming files

city_file <- here("testing", "road-width", "Washington-DC", 
                          "Washington_DC_Boundary", "Washington_DC_Boundary.shp")
city <- "Cancun"
country_code <- "USA"

create_LULC <- function(city, city_file, country_code){

  # City boundary -----------------------------------------------------------
  
  city_name <- paste0(country_code, "-", city)
  
  city_geom <- st_read(city_file)
  
  # transform data to UTM
  city_centroid <- city_geom %>% 
    st_centroid() %>% 
    st_coordinates()
  
  utm <- utm_zone(y = city_centroid[2], x = city_centroid[1], proj4string = TRUE)
  
  utm_ee <- str_split(utm, "=", simplify = TRUE)[2] %>% 
    str_to_upper()
  
  utm <- str_split(utm_ee, ":", simplify = TRUE)[2] %>% 
    as.numeric() %>% 
    st_crs()
    
  
  # Path to temp directory
  path <- "~/Desktop/LULC"

  # Create bounding box for urban area
  # buffered by 804.672 (half a mile in meters)
  # divide into grid for processing
  city_bb <- city_geom %>% 
    st_buffer(dist = 804.672) %>% 
    st_bbox() 
  
  # Create grid for LULC tiles
  city_grid <- city_geom %>% 
    st_make_grid(cellsize = c(0.15, 0.15), what = "polygons") %>% 
    st_sf() %>% 
    st_filter(city_geom) %>% 
    mutate(ID = row_number())
  
  ggplot() +
    geom_sf(data = city_geom) +
    geom_sf(data = city_grid, fill = NA)

  
  # Road download & avg lane calculation ------------------------------------
  
  
  # Download roads and calculate mean lanes per highway class for the city
  road_path <- paste0(path, "/roads")
  
  if (!dir.exists(road_path)){
    dir.create(road_path)
  } else{
    print("dir exists")
  }

  for (i in 1:length(city_grid$ID)){
    # Buffer grid cell so there will be overlap
    aoi <- city_grid %>%
      filter(ID == i) %>%
      st_as_sf()

    bb <- st_bbox(aoi)

    # Values from https://taginfo.openstreetmap.org/keys/highway#values
    # Chosen based on description, excluding footways, etc.
    # Values w/o descriptions not included

    # get road lanes data from OSM for gridcell
    roads_grid <- opq(bb, timeout = 100) %>%
      add_osm_feature(key = 'highway',
                      value = c("residential",
                                "service",
                                "unclassified",
                                "tertiary",
                                "secondary",
                                "primary",
                                "turning_circle",
                                "living_street",
                                "trunk",
                                "motorway",
                                "motorway_link",
                                "trunk_link",
                                "primary_link",
                                "secondary_link",
                                "tertiary_link",
                                "motorway_junction",
                                "turning_loop",
                                "road",
                                "mini_roundabout",
                                "passing_place",
                                "busway")) %>%
      osmdata_sf() %>%
      # cast polygons as lines
      osm_poly2line() %>%
      # retain only unique geometries (no overlap in hierarchies)
      unique_osmdata()
    
    # if (!is.null(nrow(roads_grid$osm_lines))){
    #   # if there are polygons & multipolygons
    #   roads_grid <- roads_grid$osm_lines 
    #   
    # } else {
    #   # if there are neither keep empty geometry
    #   next
    # }
    if(is.null(nrow(roads_grid$osm_lines))){
      next
    }

    # select lines geometries
    roads_grid <- roads_grid$osm_lines %>%
      dplyr::select(any_of(c("highway", "lanes"))) %>%
      # an inline anonymous function to add the needed columns
      (function(.df){
        cls <- c("lanes") # columns I need
        # adding cls columns with NAs if not present in the piped data.frame
        .df[cls[!(cls %in% colnames(.df))]] = NA
        return(.df)
      }) %>%
      mutate(lanes = as.numeric(lanes))

    # Save roads vectors to use later
    roads_grid_file <- paste0(road_path, "/roads_", i, ".geojson")
    st_write(obj = roads_grid,
             dsn = roads_grid_file,
             append = FALSE,
             delete_dsn = TRUE)

  }
  
  road_lanes <- list.files(here(road_path), full.names = TRUE) %>%
    map(\(x) st_read(x) %>%
          mutate(lanes = as.numeric(lanes))) %>%
    reduce(bind_rows) %>%
    st_drop_geometry()
  
  # Get the avg number of lanes per highway class
  lanes <- road_lanes %>% 
    group_by(highway) %>% 
    summarize(avg.lanes = ceiling(mean(lanes, na.rm = TRUE))) %>% 
    mutate(avg.lanes = case_when(is.na(avg.lanes) ~ 2,
                                 !is.na(avg.lanes) ~ avg.lanes))
  

  # Iterate over grid -------------------------------------------------------

  
  for (i in 1:length(city_grid$ID)){
    
    print(paste0(city_name, " grid ", i))
    
    # Buffer grid cell by 10-m so there will be overlap
    aoi <- city_grid %>% 
      filter(ID == i) %>% 
      st_as_sf() %>% 
      st_buffer(dist = 10) 
    
    bb <- st_bbox(aoi)
    
    bb_ee <- sf_as_ee(st_as_sfc(bb))
    
    # ESA Worldcover ----------------------------------------------------------
    
    # Read ESA land cover (2021)
    esa <- ee$ImageCollection('ESA/WorldCover/v200')
    
    # Clip to city of interest
    esa_city <- esa$filterBounds(bb_ee)
    
    # ImageCollection to image
    esa_city <- esa_city$mosaic()
    
    # Reclassify raster to combine greenspace types
    
    ## [ND, Tree, Shrubland, Grassland, Cropland, Built up, Bare/sparse vegetation,
    ##  Snow/ice, Permanent water bodies, Herbaceous wetland, Mangroves, Moss and lichen]
    FROM <- c(0, 10, 20, 30, 40, 50, 60, 80, 70, 90, 95, 100)
    
    # ND: 0 
    # Green: 1 (Tree, Shrubland, Grassland, Cropland)
    # Built up: 2 (Built up)
    # Barren: 3 (Bare/sparse vegetation, Moss and lichen)
    # Water: 20 (Snow/ice, Permanent water bodies, Herbaceous wetland, Mangroves)
    # water code matches eventual landuse code because the category matches
    TO <- c(0, 1, 1, 1, 1, 2, 3, 20, 20, 20, 20, 3)
    
    esa_city <- esa_city$
      remap(FROM, TO)
    
    esa_img <- ee_image_to_drive(esa_city,
                               description = "ESA_1m",
                               timePrefix = FALSE,
                               region = bb_ee,
                               scale = 1,
                               crs = utm_ee,
                               maxPixels = 10e10)
    
    esa_img$start()
    
    # Move from drive to local
    ee_drive_to_local(task = ee_monitoring(esa_img, max_attempts = 1000), 
                      dsn = here(path, "ESA_1m.tif"))
    
    # drive_download("rgee_backup/ESA_1m.tif",
    #                path = here(path, "ESA_1m.tif"))
    
    # Delete from drive
    drive_rm("rgee_backup/ESA_1m.tif")
    
    print("ESA raster saved")
    
    esa_rast <- rast(here(path, "ESA_1m.tif"))
    
    # Open space --------------------------------------------------------------
    
    
    ## Get OSM open space data -------------------------------------------------
    
    open_space1 <- opq(bb) %>% 
        add_osm_feature(key = 'leisure',
                        value = c("pitch",
                                  "park",
                                  "garden",
                                  "playground",
                                  "nature_reserve",
                                  "golf_course",
                                  "common",
                                  "dog_park",
                                  "recreation_ground",
                                  "disc_golf_course")) %>% 
        osmdata_sf()

    print("open space 1")
    
    open_space2 <- opq(bb) %>% 
        add_osm_feature(key = 'boundary',
                        value = c('protected_area',
                                  'national_park',
                                  'forest_compartment',
                                  'forest')) %>% 
        osmdata_sf()
    
    print("open space 2")
    
    open_space <- c(open_space1, open_space2)
    
    rm(open_space1)
    rm(open_space2)
    
    ## Tidy --------------------------------------------------------------------
    
    if (!is.null(nrow(open_space$osm_polygons)) & !is.null(nrow(open_space$osm_multipolygons))){
      # if there are polygons & multipolygons
      open_space <- open_space$osm_polygons %>% 
        bind_rows(st_cast(open_space$osm_multipolygons, "POLYGON")) 
      
    } else if (!is.null(nrow(open_space$osm_polygons)) & is.null(nrow(open_space$osm_multipolygons))){
      # if there are polygons but no multipolygons
      open_space <- open_space$osm_polygons
      
    } else if (is.null(nrow(open_space$osm_polygons)) & !is.null(nrow(open_space$osm_multipolygons))){
      # if there are multipolygons but no polygons
      
      open_space <- st_cast(open_space$osm_multipolygons, "POLYGON")
      
    } else {
      # if there are neither keep empty geometry
      open_space <- open_space$osm_polygons
    }
    
    # Add value = 10 as an attribute if there is open space data, 
    # otherwise use the bounding box with value = 0
    if (!is.null(open_space)){
      open_space <- open_space %>% 
        st_transform(utm) %>% 
        mutate(Value = 10) %>% 
        select(Value)
    } else {
      open_space <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(utm) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    print("open space combined polygons")
    
    open_space %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here(path, "open_space_1m.tif"),
                overwrite = TRUE)
    
    rm(open_space)
    
    print("Open space raster saved")
    
    # Water -------------------------------------------------------------------
    
    ## Get OSM water data --------------------------------------------------------------
    
    # get water from OSM
    water1 <- opq(bb) %>% 
      add_osm_feature(key = 'water') %>% 
      osmdata_sf()

    print("water osm")
    
    water2 <- opq(bb) %>% 
      add_osm_feature(key = 'natural', 
                      value = c('water')) %>% 
      osmdata_sf()

    print("water 2 osm")
    
    water3 <- opq(bb) %>% 
      add_osm_feature(key = 'waterway') %>% 
      osmdata_sf()
   
    print("water 3 osm")
    
    water <- c(water1, water2, water3)
    
    rm(water1)
    rm(water2)
    
    # if there are polygons but no multipolygons
    if (!is.null(nrow(water$osm_polygons)) & !is.null(nrow(water$osm_multipolygons))){
      # if there are polygons & multipolygons
      water <- water$osm_polygons %>% 
        bind_rows(st_cast(water$osm_multipolygons, "POLYGON")) 
      
    } else if (!is.null(nrow(water$osm_polygons)) & is.null(nrow(water$osm_multipolygons))){
      water <- water$osm_polygons
      
    } else if (is.null(nrow(water$osm_polygons)) & !is.null(nrow(water$osm_multipolygons))){
      # if there are multipolygons but no polygons
      water <- st_cast(water$osm_multipolygons, "POLYGON")
      
    } else {
      # if there are neither keep empty geometries
      water <- water$osm_polygons
      
    }
    
    if (!is.null(water)){
      water <- water %>% 
        st_transform(utm) %>% 
        mutate(Value = 20) %>% 
        select(Value)
    } else {
      water <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(utm) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    print("water combined polygons")
    
    water %>% 
      rasterize(esa_rast, 
                field = "Value",
                background = 0,
                filename = here(path, "water_1m.tif"),
                overwrite = TRUE)
    
    rm(water)
    
    print("water raster saved")
    
    
    
    # Roads -------------------------------------------------------------------
    
    
    ## Load roads data ----------------------------------------------------------------
    
    roads_grid_file <- paste0(road_path, "/roads_", i, ".geojson")
    
    roads <- st_read(roads_grid_file)
    
    ## Tidy --------------------------------------------------------------------
    
    
    # Fill lanes with avg lane value when missing
    roads <- roads %>% 
      mutate(lanes = as.numeric(lanes)) %>% 
      left_join(lanes, by = "highway") %>% 
      mutate(lanes = coalesce(lanes, avg.lanes))
    
    # Add value field (20)
    roads <- roads %>% 
      mutate(Value = 30)
    
    # Reproject to local state plane 
    roads <- roads %>% 
      st_transform(utm) 
    
    print("roads reproject")
    
    
    ## Buffer ------------------------------------------------------------------
    
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
                joinStyle = "MITRE") %>% 
      select(Value)

    print("roads buffered")
    
    ## Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    roads %>% rasterize(esa_rast, 
                        field = "Value",
                        background = 0, 
                        filename = here(path, "roads_1m.tif"),
                        overwrite = TRUE)
    
    rm(roads)
    
    print("Roads raster saved")

    
    # Buildings -------------------------------------------------------------------
  
    
    ## Get OSM buildings data --------------------------------------------------------------
    
    # get buildings from OSM
    buildings <- opq(bb) %>% 
      add_osm_feature(key = 'building') %>% 
      osmdata_sf()
    
    print("buildings OSM")
   
    # if there are polygons & multipolygons
    if (!is.null(nrow(buildings$osm_polygons)) & !is.null(nrow(buildings$osm_multipolygons))){
      buildings <- buildings$osm_polygons %>% 
        bind_rows(st_cast(buildings$osm_multipolygons, "POLYGON")) 
    
    # if there are polygons but no multipolygons
    } else if (!is.null(nrow(buildings$osm_polygons)) & is.null(nrow(buildings$osm_multipolygons))){
      buildings <- buildings$osm_polygons 
    
    # if there are multipolygons but no polygons
    } else if (is.null(nrow(buildings$osm_polygons)) & !is.null(nrow(buildings$osm_multipolygons))){
      buildings <- st_cast(buildings$osm_multipolygons, "POLYGON")
    
    # if there are no buildings use empty vector dataset
    # else {buildings <- esa_rast * 0}
    } else {
      buildings <- buildings$osm_polygons
    }
    
    if (is.null(buildings)){
      buildings <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        mutate(osm_id = NULL)
    }
      
    # drop excess fields
    buildings <- buildings %>% 
      select(osm_id)
    
    # Get buildings from Google-Microsoft Open Buildings Dataset
    # Save to GCS
    build_country <- paste0("projects/sat-io/open-datasets/VIDA_COMBINED/", country_code)
    
    openBuilds <- ee$FeatureCollection(build_country)$
      filterBounds(bb_ee) %>% 
      ee_table_to_drive(description = "openBuilds",
                      fileFormat = "GeoJSON",
                      timePrefix = FALSE)
    
    openBuilds$start()
    
    # Move from drive to local
    ee_drive_to_local(task = ee_monitoring(openBuilds, max_attempts = 1000), 
                      dsn = here(path, "openBuilds.geojson"))
    
    # Delete from drive
    drive_rm("rgee_backup/openBuilds.geojson")
    
    print("Open buildings saved")
    
    openBuilds <- geojsonsf::geojson_sf(here(path, "openBuilds.geojson"))
    
    buildings <- buildings[which(st_is_valid(buildings)), ]
    openBuilds <- openBuilds[which(st_is_valid(openBuilds)), ]
    
    # Intersect buildings and keep the open buildings that don't intersect OSM buildings
    openBuilds <- openBuilds %>% 
      mutate(intersects = lengths(st_intersects(., buildings))) %>% 
      filter(intersects == 0) %>% 
      select(geometry)
    
    buildings <- buildings %>% 
      bind_rows(openBuilds) 
    
    # Get rid of any 3d geometries that cause a problem
    # there was one error in Dallas grid 32
    buildings <- buildings[which(st_dimension(buildings) == 2), ]
    
    # Get rid of geometry collections
    buildings <- buildings[which(st_is(buildings, c("POLYGON","MULTIPOLYGON"))), ]

    # Add value field and transform to utm
    buildings <- buildings %>% 
      mutate(Value = 40) %>% 
      st_transform(utm)
    
    ## Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    buildings %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here(path, "buildings_1m.tif"),
                            overwrite = TRUE)
    
    rm(buildings)
    
    print("Buildings raster saved")

    
    # Parking -----------------------------------------------------------------
    
    
    # get parking from OSM
    parking1 <- opq(bb) %>% 
      add_osm_feature(key = 'amenity',
                      value = 'parking') %>% 
      osmdata_sf() 
    
    print("parking osm")
    
    parking2 <- opq(bb) %>% 
      add_osm_feature(key = 'parking') %>% 
      osmdata_sf() 
    
    print("parking osm")
    
    parking <- c(parking1, parking2)

    
    # if there are polygons but no multipolygons
    if (!is.null(nrow(parking$osm_polygons)) & !is.null(nrow(parking$osm_multipolygons))){
      # if there are polygons & multipolygons
      parking <- parking$osm_polygons %>% 
        bind_rows(st_cast(parking$osm_multipolygons, "POLYGON"))
      
    } else if (!is.null(nrow(parking$osm_polygons)) & is.null(nrow(parking$osm_multipolygons))){
      
      parking <- parking$osm_polygons
      
    } else if (is.null(nrow(parking$osm_polygons)) & !is.null(nrow(parking$osm_multipolygons))){
      # if there are multipolygons but no polygons
      
      parking <- st_cast(parking$osm_multipolygons, "POLYGON")
      
    } else {
      # if there are neither create raster of 0s
      
      parking <- parking$osm_polygons
    }
    
    if (!is.null(parking)){
      parking <- parking %>% 
        st_transform(utm) %>% 
        mutate(Value = 50) %>% 
        select(Value)
    } else {
      parking <- bb %>% 
        st_as_sfc() %>% 
        st_as_sf() %>% 
        st_transform(utm) %>% 
        mutate(Value = 0) %>% 
        select(Value)
    }
    
    
    
    # Rasterize ---------------------------------------------------------------
    
    # Rasterize to match grid of esa and save raster
    parking %>% rasterize(esa_rast, 
                            field = "Value",
                            background = 0,
                            filename = here(path, "parking_1m.tif"),
                            overwrite = TRUE)
    
    print("Parking raster saved")
    
    # Combine rasters ---------------------------------------------------------
    
    open_space_rast <- rast(here(path, "open_space_1m.tif"))
    roads_rast <- rast(here(path, "roads_1m.tif"))
    water_rast <- rast(here(path, "water_1m.tif"))
    buildings_rast <- rast(here(path, "buildings_1m.tif"))
    parking_rast <- rast(here(path, "parking_1m.tif"))
    
    LULC <- max(esa_rast,
                open_space_rast,
                roads_rast,
                water_rast,
                buildings_rast,
                parking_rast)
    
    writeRaster(x = LULC, 
                filename = here(path, "LULC.tif"),
                overwrite = TRUE,
                datatype = 'INT1U',
                gdal = c("BLOCKXSIZE=512", "BLOCKYSIZE=512"))
    
    # 2. From local to gcs
    gs_uri <- local_to_gcs(
      x = here(path, "LULC.tif"),
      bucket = 'wri-cities-lulc-gee' 
    )

    assetID <- paste0("projects/earthengine-legacy/assets/projects/wri-datalab/cities/SSC/LULC_V2/", city_name, "_LULC_1m_", i)
    
    properties <- list(city = city_name, 
                       gridCell = "i", 
                       version = "V2",
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
    
    print("LULC raster saved")

    keep <- c("city", "utm", "utm_ee", "country_code", "city_grid", "lanes", "path", "road_path", "aois", "city_name")
    rm(list = setdiff(ls(), keep))
    
    gc()
    
    
    
  }
  
  # Delete files in folder
  file.list <- list.files(here(path), full.names = TRUE)
  unlink(file.list, recursive = TRUE)
  
}

create_LULC("Mexico_City", here("cities", "boundaries", "MEX-Mexico_City.geojson"), 
            country_code = "MEX")


mexico <- tribble(
  ~ City, ~ File, ~ Code,
  "Cancun", here("cities", "boundaries", "MEX-Cancun-EJW.shp"), "MEX",
  "Hermosillo", here("cities", "boundaries", "MEX-Hermosillo-EJW.shp"), "MEX",
  "Monterrey", here("cities", "boundaries", "MEX-Monterrey.geojson"), "MEX",
  "Mexico_City", here("cities", "boundaries", "MEX-Mexico_City.geojson"), "MEX"
)

pmap(list(mexico$City, mexico$File, mexico$Code), create_LULC)




