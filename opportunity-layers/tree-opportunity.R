library(terra)
library(rgee)
library(gfcanalysis)
library(googledrive)
library(sf)
library(here)
library(tidyverse)
library(glue)
library(geoarrow)
library(sfarrow)
library(googleCloudStorageR)

library(reticulate)
use_condaenv("chri", required = TRUE)

# S3 setup ----------------------------------------------------------------


# 1) Log in via AWS SSO for that profile (opens browser)
system("aws sso login --profile cities-data-dev")

# 2) Point this R/reticulate session at that profile & region
Sys.setenv(
  AWS_SDK_LOAD_CONFIG = "1",      # make boto3 read ~/.aws/config
  AWS_PROFILE = "cities-data-dev",
  AWS_DEFAULT_REGION = "us-east-1"
)

s3 <- paws::s3()
bucket <- "wri-cities-heat"
aws_http <- "https://wri-cities-heat.s3.us-east-1.amazonaws.com"

# Function to write to s3
write_s3 <- function(obj, file_path) {
  s3_uri <- glue::glue("s3://{file_path}")
  
  # parse bucket/key
  p   <- sub("^s3://", "", s3_uri)
  bkt <- sub("/.*", "", p)
  key <- sub("^[^/]+/", "", p)
  ext <- tolower(tools::file_ext(key))
  tmp <- tempfile(fileext = paste0(".", ext))
  
  # choose writer + content-type
  if (ext %in% c("parquet", "geoparquet", "pq")) {
    # write Parquet / GeoParquet
    if (inherits(obj, "sf") || inherits(obj, "sfc")) {
      if (inherits(obj, "sfc")) obj <- sf::st_as_sf(obj)
      if (!requireNamespace("sfarrow", quietly = TRUE))
        stop("Package 'sfarrow' is required to write GeoParquet.")
      sfarrow::st_write_parquet(obj, tmp)
    } else {
      if (!requireNamespace("arrow", quietly = TRUE))
        stop("Package 'arrow' is required to write Parquet.")
      arrow::write_parquet(as.data.frame(obj), tmp)
    }
    ctype <- "application/vnd.apache.parquet"
    
  } else if (inherits(obj, "sf") || inherits(obj, "sfc")) {
    # vector data (e.g., GeoJSON, GPKG, etc.)
    if (inherits(obj, "sfc")) obj <- sf::st_as_sf(obj)
    sf::st_write(obj, tmp, delete_dsn = TRUE, quiet = TRUE)
    ctype <- if (ext %in% c("geojson","json")) "application/geo+json" else "application/octet-stream"
    
  } else if (inherits(obj, "SpatRaster")) {
    # raster data
    terra::writeRaster(obj, tmp, overwrite = TRUE)
    ctype <- if (ext %in% c("tif","tiff")) "image/tiff" else "application/octet-stream"
    
  } else if (ext == "csv") {
    # tabular CSV
    readr::write_csv(obj, file = tmp)
    ctype <- "text/csv"
    
  } else {
    stop("Unsupported extension: ", ext)
  }
  
  # upload
  raw <- readBin(tmp, "raw", file.info(tmp)$size)
  s3$put_object(Bucket = bkt, Key = key, Body = raw, ContentType = ctype)
  unlink(tmp)
  invisible(s3_uri)
}


# Rgee setup -------------------------------------------------------------
py_run_file(here("utils", "rgee.py"))
ee_Initialize(project = "ee-ssc-lulc", drive = TRUE, quiet = TRUE)
Sys.setenv(EARTHENGINE_PROJECT = "ee-ssc-lulc")

gcs_auth("/Users/elizabeth.wesley/Documents/keys/nifty-memory-399115-7aa7f647b0b0.json")  
Sys.setenv(GCS_DEFAULT_BUCKET = "wri-cities-lulc-gee")
gcs_global_bucket("wri-cities-lulc-gee")  
googleCloudStorageR::gcs_upload_set_limit(as.integer(2.5e+7))  

# Function to upload to GCS
gcs_upload_until_success <- function(file,
                                     bucket,
                                     name = basename(file),
                                     type = "image/tiff",
                                     verbose = TRUE) {
  stopifnot(file.exists(file))
  
  # make sure package is loaded
  if (!"googleCloudStorageR" %in% .packages()) {
    suppressPackageStartupMessages(require(googleCloudStorageR))
  }
  
  # 1) raise the resumable limit ABOVE your file size
  #    50 MB is fine; must be integer
  googleCloudStorageR::gcs_upload_set_limit(as.integer(50 * 1024^2))
  
  attempt <- 1L
  wait <- 2
  
  repeat {
    res <- tryCatch(
      googleCloudStorageR::gcs_upload(
        file = file,
        bucket = bucket,
        name = name,
        type = type,
        upload_type = "simple"   # <-- force simple here
      ),
      error = function(e) e
    )
    
    if (!inherits(res, "error")) {
      if (verbose) message("✅ upload succeeded on attempt ", attempt)
      return(res)
    }
    
    # otherwise, we retry forever
    if (verbose) {
      message("❌ attempt ", attempt, " failed: ", conditionMessage(res))
      message("⏳ retrying in ", wait, "s (Ctrl+C to stop)")
    }
    Sys.sleep(wait)
    wait <- min(wait * 1.5, 30)
    attempt <- attempt + 1L
  }
}


# City data ---------------------------------------------------------------

city <- "Cape_Town"
country <- "ZAF"
city_name <- glue("{country}-{city}")

extent <- ee$FeatureCollection("projects/wri-datalab/cities/urban_land_use/data/global_cities_Aug2024/urbanextents_unions_2020")$
  filter(ee$Filter$stringContains('city_name_large', str_replace(city, "_", " ")))$
  filter(ee$Filter$eq("country_ISO", country))

lulcImg <- ee$ImageCollection("projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC")$
  filter(ee$Filter$eq('city', city_name))$
  filterBounds(extent)

proj <- lulcImg$first()$projection()

lulcImg <- lulcImg$
  max()$int()$rename('lulc')

canopy_ht <- ee$ImageCollection("projects/meta-forest-monitoring-okw37/assets/CanopyHeight")


# Get WorldPop grid -------------------------------------------------------

worldpop <- ee$ImageCollection("WorldPop/GP/100m/pop")$
  filterBounds(extent)

worldpop_proj <- worldpop$first()$projection()

worldpop <- worldpop$mosaic()$clip(extent$geometry()) 

grid <- worldpop$geometry()$coveringGrid(worldpop_proj)  

write_s3(ee_as_sf(grid, maxFeatures = 10e12), 
         glue("{bucket}/OpenUrban/{city_name}/worldpop_grid_100m.parquet"))


# Create albedo -----------------------------------------------------------

source(here("opportunity_layers", "create_albedo_rgee.R"))

cityAlb <- make_s2_albedo(extent$geometry(), date_start = "2024-12-01", date_end = "2025-02-28")

# Potential ----------------------------------------------------------

# 1. Remap LULC and mask water ---------------------------------------------

from <- c(110,120,130,200,300,400,500,600,601,602,610,611,612,620,621,622)
to   <- c(110,120,130,200,  9,400,500,600,601,602,600,601,602,600,601,602)

plantableLULC <- lulcImg$
  remap(from, to)$
  rename("lulc")

# mask out water (lulc == 9)
noWaterMask <- plantableLULC$neq(9)
plantableLULC <- plantableLULC$updateMask(noWaterMask)

# 2. Pixel area, tree cover, albedo ----------------------------------------

pixelArea <- ee$Image$pixelArea()$rename("area")

# Trees are over 3 m in height
tree_cover <- canopy_ht$
  filterBounds(extent)$
  mosaic()$
  gte(3)$
  rename("tree")

# 3. Per-cell grouped stats: mean(tree) + mean(alb) + sum(area) by lulc ----

reducer <- ee$Reducer$mean()$
  `repeat`(2)$
  combine(
    reducer2     = ee$Reducer$sum(),
    sharedInputs = FALSE
  )$
  group(
    groupField = 3L,
    groupName  = "lulc"
  )


grouped <- ee$Image$cat(list(
  tree_cover$rename("tree"),
  cityAlb$rename("alb"),
  pixelArea,
  plantableLULC$int()
))$
  clip(extent$geometry())$
  reduceRegions(
    collection = grid,
    reducer    = reducer,
    scale      = 1,
    crs        = proj
  )



# 4. Flatten rows: (gid, lulc, tree_pct, albedo, area) ---------------------

flat <- ee$FeatureCollection(
  grouped$map(
    ee_utils_pyfunc(function(f) {
      f <- ee$Feature(f)
      gid <- ee$String(f$get("gid"))
      
      groups <- ee$List(
        ee$Algorithms$If(
          ee$Algorithms$IsEqual(f$get("groups"), NULL),
          ee$List(list()),      # no groups for this feature
          f$get("groups")       # list of dicts
        )
      )
      
      feats <- groups$map(
        ee_utils_pyfunc(function(g) {
          g <- ee$Dictionary(g)
          
          means <- ee$List(g$get("mean"))
          tree_pct <- means$get(0)
          albedo   <- means$get(1)
          
          ee$Feature(
            NULL,
            list(
              gid      = gid,
              lulc     = g$get("lulc"),
              tree_pct = tree_pct,
              albedo   = albedo,
              area     = g$get("sum")
            )
          )
        })
      )
      
      ee$FeatureCollection(feats)
    })
  )
)$flatten()

write_s3(ee_as_sf(flat, maxFeatures = 10e12), 
         glue("{bucket}/OpenUrban/{city_name}/opportunity-layers/tree_alb_grid_100m.parquet"))
