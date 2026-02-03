# Opportunity metrics

# Function to load and merge rasters from a list of paths
load_and_merge <- function(paths) {
  if (length(paths) == 0) {
    stop("No raster paths provided in plantable_paths")
  }
  
  rasters <- lapply(paths, rast_retry)
  
  if (length(rasters) == 1) {
    return(rasters[[1]])   # just return the single raster
  } else {
    return(do.call(merge, c(rasters)))  # merge multiple rasters
  }
}

list_tiles <- function(folder_s3_url, profile = "cities-data-dev") {
  # Normalize: ensure trailing slash so `aws s3 ls` treats it as a "folder"
  folder_s3_url <- sub("/?$", "/", folder_s3_url)
  
  lines <- system2(
    "aws",
    c("s3", "ls", folder_s3_url, "--profile", profile),
    stdout = TRUE,
    stderr = TRUE
  )
  
  tile_lines <- grep("^[[:space:]]*PRE[[:space:]]+tile_[0-9]+/", lines, value = TRUE)
  sub("^[[:space:]]*PRE[[:space:]]+(tile_[0-9]+)/$", "\\1", tile_lines)
}

bucket <- "wri-cities-indicators"
prefix <- "data/published/layers/"

tree_prefix <- "TreeCanopyHeight/tif/{city}__urban_extent__TreeCanopyHeight__Height_3.tif/"
albedo_prefix <- "AlbedoCloudMasked__ZonalStats_median/{city}__urban_extent__AlbedoCloudMasked__ZonalStats_median__StartYear_None_EndYear_None.tif"
world_pop_prefix <- "WorldPop/tif/ARG-Buenos_Aires__ADM2union__WorldPop__StartYear_2020_EndYear_2020.tif"





