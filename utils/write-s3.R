write_s3 <- function(obj, file_path) {
  s3_uri <- if (grepl("^s3://", file_path)) file_path else glue::glue("s3://{file_path}")
  
  # parse bucket/key
  p   <- sub("^s3://", "", s3_uri)
  bkt <- sub("/.*", "", p)
  key <- sub("^[^/]+/", "", p)
  ext <- tolower(tools::file_ext(key))
  
  tmp <- tempfile(fileext = paste0(".", ext))
  on.exit({
    if (file.exists(tmp)) unlink(tmp)
  }, add = TRUE)
  
  # write to a local temp file (keep 2nd function behavior + add geoparquet)
  if (inherits(obj, "sf") || inherits(obj, "sfc")) {
    
    # NEW: support GeoParquet / Parquet for sf/sfc
    if (ext %in% c("parquet", "geoparquet", "pq")) {
      if (inherits(obj, "sfc")) obj <- sf::st_as_sf(obj)
      if (!requireNamespace("sfarrow", quietly = TRUE)) {
        stop("Package 'sfarrow' is required to write GeoParquet.")
      }
      sfarrow::st_write_parquet(obj, tmp)
      ctype <- "application/octet-stream"  # keep 2nd fn style (not fancy MIME)
      
    } else {
      # Original 2nd-function sf/sfc behavior
      if (inherits(obj, "sfc")) obj <- sf::st_as_sf(obj)
      sf::st_write(obj, tmp, delete_dsn = TRUE, quiet = TRUE)
      ctype <- if (ext %in% c("geojson", "json")) "application/geo+json" else "application/octet-stream"
    }
    
  } else if (inherits(obj, "SpatRaster")) {
    
    terra::writeRaster(obj, tmp, overwrite = TRUE)
    ctype <- if (ext %in% c("tif", "tiff")) "image/tiff" else "application/octet-stream"
    
  } else if (inherits(obj, c("RasterLayer", "RasterStack", "RasterBrick"))) {
    
    raster::writeRaster(obj, tmp, overwrite = TRUE)
    ctype <- if (ext %in% c("tif", "tiff")) "image/tiff" else "application/octet-stream"
    
  } else if (ext == "csv") {
    
    readr::write_csv(obj, file = tmp)
    ctype <- "application/octet-stream"  # keep 2nd fn behavior
    
  } else if (ext == "txt") {
    
    readr::write_delim(obj, file = tmp, delim = "\t")
    ctype <- "text/plain"
    
  } else if (ext %in% c("parquet", "pq")) {
    
    # NEW: (optional but useful) Parquet for non-sf objects
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("Package 'arrow' is required to write Parquet.")
    }
    arrow::write_parquet(as.data.frame(obj), tmp)
    ctype <- "application/octet-stream"  # keep 2nd fn style
    
  } else {
    stop("Unsupported extension: ", ext)
  }
  
  # upload
  raw <- readBin(tmp, "raw", file.info(tmp)$size)
  s3$put_object(Bucket = bkt, Key = key, Body = raw, ContentType = ctype)
  
  invisible(s3_uri)
}
