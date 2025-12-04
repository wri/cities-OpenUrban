#!/usr/bin/env Rscript

# Standalone helper to write various R objects to S3 using paws
# - Infers format from file extension
# - Supports: parquet / geoparquet, sf/sfc (vector), terra SpatRaster, csv
# - Expects `file_path` in the form "bucket/key/filename.ext" or "bucket/…"

# ---- Package checks -------------------------------------------------------

.required_pkgs <- c("paws", "glue", "sf", "terra", "readr")

.missing <- .required_pkgs[
  vapply(.required_pkgs, function(p) !requireNamespace(p, quietly = TRUE), logical(1))
]

if (length(.missing) > 0) {
  stop(
    "The following packages are required but not installed: ",
    paste(.missing, collapse = ", "),
    "\nPlease install them, e.g.: install.packages(c(",
    paste(sprintf('"%s"', .missing), collapse = ", "),
    "))",
    call. = FALSE
  )
}

rm(.required_pkgs, .missing)

# ---- Optional packages (checked lazily inside the function) --------------
# - sfarrow (for GeoParquet with sf)
# - arrow   (for Parquet with data.frames / tibbles)

# ---- Defaults you might want in other scripts ----------------------------

# Create a single S3 client when this file is sourced
s3 <- paws::s3()

#' Write an object to S3, inferring the format from the file extension
#'
#' @param obj       Object to write (sf/sfc, terra SpatRaster, data.frame, etc.)
#' @param file_path S3 path in the form "bucket/path/to/file.ext"
#'                  (you can build this with glue, e.g.
#'                  glue("{bucket}/{folder}/file.geojson"))
#' @param s3_client (optional) paws S3 client; defaults to the one defined above.
#' @return Invisibly returns the "s3://bucket/key" URI
write_s3 <- function(obj, out_file_path, s3_client = s3) {
  # construct s3:// URI
  s3_uri <- glue::glue("s3://{out_file_path}")
  
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
    ctype <- if (ext %in% c("geojson", "json")) {
      "application/geo+json"
    } else {
      "application/octet-stream"
    }
    
  } else if (inherits(obj, "SpatRaster")) {
    # raster data
    terra::writeRaster(obj, tmp, overwrite = TRUE)
    ctype <- if (ext %in% c("tif", "tiff")) {
      "image/tiff"
    } else {
      "application/octet-stream"
    }
    
  } else if (ext == "csv") {
    # tabular CSV
    readr::write_csv(obj, file = tmp)
    ctype <- "text/csv"
    
  } else {
    stop("Unsupported extension: ", ext)
  }
  
  # upload to S3
  raw <- readBin(tmp, "raw", file.info(tmp)$size)
  s3_client$put_object(
    Bucket      = bkt,
    Key         = key,
    Body        = raw,
    ContentType = ctype
  )
  
  unlink(tmp)
  invisible(s3_uri)
}
