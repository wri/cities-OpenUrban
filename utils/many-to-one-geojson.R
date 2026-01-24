#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(stringr)
  library(geoarrow)
  library(sfarrow)
})

# ----------------------------
# Settings
# ----------------------------
bucket <- "wri-cities-heat"
root_prefix <- "OpenUrban"
layers <- c("buildings", "open_space", "parking", "roads")

# Control flags
DRY_RUN <- FALSE        # TRUE = print actions only, do not write/upload/delete
QUIET_AWS <- TRUE       # TRUE = --only-show-errors

# If you want to limit to certain cities, put them here; otherwise all cities found are processed.
CITY_WHITELIST <- character(0)  # e.g. c("USA-Boston", "ZAF-Cape_Town")

# ----------------------------
# AWS helpers
# ----------------------------
aws_run <- function(args) {
  out <- suppressWarnings(system2("aws", args = args, stdout = TRUE, stderr = TRUE))
  status <- attr(out, "status"); if (is.null(status)) status <- 0L
  list(ok = (status == 0L), status = status, output = paste(out, collapse = "\n"))
}

s3_uri <- function(bucket, key) sprintf("s3://%s/%s", bucket, sub("^/+", "", key))

s3_exists <- function(bucket, key) {
  # fast existence check
  res <- aws_run(c("s3api", "head-object", "--bucket", bucket, "--key", key))
  isTRUE(res$ok)
}

s3_cp_down <- function(bucket, key, local_path) {
  args <- c("s3", "cp", s3_uri(bucket, key), local_path)
  if (QUIET_AWS) args <- c(args, "--only-show-errors")
  aws_run(args)
}

s3_cp_up <- function(local_path, bucket, key) {
  args <- c("s3", "cp", local_path, s3_uri(bucket, key))
  if (QUIET_AWS) args <- c(args, "--only-show-errors")
  aws_run(args)
}

# List keys under prefix via `aws s3 ls --recursive` (handles pagination)
s3_list_keys <- function(bucket, prefix_key) {
  res <- aws_run(c("s3", "ls", s3_uri(bucket, prefix_key), "--recursive"))
  if (!res$ok) stop("Failed to list objects:\n", res$output)
  
  lines <- unlist(strsplit(res$output, "\n", fixed = TRUE), use.names = FALSE)
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  
  # Extract last whitespace-delimited field as KEY
  keys <- vapply(lines, function(x) {
    parts <- strsplit(x, "\\s+", perl = TRUE)[[1]]
    tail(parts, 1)
  }, character(1))
  
  keys
}

# ----------------------------
# Geo helpers
# ----------------------------
read_any_geojson_from_s3 <- function(bucket, key) {
  tmp <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp, force = TRUE), add = TRUE)
  r <- s3_cp_down(bucket, key, tmp)
  if (!r$ok) stop("Download failed: ", key, "\n", r$output)
  sf::read_sf(tmp, quiet = TRUE)
}

read_parquet_from_s3 <- function(bucket, key) {
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp, force = TRUE), add = TRUE)
  r <- s3_cp_down(bucket, key, tmp)
  if (!r$ok) stop("Download failed: ", key, "\n", r$output)
  st_read_parquet(tmp, quiet = TRUE)
}

write_geojson_local <- function(gdf, path) {
  # GeoJSON driver
  sf::write_sf(gdf, path, driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE)
}

dedup_by_id <- function(gdf) {
  if (!("id" %in% names(gdf))) {
    warning("No 'id' column found; returning as-is.")
    return(gdf)
  }
  # Keep first row per id
  gdf %>% group_by(id) %>% slice(1) %>% ungroup()
}

# ----------------------------
# Discover cities
# ----------------------------
all_keys <- s3_list_keys(bucket, paste0(root_prefix, "/"))
# City folders look like: OpenUrban/<CITY>/<layer>/...
cities <- all_keys |>
  str_subset(paste0("^", root_prefix, "/[^/]+/")) |>
  str_match(paste0("^", root_prefix, "/([^/]+)/")) |>
  (\(m) m[,2])() |>
  unique() |>
  sort()

if (length(CITY_WHITELIST) > 0) {
  cities <- intersect(cities, CITY_WHITELIST)
}

message("Found ", length(cities), " cities to process.")

# Build an index (data frame) of keys for fast per-city/layer lookup
idx <- tibble(key = all_keys) %>%
  mutate(
    city  = str_match(key, paste0("^", root_prefix, "/([^/]+)/"))[,2],
    layer = str_match(key, paste0("^", root_prefix, "/[^/]+/([^/]+)/"))[,2],
    file  = str_match(key, paste0("^", root_prefix, "/[^/]+/[^/]+/(.+)$"))[,2]
  ) %>%
  filter(!is.na(city), !is.na(layer), layer %in% layers)

# ----------------------------
# Main loop
# ----------------------------
results <- list()

for (city in cities) {
  for (layer in layers) {
    
    # Keys we care about in this folder
    folder_prefix <- paste0(root_prefix, "/", city, "/", layer, "/")
    sub <- idx %>% filter(city == !!city, layer == !!layer)
    
    all_geojson_key <- paste0(folder_prefix, layer, "_all.geojson")
    all_parquet_key <- paste0(folder_prefix, layer, "_all.parquet")
    
    # numbered tiles: layer_1.geojson, layer_2.geojson, ...
    tile_geojson <- sub$key[str_detect(sub$file, paste0("^", layer, "_\\d+\\.geojson$"))]
    tile_geojson <- sort(tile_geojson)
    
    message("\n=== ", city, " / ", layer, " ===")
    
    # (1) If *_all.geojson exists -> skip
    if (s3_exists(bucket, all_geojson_key)) {
      message("Exists: ", all_geojson_key, " (skip)")
      results[[length(results)+1]] <- list(city=city, layer=layer, action="skip_exists_all_geojson", ok=TRUE)
      next
    }
    
    # (2) If *_all.parquet exists -> convert parquet -> all.geojson
    if (s3_exists(bucket, all_parquet_key)) {
      message("Convert parquet -> geojson: ", all_parquet_key, " -> ", all_geojson_key)
      
      if (DRY_RUN) {
        results[[length(results)+1]] <- list(city=city, layer=layer, action="dryrun_convert_parquet", ok=TRUE)
        next
      }
      
      ok <- tryCatch({
        gdf <- read_parquet_from_s3(bucket, all_parquet_key)
        out <- tempfile(fileext = ".geojson")
        on.exit(unlink(out, force = TRUE), add = TRUE)
        
        write_geojson_local(gdf, out)
        
        up <- s3_cp_up(out, bucket, all_geojson_key)
        if (!up$ok) stop(up$output)
        
        TRUE
      }, error = function(e) {
        message("ERROR: ", conditionMessage(e))
        FALSE
      })
      
      results[[length(results)+1]] <- list(city=city, layer=layer, action="convert_parquet", ok=ok)
      next
    }
    
    # (3) Else if tiles exist -> combine tiles -> dedup -> upload all.geojson
    if (length(tile_geojson) > 0) {
      message("Combine ", length(tile_geojson), " tile geojsons -> ", all_geojson_key)
      
      if (DRY_RUN) {
        results[[length(results)+1]] <- list(city=city, layer=layer, action="dryrun_combine_tiles", ok=TRUE)
        next
      }
      
      ok <- tryCatch({
        # Read + row-bind
        g_list <- lapply(tile_geojson, function(k) read_any_geojson_from_s3(bucket, k))
        gdf <- do.call(rbind, g_list)
        
        # Deduplicate by id
        gdf <- dedup_by_id(gdf)
        
        out <- tempfile(fileext = ".geojson")
        on.exit(unlink(out, force = TRUE), add = TRUE)
        
        write_geojson_local(gdf, out)
        
        up <- s3_cp_up(out, bucket, all_geojson_key)
        if (!up$ok) stop(up$output)
        
        TRUE
      }, error = function(e) {
        message("ERROR: ", conditionMessage(e))
        FALSE
      })
      
      results[[length(results)+1]] <- list(city=city, layer=layer, action="combine_tiles", ok=ok)
      next
    }
    
    # (4) Nothing to do
    message("No *_all.geojson, no *_all.parquet, and no tiles to combine (skip).")
    results[[length(results)+1]] <- list(city=city, layer=layer, action="skip_nothing_found", ok=TRUE)
  }
}

# ----------------------------
# Summary
# ----------------------------
ok_vec <- vapply(results, function(x) isTRUE(x$ok), logical(1))
message("\n==============================")
message("Done. Success: ", sum(ok_vec), " / ", length(ok_vec))
fail <- which(!ok_vec)
if (length(fail)) {
  message("\nFailures:")
  for (i in fail) {
    x <- results[[i]]
    message("- ", x$city, " / ", x$layer, " (action=", x$action, ")")
  }
}

# saveRDS(results, "openurban_make_all_geojson_results.rds")
