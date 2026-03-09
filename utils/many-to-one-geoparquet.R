#!/usr/bin/env Rscript

# Rscript utils/many-to-one-geoparquet.R \
# --city=USA-Boston \
# --types=buildings,roads,open_space,water,parking


suppressPackageStartupMessages({
  library(sf)
  library(stringr)
  library(sfarrow)
})

# ----------------------------
# Defaults / constants
# ----------------------------
DEFAULT_BUCKET <- "wri-cities-tcm"
DEFAULT_ROOT_PREFIX <- "OpenUrban"
ALLOWED_TYPES <- c("buildings", "roads", "open_space", "water", "parking")

QUIET_AWS <- TRUE

usage <- function() {
  cat(
    paste0(
      "Usage:\n",
      "  Rscript utils/many-to-one-geoparquet.R --city=<CITY> --types=<TYPE1,TYPE2,...> [--bucket=<BUCKET>] [--root-prefix=<PREFIX>] [--dry-run]\n\n",
      "Example:\n",
      "  Rscript utils/many-to-one-geoparquet.R --city=BRA-Rio_de_Janeiro --types=buildings,roads,open_space,water,parking\n"
    )
  )
}

parse_args <- function(args) {
  opts <- list(
    city = NULL,
    types = NULL,
    bucket = DEFAULT_BUCKET,
    root_prefix = DEFAULT_ROOT_PREFIX,
    dry_run = FALSE
  )

  for (arg in args) {
    if (arg %in% c("-h", "--help")) {
      usage()
      quit(status = 0)
    } else if (startsWith(arg, "--city=")) {
      opts$city <- sub("^--city=", "", arg)
    } else if (startsWith(arg, "--types=")) {
      opts$types <- sub("^--types=", "", arg)
    } else if (startsWith(arg, "--bucket=")) {
      opts$bucket <- sub("^--bucket=", "", arg)
    } else if (startsWith(arg, "--root-prefix=")) {
      opts$root_prefix <- sub("^--root-prefix=", "", arg)
    } else if (arg == "--dry-run") {
      opts$dry_run <- TRUE
    } else {
      stop("Unknown argument: ", arg)
    }
  }

  if (is.null(opts$city) || !nzchar(opts$city)) {
    stop("Missing required argument: --city")
  }
  if (is.null(opts$types) || !nzchar(opts$types)) {
    stop("Missing required argument: --types")
  }

  type_vec <- strsplit(opts$types, ",", fixed = TRUE)[[1]]
  type_vec <- trimws(type_vec)
  type_vec <- unique(type_vec[nzchar(type_vec)])

  invalid <- setdiff(type_vec, ALLOWED_TYPES)
  if (length(invalid) > 0) {
    stop(
      "Invalid --types values: ",
      paste(invalid, collapse = ", "),
      "\nAllowed values: ",
      paste(ALLOWED_TYPES, collapse = ", ")
    )
  }

  opts$type_vec <- type_vec
  opts
}

# ----------------------------
# AWS helpers
# ----------------------------
aws_run <- function(args) {
  out <- suppressWarnings(system2("aws", args = args, stdout = TRUE, stderr = TRUE))
  status <- attr(out, "status")
  if (is.null(status)) status <- 0L
  list(ok = (status == 0L), status = status, output = paste(out, collapse = "\n"))
}

s3_uri <- function(bucket, key) sprintf("s3://%s/%s", bucket, sub("^/+", "", key))

s3_list_keys <- function(bucket, prefix_key) {
  res <- aws_run(c("s3", "ls", s3_uri(bucket, prefix_key), "--recursive"))
  if (!res$ok) {
    stop("Failed to list objects for ", s3_uri(bucket, prefix_key), "\n", res$output)
  }

  lines <- unlist(strsplit(res$output, "\n", fixed = TRUE), use.names = FALSE)
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  if (length(lines) == 0) return(character(0))

  vapply(lines, function(x) {
    parts <- strsplit(x, "\\s+", perl = TRUE)[[1]]
    tail(parts, 1)
  }, character(1))
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

# ----------------------------
# Geo helpers
# ----------------------------
read_geojson_from_s3 <- function(bucket, key) {
  tmp <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp, force = TRUE), add = TRUE)
  r <- s3_cp_down(bucket, key, tmp)
  if (!r$ok) stop("Download failed: ", key, "\n", r$output)
  sf::read_sf(tmp, quiet = TRUE)
}

bind_sf_fill <- function(g_list) {
  all_cols <- unique(unlist(lapply(g_list, names), use.names = FALSE))
  g_list_aligned <- lapply(g_list, function(g) {
    miss <- setdiff(all_cols, names(g))
    if (length(miss) > 0) {
      for (m in miss) g[[m]] <- NA
    }
    g[, all_cols]
  })
  do.call(rbind, g_list_aligned)
}

dedup_buildings <- function(gdf) {
  if (!("id" %in% names(gdf))) {
    warning("No 'id' column found for buildings; falling back to spatial dedup.")
    return(dedup_spatial(gdf))
  }
  # Fast O(n) dedup for large building tables.
  gdf[!duplicated(gdf$id), , drop = FALSE]
}

dedup_spatial <- function(gdf) {
  n <- nrow(gdf)
  if (n <= 1) return(gdf)

  # First pass: exact-geometry dedup via WKB signature.
  wkb <- sf::st_as_binary(sf::st_geometry(gdf), EWKB = TRUE)
  wkb_sig <- vapply(wkb, function(x) paste(as.integer(x), collapse = ","), character(1))
  keep_exact <- !duplicated(wkb_sig)
  gdf_exact <- gdf[keep_exact, , drop = FALSE]

  if (nrow(gdf_exact) <= 1) return(gdf_exact)

  # Second pass: topological equality in case same shape is encoded differently.
  eq <- sf::st_equals(gdf_exact)
  keep <- rep(FALSE, nrow(gdf_exact))
  visited <- rep(FALSE, nrow(gdf_exact))

  for (i in seq_len(nrow(gdf_exact))) {
    if (visited[i]) next
    grp <- eq[[i]]
    visited[grp] <- TRUE
    keep[i] <- TRUE
  }

  gdf_exact[keep, , drop = FALSE]
}

write_parquet_to_s3 <- function(gdf, bucket, key) {
  out <- tempfile(fileext = ".parquet")
  on.exit(unlink(out, force = TRUE), add = TRUE)
  sfarrow::st_write_parquet(gdf, out)
  up <- s3_cp_up(out, bucket, key)
  if (!up$ok) stop("Upload failed: ", key, "\n", up$output)
  invisible(TRUE)
}

process_type <- function(bucket, root_prefix, city, type_name, dry_run = FALSE) {
  prefix <- paste0(root_prefix, "/", city, "/", type_name, "/")
  out_key <- paste0(prefix, type_name, "_all.parquet")

  message("\n=== ", city, " / ", type_name, " ===")
  keys <- s3_list_keys(bucket, prefix)
  if (length(keys) == 0) {
    message("No files found under ", s3_uri(bucket, prefix), " (skip)")
    return(list(type = type_name, ok = TRUE, action = "skip_no_files", n_in = 0L, n_out = 0L))
  }

  tile_re <- paste0("^", root_prefix, "/", city, "/", type_name, "/", type_name, "_\\d+\\.geojson$")
  tile_keys <- sort(keys[str_detect(keys, tile_re)])

  if (length(tile_keys) == 0) {
    message("No tile geojsons matching ", type_name, "_<tile>.geojson (skip)")
    return(list(type = type_name, ok = TRUE, action = "skip_no_tiles", n_in = 0L, n_out = 0L))
  }

  message("Found ", length(tile_keys), " tile files")
  message("Output: ", s3_uri(bucket, out_key))

  if (dry_run) {
    return(list(type = type_name, ok = TRUE, action = "dry_run", n_in = NA_integer_, n_out = NA_integer_))
  }

  g_list <- lapply(tile_keys, function(k) read_geojson_from_s3(bucket, k))
  gdf <- bind_sf_fill(g_list)
  n_in <- nrow(gdf)

  gdf_dedup <- if (type_name == "buildings") dedup_buildings(gdf) else dedup_spatial(gdf)
  n_out <- nrow(gdf_dedup)

  message("Rows before dedup: ", n_in)
  message("Rows after dedup:  ", n_out)

  write_parquet_to_s3(gdf_dedup, bucket, out_key)
  message("Uploaded ", s3_uri(bucket, out_key))

  list(type = type_name, ok = TRUE, action = "merged", n_in = n_in, n_out = n_out)
}

main <- function() {
  opts <- parse_args(commandArgs(trailingOnly = TRUE))

  message("City: ", opts$city)
  message("Types: ", paste(opts$type_vec, collapse = ", "))
  message("Bucket: ", opts$bucket)
  message("Root prefix: ", opts$root_prefix)
  message("Dry run: ", opts$dry_run)

  results <- lapply(opts$type_vec, function(type_name) {
    tryCatch(
      process_type(
        bucket = opts$bucket,
        root_prefix = opts$root_prefix,
        city = opts$city,
        type_name = type_name,
        dry_run = opts$dry_run
      ),
      error = function(e) {
        message("ERROR for ", type_name, ": ", conditionMessage(e))
        list(type = type_name, ok = FALSE, action = "error", n_in = NA_integer_, n_out = NA_integer_)
      }
    )
  })

  ok <- vapply(results, function(x) isTRUE(x$ok), logical(1))
  message("\n==============================")
  message("Done. Success: ", sum(ok), " / ", length(ok))

  if (any(!ok)) {
    message("Failed types: ", paste(vapply(results[!ok], `[[`, character(1), "type"), collapse = ", "))
  }
}

main()
