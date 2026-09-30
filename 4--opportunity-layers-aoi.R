#!/usr/bin/env Rscript
# Opportunity layers for a custom boundary (AOI)
# - get_aoi_data.py checks OpenUrban coverage and fetches the inputs with CIF into
#   s3://wri-cities-tcm/OpenUrban/{city}/aoi/{aoi_name}/inputs/
# - compute_opportunity() (3--opportunity-layers.R) runs the same method as the citywide
#   run, with targets computed from the cells inside the boundary
# - outputs go to s3://wri-cities-tcm/OpenUrban/{city}/aoi/{aoi_name}/opportunity-layers/

suppressPackageStartupMessages({
  library(optparse)
  library(stringr)
  library(glue)
  library(here)
})

Sys.setenv(
  GDAL_HTTP_MAX_RETRY = "20",
  GDAL_HTTP_RETRY_DELAY = "2",
  CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tif,.tiff,.geojson,.json",
  GDAL_DISABLE_READDIR_ON_OPEN = "EMPTY_DIR",
  PYTHONUNBUFFERED = "1"
)

source(here("3--opportunity-layers.R"))
source(here("utils", "opportunity-keys.R"))
source(here("utils", "open-urban-helpers.R"))

aws_http <- "https://wri-cities-tcm.s3.us-east-1.amazonaws.com"

# ---- CLI options ----
option_list <- list(
  make_option(c("-c", "--city"), type = "character",
              help = "City key for the output folder, e.g. 'USA-Oakland'"),
  make_option(c("--aoi-name"), type = "character",
              help = "Name for this boundary, e.g. 'city-limits' (letters, numbers, . _ -)"),
  make_option(c("--boundary"), type = "character", default = NULL,
              help = "Boundary file or URL readable by geopandas (not needed with --skip-download)"),
  make_option(c("--opportunity"), type = "character", default = "all",
              help = "Comma-separated opportunity layers (default: all)"),
  make_option(c("--lulc-source"), type = "character", default = "auto",
              help = "OpenUrban source: auto, tcm (generated tiles in S3) or cif (GEE asset). Default: auto"),
  make_option(c("--worldpop-version"), type = "integer", default = 2,
              help = "WorldPop version to use (default: 2)"),
  make_option(c("--albedo-start"), type = "character", default = NULL,
              help = "Albedo start date YYYY-MM-DD (default: CIF's previous-summer range)"),
  make_option(c("--albedo-end"), type = "character", default = NULL,
              help = "Albedo end date YYYY-MM-DD (default: CIF's previous-summer range)"),
  make_option(c("--skip-download"), action = "store_true", default = FALSE,
              help = "Use inputs already in S3 instead of running get_aoi_data.py")
)

parser <- OptionParser(option_list = option_list)
opt <- parse_args(parser)

city <- opt$city
aoi_name <- opt$`aoi-name`

if (is.null(city) || !nzchar(city) || is.null(aoi_name) || !nzchar(aoi_name)) {
  print_help(parser)
  stop("\n--city and --aoi-name are required.", call. = FALSE)
}
if (!str_detect(aoi_name, "^[A-Za-z0-9._-]+$")) {
  stop("--aoi-name may only contain letters, numbers, '.', '_' and '-'.", call. = FALSE)
}
if (!isTRUE(opt$`skip-download`) && (is.null(opt$boundary) || !nzchar(opt$boundary))) {
  stop("--boundary is required unless --skip-download is set.", call. = FALSE)
}
if (!opt$`lulc-source` %in% c("auto", "tcm", "cif")) {
  stop("--lulc-source must be one of: auto, tcm, cif.", call. = FALSE)
}

opp_keys <- parse_opportunity_keys(opt$opportunity)
keys <- resolve_write_keys(opp_keys)
fetch_layers <- c(if (keys$need_tree) "trees", if (keys$need_cool) "cool-roofs")

aoi_http <- glue("{aws_http}/OpenUrban/{city}/aoi/{aoi_name}")

message("City: ", city)
message("AOI: ", aoi_name)
message("Opportunity keys: ", paste(opp_keys, collapse = ", "))
message("Output: s3://wri-cities-tcm/OpenUrban/", city, "/aoi/", aoi_name, "/opportunity-layers/")

status <- tryCatch({

  # ---- 1) Inputs ----
  if (!isTRUE(opt$`skip-download`)) {
    boundary <- if (file.exists(opt$boundary)) normalizePath(opt$boundary) else opt$boundary
    # Same python resolution as 2--OpenUrban-generation.R
    py <- Sys.getenv("OPENURBAN_PYTHON", unset = "python")

    args <- c(
      "run", "--no-capture-output", "-n", "open-urban",
      py, "-u", "get_aoi_data.py", city,
      "--aoi-name", aoi_name,
      "--boundary", boundary,
      "--lulc-source", opt$`lulc-source`,
      "--layers", paste(fetch_layers, collapse = ","),
      "--worldpop-version", opt$`worldpop-version`
    )
    if (!is.null(opt$`albedo-start`)) args <- c(args, "--albedo-start", opt$`albedo-start`)
    if (!is.null(opt$`albedo-end`)) args <- c(args, "--albedo-end", opt$`albedo-end`)

    message("==> Fetching inputs (get_aoi_data.py)...")
    exit_status <- run_python_live(args, wd = here())
    if (!identical(as.integer(exit_status), 0L)) {
      stop(glue("get_aoi_data.py failed (exit status {exit_status}); see output above."))
    }
  } else {
    message("Skipping input download (--skip-download).")
  }

  # ---- 2) Read inputs ----
  manifest <- jsonlite::fromJSON(glue("{aoi_http}/inputs/manifest.json"))
  message("OpenUrban source: ", manifest$lulc_source,
          if (!is.null(manifest$lulc_source_city)) glue(" ({manifest$lulc_source_city})") else "")

  boundary_sf <- st_read(glue("{aoi_http}/boundaries/aoi.geojson"), quiet = TRUE)
  tiles <- st_read(glue("{aoi_http}/inputs/tiles.geojson"), quiet = TRUE) |>
    mutate(across(c(lulc_path, tree_path, albedo_path), as.character))

  if (keys$need_tree && any(is.na(tiles$tree_path))) {
    stop("Tree canopy inputs are missing; rerun without --skip-download.")
  }
  if (keys$need_cool && any(is.na(tiles$albedo_path))) {
    stop("Albedo inputs are missing; rerun without --skip-download.")
  }

  # ---- 3) Opportunity layers ----
  message("==> Running opportunity workflow...")
  compute_opportunity(
    city = city,
    boundary = boundary_sf,
    wp_path = manifest$worldpop_path,
    tiles = tiles,
    write_keys = opp_keys,
    out_prefix = glue("wri-cities-tcm/OpenUrban/{city}/aoi/{aoi_name}/opportunity-layers"),
    checkpoint_dir = here("tmp", "opportunity-checkpoints", city, "aoi", aoi_name),
    crop_to_boundary = TRUE
  )
  message("==> Opportunity layers complete.")
  0L

}, error = function(e) {
  message("!! FAILED: ", city, " / ", aoi_name)
  message("!! ERROR: ", conditionMessage(e))
  1L
})

quit(status = status)
