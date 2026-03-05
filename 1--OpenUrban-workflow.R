#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse)
  library(stringr)
  library(glue)
  library(here)
})

Sys.setenv(
  GDAL_HTTP_MAX_RETRY = "20",
  GDAL_HTTP_RETRY_DELAY = "2",
  CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tif,.tiff,.geojson",
  GDAL_DISABLE_READDIR_ON_OPEN = "EMPTY_DIR"
)

# ---- source workflows ----
source(here("2--OpenUrban-generation.R"))
source(here("3--opportunity-layers.R"))

# ---- allowed opportunity keys ----
OPP_KEYS <- c("baseline__trees",
              "baseline__cool-roofs",
              "trees__all-plantable", 
              "trees__all-pedestrian",
              "cool-roofs__all-roofs",
              "all")

parse_opportunity_keys <- function(x) {
  if (is.null(x) || is.na(x) || !nzchar(x)) return(character(0))
  keys <- unlist(str_split(x, "\\s*,\\s*"))
  keys <- keys[nzchar(keys)]
  bad <- setdiff(keys, OPP_KEYS)
  if (length(bad) > 0) {
    stop(glue(
      "Unknown --opportunity key(s): {paste(bad, collapse = ', ')}.\n",
      "Allowed: {paste(OPP_KEYS, collapse = ', ')}"
    ), call. = FALSE)
  }
  unique(keys)
}

parse_cities <- function(x) {
  # allow comma-separated list OR repeated whitespace accidentally
  cities <- unlist(str_split(x, "\\s*,\\s*"))
  cities <- str_trim(cities)
  cities <- cities[nzchar(cities)]
  unique(cities)
}

extract_openurban_mode <- function(args) {
  idx <- grep("^--openurban\\[[dDgG]+\\]$", args)
  if (length(idx) > 1) {
    stop("Specify only one --openurban[...] flag.", call. = FALSE)
  }
  if (length(idx) == 0) {
    return(list(args = args, mode = NULL))
  }
  
  raw_mode <- sub("^--openurban\\[([dDgG]+)\\]$", "\\1", args[idx])
  chars <- unique(str_split(tolower(raw_mode), "")[[1]])
  bad <- setdiff(chars, c("d", "g"))
  if (length(bad) > 0 || length(chars) == 0) {
    stop("Invalid --openurban[...] mode. Use only d and/or g.", call. = FALSE)
  }
  
  mode <- paste(chars, collapse = "")
  list(args = args[-idx], mode = mode)
}

# ---- CLI options ----
option_list <- list(
  make_option(c("-c", "--city"), type = "character",
              help = "One or more city keys, comma-separated. e.g. 'NLD-Rotterdam' or 'USA-Phoenix,BRA-Teresina'"),
  # make_options(c("--add_urban_extent"), action = "store_true", default = FALSE,
  #              help = "Add urban extent to CIF"),
  make_option(c("--openurban"), action = "store_true", default = FALSE,
              help = "Run OpenUrban default mode (equivalent to --openurban[dg]). Also supported: --openurban[d], --openurban[g], --openurban[dg]"),
  make_option(c("--opportunity"), type = "character", default = NULL,
              help = "Comma-separated opportunity layers: trees__all-trees,cool-roofs__all-roofs"),
  make_option(c("--fail-fast"), action = "store_true", default = FALSE,
              help = "Stop immediately if any city fails (default: keep going)")
)

parser <- OptionParser(option_list = option_list)
raw_args <- commandArgs(trailingOnly = TRUE)
openurban_mode_info <- extract_openurban_mode(raw_args)
opt <- parse_args(parser, args = openurban_mode_info$args)

openurban_requested <- isTRUE(opt$openurban) || !is.null(openurban_mode_info$mode)
openurban_mode <- if (is.null(openurban_mode_info$mode)) "dg" else openurban_mode_info$mode
openurban_download <- openurban_requested && str_detect(openurban_mode, "d")
openurban_generate <- openurban_requested && str_detect(openurban_mode, "g")

if (is.null(opt$city) || !nzchar(opt$city)) {
  print_help(parser)
  stop("\n--city is required.", call. = FALSE)
}

cities <- parse_cities(opt$city)
opp_keys <- parse_opportunity_keys(opt$opportunity)

if (!openurban_requested && length(opp_keys) == 0) {
  print_help(parser)
  stop("\nNothing to do: specify --openurban and/or --opportunity ...", call. = FALSE)
}

message("Cities: ", paste(cities, collapse = ", "))
# message("Add urban extent: ", opts$add_urban_extent)
message("Run OpenUrban: ", openurban_requested)
if (openurban_requested) {
  message("OpenUrban mode: ", openurban_mode,
          " (download=", openurban_download, ", generate=", openurban_generate, ")")
}
message("Opportunity keys: ", if (length(opp_keys) == 0) "(none)" else paste(opp_keys, collapse = ", "))
message("Fail fast: ", opt$`fail-fast`)

results <- vector("list", length(cities))
names(results) <- cities
failures <- list()

for (city in cities) {
  message("\n==============================")
  message("CITY: ", city)
  message("==============================")
  
  out <- tryCatch({
    
    # if (isTRUE(opt$add_urban_extent)) {
    #   message("==> Adding urban extent to CIF...")
    #   add_urban_extent_cif(city)
    #   message("==> Urban extent available.")
    # }
    
    if (isTRUE(openurban_requested)) {
      message("==> Running OpenUrban...")
      generate_openurban_city(
        city = city,
        download_data = openurban_download,
        generate_tiles = openurban_generate
      )
      message("==> OpenUrban step complete.")
    }
    
    if (length(opp_keys) > 0) {
      message("==> Running opportunity workflow...")
      run_city_opportunity(
        city = city,
        write_keys = opp_keys
      )
      message("==> Opportunity layers complete.")
    }
    
    list(ok = TRUE)
    
  }, error = function(e) {
    msg <- conditionMessage(e)
    message("!! FAILED: ", city)
    message("!! ERROR: ", msg)
    trace <- paste(utils::capture.output(traceback(2)), collapse = "\n")
    failures[[city]] <<- list(
      message = msg,
      traceback = trace,
      time = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
    )
    
    if (isTRUE(opt$`fail-fast`)) stop(e)
    list(ok = FALSE, error = msg)
  })
  
  results[[city]] <- out
}

message("\n==============================")
message("SUMMARY")
message("==============================")

ok_cities  <- names(results)[vapply(results, function(x) isTRUE(x$ok), logical(1))]
bad_cities <- names(results)[!vapply(results, function(x) isTRUE(x$ok), logical(1))]

message("Succeeded: ", if (length(ok_cities) == 0) "(none)" else paste(ok_cities, collapse = ", "))
message("Failed: ",    if (length(bad_cities) == 0) "(none)" else paste(bad_cities, collapse = ", "))

if (length(bad_cities) > 0) {
  message("\nFailure details:")
  for (ct in bad_cities) {
    info <- failures[[ct]]
    if (is.list(info)) {
      message("- ", ct, ": ", info$message)
    } else {
      message("- ", ct, ": ", info)
    }
  }
}

# -------------------------
# Write per-city error logs (persistent: here("logs"))
# -------------------------
log_dir <- here("logs")
dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)
ts <- format(Sys.time(), "%Y%m%d_%H%M%S")

if (length(bad_cities) == 0) {
  message("No failures; no per-city error logs written.")
} else {
  for (ct in bad_cities) {
    info <- failures[[ct]]
    
    safe_city <- gsub("[^A-Za-z0-9._-]+", "_", ct)
    log_path <- file.path(log_dir, paste0("ERROR_", safe_city, "_", ts, ".log"))
    
    con <- file(log_path, open = "wt")
    on.exit(try(close(con), silent = TRUE), add = TRUE)
    
    writeLines(glue("Run timestamp: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}"), con)
    writeLines(glue("City: {ct}"), con)
    writeLines(glue("Run OpenUrban: {openurban_requested}"), con)
    if (isTRUE(openurban_requested)) {
      writeLines(glue("OpenUrban mode: {openurban_mode} (download={openurban_download}, generate={openurban_generate})"), con)
    }
    writeLines(glue("Opportunity keys: {if (length(opp_keys)==0) '(none)' else paste(opp_keys, collapse=', ')}"), con)
    writeLines(glue("Fail fast: {opt$`fail-fast`}"), con)
    writeLines(strrep("-", 60), con)
    
    if (is.list(info)) {
      writeLines(glue("Failure time: {info$time}"), con)
      writeLines(glue("Error: {info$message}"), con)
      if (!is.null(info$traceback) && nzchar(info$traceback)) {
        writeLines("\nTraceback:", con)
        writeLines(info$traceback, con)
      }
    } else {
      writeLines(glue("Error: {info}"), con)
    }
    
    close(con)
    message("Wrote error log: ", normalizePath(log_path, winslash = "/", mustWork = FALSE))
  }
}

# -------------------------
# Exit status
# -------------------------
status <- if (length(bad_cities) > 0) 1 else 0

# -------------------------
# Always shutdown if flag present
# -------------------------
term_flag <- tolower(Sys.getenv("EC2_TERMINATE_ON_COMPLETE", "false"))
if (term_flag %in% c("true", "1", "yes")) {
  message("EC2_TERMINATE_ON_COMPLETE set; shutting down now...")
  system("sudo -n shutdown -h now || sudo -n poweroff || true")
}

quit(status = status)
