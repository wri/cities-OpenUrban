#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse)
  library(stringr)
  library(glue)
  library(here)
})

# ---- source workflows ----
source(here("OpenUrban-generation.R"))
source(here("opportunity-layers.R"))

# ---- allowed opportunity keys ----
OPP_KEYS <- c("trees__all-trees", "cool-roofs__all-roofs")

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

# ---- CLI options ----
option_list <- list(
  make_option(c("-c", "--city"), type = "character",
              help = "One or more city keys, comma-separated. e.g. 'NLD-Rotterdam' or 'USA-Phoenix,BRA-Teresina'"),
  # make_options(c("--add_urban_extent"), action = "store_true", default = FALSE,
  #              help = "Add urban extent to CIF"),
  make_option(c("--openurban"), action = "store_true", default = FALSE,
              help = "Generate OpenUrban LULC and upload to S3/GEE"),
  make_option(c("--opportunity"), type = "character", default = NULL,
              help = "Comma-separated opportunity layers: trees__all-trees,cool-roofs__all-roofs"),
  make_option(c("--fail-fast"), action = "store_true", default = FALSE,
              help = "Stop immediately if any city fails (default: keep going)")
)

parser <- OptionParser(option_list = option_list)
opt <- parse_args(parser)

if (is.null(opt$city) || !nzchar(opt$city)) {
  print_help(parser)
  stop("\n--city is required.", call. = FALSE)
}

cities <- parse_cities(opt$city)
opp_keys <- parse_opportunity_keys(opt$opportunity)

if (!opt$openurban && length(opp_keys) == 0) {
  print_help(parser)
  stop("\nNothing to do: specify --openurban and/or --opportunity ...", call. = FALSE)
}

message("Cities: ", paste(cities, collapse = ", "))
# message("Add urban extent: ", opts$add_urban_extent)
message("Run OpenUrban: ", opt$openurban)
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
    
    if (isTRUE(opt$openurban)) {
      message("==> Generating OpenUrban...")
      generate_openurban_city(city)
      message("==> OpenUrban saved to s3 and GEE.")
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
    writeLines(glue("Run OpenUrban: {opt$openurban}"), con)
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
