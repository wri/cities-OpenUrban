library(processx)

# ============================================================
# Python helper (PTY streaming)
# ============================================================

run_python_live <- function(args,
                            conda = "/home/ubuntu/miniconda3/condabin/conda") {
  
  p <- processx::process$new(
    conda,
    args = args,
    stdout = NULL,
    stderr = NULL,
    pty = TRUE
  )
  
  safe_read <- function() {
    tryCatch(
      p$read_output(),
      error = function(e) ""
    )
  }
  
  while (p$is_alive()) {
    txt <- safe_read()
    if (nzchar(txt)) {
      cat(txt)
      flush.console()
    }
    Sys.sleep(0.1)
  }
  
  txt <- safe_read()
  if (nzchar(txt)) cat(txt)
  
  invisible(p$get_exit_status())
}


# ============================================================
# S3 existence check (CLI)
# ============================================================

s3_keys_exist_by_prefix <- function(s3, bucket, keys) {
  prefixes <- unique(sub("([^/]+)$", "", keys))
  present <- character(0)
  
  for (pref in prefixes) {
    token <- NULL
    repeat {
      resp <- s3$list_objects_v2(
        Bucket = bucket,
        Prefix = pref,
        ContinuationToken = token
      )
      
      if (!is.null(resp$Contents)) {
        present <- c(present, vapply(resp$Contents, `[[`, character(1), "Key"))
      }
      
      if (isTRUE(resp$IsTruncated)) {
        token <- resp$NextContinuationToken
      } else {
        break
      }
    }
  }
  
  present <- unique(present)
  missing <- setdiff(keys, present)
  
  list(ok = length(missing) == 0, missing = missing)
}

openurban_expected_s3_keys <- function(city, grid_ids) {
  c(
    glue("OpenUrban/{city}/open_space/open_space_all.parquet"),
    glue("OpenUrban/{city}/water/water_all.parquet"),
    glue("OpenUrban/{city}/roads/roads_all.parquet"),
    glue("OpenUrban/{city}/parking/parking_all.parquet"),
    glue("OpenUrban/{city}/roads/average_lanes.csv"),
    glue("OpenUrban/{city}/city_grid/city_grid.geojson"),
    glue("OpenUrban/{city}/buildings/buildings_{grid_ids}.parquet"),
    glue("OpenUrban/{city}/OpenUrban/{city}_{grid_ids}.tif")
  ) |> as.character()
}

# ============================================================
# GEE existence check (Python API)
# ============================================================

gee_tiles_exist <- function(city, grid_ids, collection_id, env_name) {
  
  pyfile <- tempfile(fileext = ".py")
  
  writeLines(c(
    "import sys, ee",
    "ee.Initialize()",
    "city = sys.argv[1]",
    "collection = sys.argv[2]",
    "expected = set(sys.argv[3].split(','))",
    "col = ee.ImageCollection(collection).filter(ee.Filter.eq('city', city))",
    "ids = col.aggregate_array('system:id').getInfo()",
    "found = set(i.split('/')[-1] for i in ids)",
    "missing = expected - found",
    "print('OK' if not missing else 'MISSING')",
    "for m in sorted(missing): print(m)"
  ), pyfile)
  
  args <- c(
    "run", "--no-capture-output",
    "-n", env_name,
    "python", "-u", pyfile,
    city,
    collection_id,
    paste0(city, "_", grid_ids, collapse = ",")
  )
  
  out <- system2("/home/ubuntu/miniconda3/condabin/conda",
                 args = args,
                 stdout = TRUE,
                 stderr = TRUE)
  
  list(ok = any(grepl("^OK$", out)), raw = out)
}