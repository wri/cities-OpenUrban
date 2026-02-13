# opportunity_workflow.R
# End-to-end workflow:
# - load CIF rasters (WorldPop, OpenUrban LULC, Albedo, Tree Canopy Height) + urban extent
# - compute (gid, lulc) zonal stats on the LULC grid using terra (raster-native)
# - compute tree_opportunity (delta_tree_pct) + cool_roof_opportunity (delta_alb) per WorldPop cell
# - write rasters that MATCH the WorldPop grid exactly

library(sf)
library(terra)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(glue)
library(here)
library(fs)

source(here("utils", "write-s3.R"))

# -------------------------
# LULC code sets (authoritative)
# -------------------------
L_GREEN   <- 110L
L_BUILT   <- 120L
L_BARREN  <- 130L
L_PARKS   <- 200L
L_WATER   <- 300L
L_PARKING <- 400L
L_ROADS   <- 500L
L_PED     <- 700L

# Buildings (hierarchy)
# Non-U.S. cities only have parent building codes: 600, 610, 620 (no slope)
# U.S. cities have additional slope sub-classes
B_PARENT_NONUS <- c(600L, 610L, 620L)

# U.S. rule 
# low slope buildings: (600, 601, 611, 620, 621)
# high slope buildings: (602, 610, 612, 622)
B_LOW_US  <- c(600L, 601L, 611L, 620L, 621L)
B_HIGH_US <- c(602L, 610L, 612L, 622L)

# Plantable LULC codes 
PLANTABLE_CODES <- c(L_GREEN, L_BUILT, L_BARREN, L_PARKS, L_PARKING, L_PED)

is_us_city <- function(city_key) {
  str_starts(city_key, "USA-")
}

# Achievable target quantile by LULC label
lulc_prob <- function(lulc_label) {
  dplyr::case_when(
    lulc_label == "Green space (other)" ~ 0.50,
    lulc_label == "Built up (other)"    ~ 0.30,
    lulc_label == "Barren"              ~ 0.30,
    lulc_label == "Parking"             ~ 0.30,
    lulc_label == "Public open space"   ~ 0.90,
    lulc_label == "Pedestrian right-of-way" ~ 0.90,
    TRUE                                ~ NA_real_
  )
}

# Cool roofs updated albedo:
# - Non-US: buildings (600/610/620) => target 0.62, apply only if baseline < target
# - US: low slope => 0.62, high slope => 0.28, apply only if baseline < target
make_updated_albedo <- function(city, lulc_int, alb,
                                target_low = 0.62,
                                target_high = 0.28) {
  us <- is_us_city(city)
  target <- alb
  
  if (us) {
    target[lulc_int %in% B_LOW_US]  <- target_low
    target[lulc_int %in% B_HIGH_US] <- target_high
  } else {
    target[lulc_int %in% B_PARENT_NONUS] <- target_low
  }
  
  updated <- app(c(alb, target), fun = max, na.rm = TRUE)  # keep existing if already higher
  names(updated) <- "updatedAlb"
  updated
}

# Normalize by percentile rank
normalize_percentile <- function(r, probs = seq(0, 1, by = 0.01)) {
  
  qs <- quantile(values(r), probs = probs, na.rm = TRUE)
  
  classify(
    r,
    rcl = cbind(qs[-length(qs)], qs[-1], probs[-1]),
    include.lowest = TRUE
  )
}

# Categorize into 5 levels
cat5_from_01 <- function(x) {
  
  # convert 0–1 to 1–5
  y <- ceiling(x * 5)
  
  # handle edge case where x == 0
  y[y == 0] <- 1
  
  y
}

# Function to load and merge rasters from a list of paths
load_and_merge <- function(paths) {
  if (length(paths) == 0) {
    stop("No raster paths provided")
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
  
  # keep only .tif tiles
  is_tile <- grepl("tile_\\d+\\.tif$", lines)
  
  # drop failed ones
  is_ok <- !grepl("processing_failed", lines)
  
  # extract filenames
  tiles_ok <- sub(".*\\s", "", lines[is_tile & is_ok])
}

rast_retry <- function(path, attempts = 6, base_sleep = 0.5, quiet = FALSE) {
  last_err <- NULL
  
  for (i in seq_len(attempts)) {
    r <- tryCatch(
      terra::rast(path),
      error = function(e) { last_err <<- e; NULL }
    )
    
    if (!is.null(r)) return(r)
    
    # exponential-ish backoff with jitter
    sleep <- base_sleep * (1.6 ^ (i - 1)) + stats::runif(1, 0, 0.25)
    if (!quiet) message(sprintf("rast() failed (%d/%d). Retrying in %.2fs: %s", i, attempts, sleep, path))
    Sys.sleep(sleep)
  }
  
  stop(sprintf("[rast_retry] Failed after %d attempts:\n%s\n%s",
               attempts, path, conditionMessage(last_err)), call. = FALSE)
}

# Find albedo folder
find_city_dataset_folder <- function(s3_parent, city, dataset_stub, profile = "cities-data-dev") {
  # s3_parent example:
  # "s3://wri-cities-indicators/data/published/layers/AlbedoCloudMasked__ZonalStats_median__NumSeasons_3/tif/"
  s3_parent <- sub("/?$", "/", s3_parent)
  
  # list "folders" under parent
  lines <- system2(
    "aws",
    c("s3", "ls", s3_parent, "--profile", profile),
    stdout = TRUE,
    stderr = TRUE
  )
  
  # folder lines look like: "                           PRE <name>/"
  dirs <- sub(".*PRE\\s+", "", lines[grepl("\\sPRE\\s", lines)])
  dirs <- sub("/$", "", dirs)
  
  # match: "{city}__urban_extent__{dataset_stub}__Start..._End....tif"
  # dates can vary; allow anything between dataset stub and ".tif"
  pat <- paste0("^", stringr::fixed(city), "__urban_extent__", stringr::fixed(dataset_stub), ".*\\.tif$")
  matches <- dirs[grepl(pat, dirs)]
  
  if (length(matches) == 0) {
    stop("No matching folder found under ", s3_parent, " for city=", city, " stub=", dataset_stub, call. = FALSE)
  }
  
  # Prefer most recent by parsing StartDate/EndDate if present; otherwise take last lexicographically
  # (Lexicographic works well if dates are YYYY-MM-DD)
  matches <- sort(matches)
  
  # if multiple, take the last (usually latest date range)
  matches[length(matches)]
}


# Terra options
pick_terra_tempdir <- function(
    candidates = c("/mnt/terra", "/mnt/tmp/terra", "~/terra_tmp", file.path(tempdir(), "terra"))
) {
  # expand ~
  candidates <- path_expand(candidates)
  
  for (p in candidates) {
    # try create + write test
    ok <- tryCatch({
      dir_create(p, recurse = TRUE)
      testfile <- file.path(p, paste0(".terra_write_test_", Sys.getpid()))
      writeLines("ok", testfile)
      unlink(testfile)
      TRUE
    }, error = function(e) FALSE)
    
    if (isTRUE(ok)) return(p)
  }
  
  # absolute last resort (should basically never happen)
  tempdir()
}

set_terra_options_auto <- function(memfrac = 0.85, progress = 3) {
  td <- pick_terra_tempdir()
  terraOptions(memfrac = memfrac, progress = progress, tempdir = td)
  message("terra tempdir: ", td)
  invisible(td)
}

auto_memfrac <- function() {
  # conservative on laptops, aggressive on big servers
  if (dir.exists("/mnt")) 0.9 else 0.75
}


# -------------------------
# Main function
# -------------------------
run_city_opportunity <- function(
    city,
    cif_bucket = "wri-cities-indicators",
    cif_prefix = "data/published/layers",
    write_keys = c("trees__all-plantable", "cool-roofs__all-roofs", "trees__pedestrian",
                   "baseline__trees", "baseline__cool-roofs"),
    # Inputs: CIF layer names/patterns
    urban_extent_path = NULL,  # if NULL, uses standard CIF path for 2020 extents
    worldpop_path     = NULL,
    lulc_path         = NULL,
    albedo_path       = NULL,
    treeheight_path   = NULL,
    # Method knobs
    cool_roof_target_low = 0.62,
    cool_roof_target_high = 0.28,
    terra_tempdir = "/tmp/terra",
    terra_memfrac = 0.8
) {
  
  set_terra_options_auto(memfrac = auto_memfrac(), progress = 3)
  write_keys <- str_to_lower(write_keys)
  
  requested_all <- "all" %in% write_keys
  
  request_tree_family <- requested_all || any(write_keys %in% c("tree", "trees"))
  request_cool_family <- requested_all || any(write_keys %in% c("cool-roofs", "cool_roofs", "coolroof", "coolroofs"))
  request_baseline_family <- requested_all || any(write_keys == "baseline")
  
  request_tree_all_plantable <- requested_all ||
    request_tree_family ||
    any(write_keys %in% c(
      "trees__all-plantable",
      "opportunity__trees__all-plantable"
    ))
  
  request_tree_pedestrian <- requested_all ||
    request_tree_family ||
    any(write_keys %in% c(
      "trees__pedestrian",
      "opportunity__trees__pedestrian"
    ))
  
  request_tree_baseline <- requested_all ||
    request_tree_family ||
    request_baseline_family ||
    any(write_keys %in% c("baseline__trees", "trees__baseline"))
  
  request_cool_roof_opportunity <- requested_all ||
    request_cool_family ||
    any(write_keys %in% c(
      "cool-roofs__all-roofs",
      "opportunity__cool-roofs__all-roofs",
      "cool_roofs__all_roofs"
    ))
  
  request_cool_roof_baseline <- requested_all ||
    request_cool_family ||
    request_baseline_family ||
    any(write_keys %in% c(
      "baseline__cool-roofs",
      "cool-roofs__baseline",
      "baseline__cool_roofs"
    ))
  
  request_tree_any_opportunity <- request_tree_all_plantable || request_tree_pedestrian
  
  need_tree <- request_tree_any_opportunity || request_tree_baseline
  need_cool <- request_cool_roof_opportunity || request_cool_roof_baseline
  
  if (!need_tree && !need_cool) {
    stop(
      "No recognized write_keys requested. ",
      "Use one or more of: all, tree/trees, cool-roofs, baseline, ",
      "trees__all-plantable, trees__pedestrian, baseline__trees, ",
      "cool-roofs__all-roofs, baseline__cool-roofs."
    )
  }
  
  cif_aws_http <- glue("https://{cif_bucket}.s3.us-east-1.amazonaws.com")
  
  # -------- Paths (defaults) --------
  if (is.null(urban_extent_path)) {
    urban_extent_path <- glue(
      "{cif_aws_http}/{cif_prefix}/UrbanExtents/geojson/",
      "{city}__urban_extent__UrbanExtents__StartYear_2020_EndYear_2020.geojson"
    )
    
    urban_extent <- st_read(urban_extent_path, quiet = TRUE) %>% 
      vect()
  }
  if (is.null(worldpop_path)) {
    worldpop_path <- glue(
      "{cif_aws_http}/{cif_prefix}/WorldPop/tif/",
      "{city}__urban_extent__WorldPop__StartYear_2020_EndYear_2020.tif"
    )
  }
  if (is.null(lulc_path)) {
    lulc_grid <- st_read(glue(
      "{cif_aws_http}/{cif_prefix}/OpenUrban/tif/",
      "{city}__urban_extent__OpenUrban.tif/fishnet_grid.json"
    )) %>% st_filter(st_as_sf(urban_extent))
    
    lulc_tiles <- list_tiles(glue("s3://wri-cities-indicators/{cif_prefix}/OpenUrban/tif/",
                                  "{city}__urban_extent__OpenUrban.tif/"))
    
    lulc_tiles <- lulc_tiles[which(str_remove(lulc_tiles, ".tif") %in% lulc_grid$tile_name)]

    lulc_paths <- glue(
      "{cif_aws_http}/{cif_prefix}/OpenUrban/tif/",
      "{city}__urban_extent__OpenUrban.tif/{lulc_tiles}"
    )
    
    if (length(lulc_tiles) != nrow(lulc_grid)) {
      stop(glue("Missing OpenUrban tiles in ", 
                "s3://wri-cities-indicators/{cif_prefix}/OpenUrban/tif/",
                 "{city}__urban_extent__OpenUrban.tif/"))
    }
  }

  if (need_cool && is.null(albedo_path)) {
    
    s3_parent <- glue("s3://wri-cities-indicators/{cif_prefix}/AlbedoCloudMasked__ZonalStats_median__NumSeasons_3/tif/")
    
    folder_name <- find_city_dataset_folder(
      s3_parent = s3_parent,
      city = city,
      dataset_stub = "AlbedoCloudMasked__ZonalStats_median__NumSeasons_3",
      profile = "cities-data-dev"   
    )
    
    # Now list tiles inside the discovered folder
    albedo_tiles <- list_tiles(glue("{s3_parent}{folder_name}/"), profile = "cities-data-dev")
    albedo_tiles <- albedo_tiles[which(str_remove(albedo_tiles, ".tif") %in% lulc_grid$tile_name)]
    
    albedo_paths <- glue(
      "{cif_aws_http}/{cif_prefix}/AlbedoCloudMasked__ZonalStats_median__NumSeasons_3/tif/",
      "{folder_name}/{albedo_tiles}"
    )
    
    if (length(albedo_tiles) != nrow(lulc_grid)) {
      stop(glue("Missing albedo tiles in ", 
                "s3://wri-cities-indicators/{cif_prefix}/AlbedoCloudMasked__ZonalStats_median__NumSeasons_3",
                "/tif/{folder_name}"))
    }

  }
  
  if (need_tree && is.null(treeheight_path)) {
    tree_tiles <- list_tiles(glue("s3://wri-cities-indicators/{cif_prefix}/TreeCanopyHeight/tif/",
                                  "{city}__urban_extent__TreeCanopyHeight__Height_3.tif/"))
    
    tree_tiles <- tree_tiles[which(str_remove(tree_tiles, ".tif") %in% lulc_grid$tile_name)]
    
    treeheight_paths <- glue(
      "{cif_aws_http}/{cif_prefix}/TreeCanopyHeight/tif/",
      "{city}__urban_extent__TreeCanopyHeight__Height_3.tif/{tree_tiles}"
    )
    
    if (length(tree_tiles) != nrow(lulc_grid)) {
      stop(glue("Missing tree canopy tiles in ", 
                "s3://wri-cities-indicators/{cif_prefix}/TreeCanopyHeight/tif/",
                "{city}__urban_extent__TreeCanopyHeight__Height_3.tif/"))
    }

  }
  
  # -------- Load data --------
  
  # -------- WorldPop grid --------
  print("Processing worldpop ...")
  # Create a 100m WorldPop grid as polygons, give each cell a stable gid
  wp <- rast(worldpop_path)
  wp[is.na(wp)] <- 1
  wp <- wp %>% mask(urban_extent)
  wp_cells <- as.polygons(wp, values = FALSE, na.rm = TRUE, aggregate = FALSE) 
  wp_cells$gid <- seq_len(nrow(wp_cells))
  
  wp_cells <- st_as_sf(wp_cells)
  
  # Batch the zonal statistics to avoid loading the entire raster
  tile_grid <- wp_cells %>%
    st_join(lulc_grid %>% select(tile_name), join = st_intersects, left = TRUE) %>% 
    st_drop_geometry()
  
  wp_cells <- wp_cells %>% 
    full_join(tile_grid)
  
  # Divide wp_cells into batches
  process_batch <- function(id) {
    
    on.exit({
      try(terra::tmpFiles(remove = TRUE), silent = TRUE)
      gc()
    }, add = TRUE)
    
    print(glue("Processing batch for {id}"))
    
    wp_cells_batch <- wp_cells %>% 
      filter(tile_name == id)
    
    bb <- st_as_sf(st_as_sfc(st_bbox(wp_cells_batch)))
    
    tile <- lulc_grid |> 
      filter(tile_name == id)
    
    idx_touch <- st_touches(lulc_grid, tile, sparse = FALSE)[, 1]
    
    tiles_touching <- lulc_grid %>%
      filter(tile_name == id | idx_touch) |> 
      st_filter(bb) |> 
      pull(tile_name)
    
    # Load data
    print("Loading rasters...")
    lulc    <- load_and_merge(str_subset(lulc_paths, str_c(tiles_touching, collapse = "|"))) |> 
      crop(bb)
    
    if (need_cool) {
      alb <- load_and_merge(str_subset(albedo_paths, str_c(tiles_touching, collapse = "|"))) |>
        crop(bb)
      alb <- resample(alb, lulc, method = "bilinear")
    }
    
    if (need_tree) {
      tree_h <- load_and_merge(str_subset(treeheight_paths, str_c(tiles_touching, collapse = "|"))) |>
        crop(bb)
      tree <- as.numeric(!is.na(tree_h))
    }
    
    # Reproject the *vector* grid into the LULC CRS 
    wp_cells_lulc_batch <- st_transform(wp_cells_batch, st_crs(lulc))
    
    # Rasterize gid onto the LULC grid (zone id only)
    gid_on_lulc <- rasterize(wp_cells_lulc_batch, lulc, field = "gid", touches = FALSE)
    names(gid_on_lulc) <- "gid"
    
    lulc <- lulc %>% mask(gid_on_lulc)
    if (need_cool) {
      alb <- alb %>% mask(gid_on_lulc)
    }
    if (need_tree) {
      tree <- tree %>% mask(gid_on_lulc)
    }
    
    # Zonal stats
    print("Calculating zonal stats...")
    # -------- Work on LULC grid (analysis grid) --------
    # Pixel area on the LULC grid (m^2)
    area <- cellSize(lulc, unit = "m")
    names(area) <- "area_m2"
    
    # Mask water (LULC 300)
    lulc_int <- as.int(lulc)
    lulc_int <- mask(lulc_int, lulc_int, maskvalues = 300)
    
    if (request_tree_any_opportunity) {
      # Create pedestrian right-of-way from buffered roads
      roads <- lulc_int == 500
      k <- matrix(1, nrow = 11, ncol = 11)
      roads_dilated <- focal(roads, w = k, fun = "max", na.rm = TRUE)
      ped_area <- ifel(roads_dilated == 1 & roads == 0, 1, 0)
      lulc_int[ped_area == 1 & lulc_int != 500] <- 700L
    }
    
    if (request_cool_roof_opportunity) {
      updatedAlb <- make_updated_albedo(
        city = city,
        lulc_int = lulc_int,
        alb = alb,
        target_low  = cool_roof_target_low,
        target_high = cool_roof_target_high
      )
    }
    
    # -------- Zone id = gid*K + lulc --------
    K <- 1000L
    zone <- gid_on_lulc * K + lulc_int
    names(zone) <- "zone"
    
    # -------- Zonal stats by (gid,lulc) --------
    mean_layers <- list()
    if (need_tree) {
      mean_layers <- c(mean_layers, list(tree = tree))
    }
    if (need_cool) {
      mean_layers <- c(mean_layers, list(albedo = alb))
    }
    if (request_cool_roof_opportunity) {
      mean_layers <- c(mean_layers, list(updatedAlb = updatedAlb))
    }
    
    mean_stack <- rast(mean_layers)
    m <- zonal(mean_stack, zone, fun = "mean", na.rm = TRUE)
    a <- zonal(area, zone, fun = "sum",  na.rm = TRUE)
    
    stats_long <- as.data.frame(m) |>
      left_join(as.data.frame(a), by = "zone") |>
      mutate(
        gid  = zone %/% K,
        lulc_code = zone %% K,
        plantable = lulc_code %in% PLANTABLE_CODES
      )
    
    if ("tree" %in% names(stats_long)) {
      stats_long <- stats_long |> rename(tree_pct = tree)
    } else {
      stats_long$tree_pct <- NA_real_
    }
    if (!"albedo" %in% names(stats_long)) {
      stats_long$albedo <- NA_real_
    }
    if (!"updatedAlb" %in% names(stats_long)) {
      stats_long$updatedAlb <- NA_real_
    }
    
    stats_long <- stats_long |>
      select(gid, lulc_code, plantable, tree_pct, albedo, updatedAlb, area_m2)
    
    # Label LULC for the percentile logic (only needs the plantable classes)
    # (We only label what we use; everything else can remain NA for lulc_label)
    if (request_tree_any_opportunity) {
      stats_long <- stats_long |>
        mutate(
          lulc_label = case_when(
            lulc_code == 110L ~ "Green space (other)",
            lulc_code == 120L ~ "Built up (other)",
            lulc_code == 130L ~ "Barren",
            lulc_code == 200L ~ "Public open space",
            lulc_code == 400L ~ "Parking",
            lulc_code == 700L ~ "Pedestrian right-of-way",
            TRUE              ~ NA_character_
          )
        )
    }
    
    # Join to wp_cells
    # wp_cells_batch <- st_as_sf(wp_cells_batch) %>% 
    #   left_join(stats_long, by = "gid")
    
    return(stats_long)
  }

  tile_ids <- unique(tile_grid$tile_name)
  tile_ids <- tile_ids[!is.na(tile_ids)]
  full_stats <- bind_rows(lapply(tile_ids, process_batch))
  
  gid_stats <- full_stats |>
    group_by(gid) |>
    summarise(
      total_area_m2 = sum(area_m2, na.rm = TRUE),
      .groups = "drop"
    )
  
  if (need_tree) {
    tree_base <- full_stats |>
      mutate(tree_area_m2 = area_m2 * tree_pct) |>
      group_by(gid) |>
      summarise(
        tree_area_m2 = sum(tree_area_m2, na.rm = TRUE),
        .groups = "drop"
      )
    
    gid_stats <- gid_stats |>
      left_join(tree_base, by = "gid") |>
      mutate(
        tree_pct_existing = if_else(total_area_m2 > 0, tree_area_m2 / total_area_m2, NA_real_)
      )
  }
  
  if (request_tree_any_opportunity) {
    percentiles <- full_stats |>
      filter(plantable, tree_pct > 0, !is.na(lulc_label)) |>
      mutate(prob = lulc_prob(lulc_label)) |>
      group_by(lulc_label) |>
      summarise(
        target = quantile(tree_pct, probs = first(prob), na.rm = TRUE),
        .groups = "drop"
      )
    
    tree_opp <- full_stats |>
      left_join(percentiles, by = "lulc_label") |>
      mutate(target = replace_na(target, 0)) |>
      mutate(
        tree_area_m2       = area_m2 * tree_pct,
        plantable_area_m2  = (area_m2 * (1 - tree_pct)) * plantable,
        target_area_m2     = if_else(
          tree_area_m2 > plantable_area_m2 * target,
          tree_area_m2,
          plantable_area_m2 * target
        ),
        delta_tree_area_m2 = target_area_m2 - tree_area_m2
      ) |>
      group_by(gid) |>
      summarise(
        delta_tree_area_m2 = sum(delta_tree_area_m2, na.rm = TRUE),
        delta_street_tree_area_m2 = sum(delta_tree_area_m2[lulc_code == 700], na.rm = TRUE),
        .groups = "drop"
      )
    
    gid_stats <- gid_stats |>
      left_join(tree_opp, by = "gid") |>
      mutate(
        tree_area_m2_achievable = tree_area_m2 + delta_tree_area_m2,
        tree_pct_achievable = if_else(total_area_m2 > 0, tree_area_m2_achievable / total_area_m2, NA_real_),
        delta_tree_pct = tree_pct_achievable - tree_pct_existing,
        street_tree_area_m2_achievable = tree_area_m2 + delta_street_tree_area_m2,
        street_tree_pct_achievable = if_else(total_area_m2 > 0, street_tree_area_m2_achievable / total_area_m2, NA_real_),
        delta_street_tree_pct = street_tree_pct_achievable - tree_pct_existing
      )
  }
  
  if (need_cool) {
    cool_base <- full_stats |>
      group_by(gid) |>
      summarise(
        alb_existing_n = sum(albedo * area_m2, na.rm = TRUE),
        area_for_alb_m2 = sum(area_m2, na.rm = TRUE),
        .groups = "drop"
      ) |>
      mutate(alb_existing = if_else(area_for_alb_m2 > 0, alb_existing_n / area_for_alb_m2, NA_real_)) |>
      select(gid, alb_existing)
    
    gid_stats <- gid_stats |>
      left_join(cool_base, by = "gid")
  }
  
  if (request_cool_roof_opportunity) {
    cool_opp <- full_stats |>
      group_by(gid) |>
      summarise(
        alb_achievable_n = sum(updatedAlb * area_m2, na.rm = TRUE),
        area_for_alb_m2 = sum(area_m2, na.rm = TRUE),
        .groups = "drop"
      ) |>
      mutate(alb_achievable = if_else(area_for_alb_m2 > 0, alb_achievable_n / area_for_alb_m2, NA_real_)) |>
      select(gid, alb_achievable)
    
    gid_stats <- gid_stats |>
      left_join(cool_opp, by = "gid") |>
      mutate(delta_alb = pmax(alb_achievable - alb_existing, 0))
  }
  
  cols_to_scale <- intersect(
    c(
      "tree_pct_existing",
      "tree_pct_achievable",
      "delta_tree_pct",
      "street_tree_pct_achievable",
      "delta_street_tree_pct",
      "alb_existing",
      "alb_achievable",
      "delta_alb"
    ),
    names(gid_stats)
  )
  
  if (length(cols_to_scale) > 0) {
    gid_stats <- gid_stats |>
      mutate(across(all_of(cols_to_scale), ~ .x * 100))
  }
  
  # -------- Burn back to rasters that match WorldPop EXACTLY --------
  wp_cells <- wp_cells |> 
    left_join(gid_stats, by = "gid")
  
  outputs <- list(full_stats = gid_stats)
  
  if (request_tree_all_plantable) {
    tree_opportunity <- rasterize(wp_cells, wp, "delta_tree_pct")
    names(tree_opportunity) <- "tree_opportunity"
    stopifnot(compareGeom(tree_opportunity, wp, stopOnError = FALSE))
    
    write_s3(
      tree_opportunity,
      glue("wri-cities-tcm/OpenUrban/{city}/opportunity-layers/opportunity__trees__all-plantable.tif")
    )
    
    tree_opportunity_cat <- normalize_percentile(tree_opportunity) %>% cat5_from_01()
    write_s3(
      tree_opportunity_cat,
      glue("wri-cities-tcm/OpenUrban/{city}/opportunity-layers/opportunity-CAT__trees__all-plantable__.tif")
    )
    
    outputs$tree_opportunity <- tree_opportunity
  }
  
  if (request_tree_pedestrian) {
    street_tree_opportunity <- rasterize(wp_cells, wp, "delta_street_tree_pct")
    names(street_tree_opportunity) <- "street_tree_opportunity"
    
    write_s3(
      street_tree_opportunity,
      glue("wri-cities-tcm/OpenUrban/{city}/opportunity-layers/opportunity__trees__pedestrian.tif")
    )
    
    outputs$street_tree_opportunity <- street_tree_opportunity
  }
  
  if (request_tree_baseline) {
    tree_baseline <- rasterize(wp_cells, wp, "tree_pct_existing")
    names(tree_baseline) <- "tree_baseline"
    
    write_s3(
      tree_baseline,
      glue("wri-cities-tcm/OpenUrban/{city}/opportunity-layers/baseline__trees.tif")
    )
    
    outputs$tree_baseline <- tree_baseline
  }
  
  if (request_cool_roof_opportunity) {
    cool_roof_opportunity <- rasterize(wp_cells, wp, "delta_alb")
    names(cool_roof_opportunity) <- "cool_roof_opportunity"
    stopifnot(compareGeom(cool_roof_opportunity, wp, stopOnError = FALSE))
    
    write_s3(
      cool_roof_opportunity,
      glue("wri-cities-tcm/OpenUrban/{city}/opportunity-layers/opportunity__cool-roofs__all-roofs.tif")
    )
    
    cool_roof_opportunity_cat <- normalize_percentile(cool_roof_opportunity) %>% cat5_from_01()
    write_s3(
      cool_roof_opportunity_cat,
      glue("wri-cities-tcm/OpenUrban/{city}/opportunity-layers/opportunity-CAT__cool-roofs__all-roofs.tif")
    )
    
    outputs$cool_roof_opportunity <- cool_roof_opportunity
  }
  
  if (request_cool_roof_baseline) {
    cool_roof_baseline <- rasterize(wp_cells, wp, "alb_existing")
    names(cool_roof_baseline) <- "cool_roof_baseline"
    
    write_s3(
      cool_roof_baseline,
      glue("wri-cities-tcm/OpenUrban/{city}/opportunity-layers/baseline__cool-roofs.tif")
    )
    
    outputs$cool_roof_baseline <- cool_roof_baseline
  }
  
  invisible(outputs)
}
