library(here)
library(googleCloudStorageR)
library(reticulate)
library(sf)
library(terra)
library(tidyverse)
library(glue)


# ======================================================================
# Environment setup & permissions
# ======================================================================

# --- Python environment -------------------------------------------------
use_condaenv("chri", required = TRUE)

# --- Google Cloud Storage ----------------------------------------------
Sys.setenv(
  GOOGLE_APPLICATION_CREDENTIALS = "/Users/elizabeth.wesley/Documents/keys/nifty-memory-399115-7aa7f647b0b0.json",
  GOOGLE_CLOUD_PROJECT           = "nifty-memory-399115",
  GCS_DEFAULT_BUCKET             = "wri-cities-lulc-gee"
)

# --- AWS credentials ----------------------------------------------------
# Log in via AWS SSO (opens browser)
system("aws sso login --profile cities-data-dev")

# Add credentials to environment
Sys.setenv(
  AWS_SDK_LOAD_CONFIG = "1",
  AWS_PROFILE         = "cities-data-dev",
  AWS_DEFAULT_REGION  = "us-east-1"
)



# ======================================================================
# City configuration
# ======================================================================

city      <- "Cape_Town"
country   <- "ZAF"
city_name <- glue("{country}-{city}")



# ======================================================================
# Generate data from GEE (Python module)
# ======================================================================

# Import the Python module
script_dir <- here("opportunity-layers")
sys <- import("sys", convert = FALSE)
sys$path$append(script_dir)

og <- import("opportunity_grids")

# Run the Python function for this city
og$run_city(
  city      = city,
  country   = country,
  data_path = here()
)



# ======================================================================
# Process data
# ======================================================================

# --- Load tabular opportunity data from AWS ----------------------------
dat <- read_csv(
  glue("https://wri-cities-heat.s3.us-east-1.amazonaws.com/OpenUrban/{city_name}/opportunity/tree-albedo-grid.csv"),
  col_types = cols(
    gid = col_character()
  )
)

# --- Get projection from data ------------------------------------------
proj <- dat %>% 
  pull(crs) 

# --- Load grid and reproject -------------------------------------------
grid <- st_read(glue("https://wri-cities-heat.s3.us-east-1.amazonaws.com/OpenUrban/{city_name}/opportunity/worldpop_grid_100m.geojson")) %>% 
  st_transform(proj)

# LULC class labels
lulc_codes <- c("Green space (other)",
                "Built up (other)",
                "Barren",
                "Public open space",
                "Parking",
                "Roads",
                "Buildings")

# Plantable LULC classes (codes)
plantable <- c(110, 120, 130, 200, 400)

# --- Add plantable flag, LULC factor, city & area in hectares ----------
dat <- dat %>% 
  mutate(
    plantable = ifelse(lulc %in% plantable, TRUE, FALSE),
    lulc      = factor(lulc, levels = c(110, 120, 130, 200, 400, 500, 600), labels = lulc_codes),
    city      = city_name,
    area_ha   = area / 10000
  ) 

# --- Achievable tree-cover targets per plantable LULC ------------------
percentiles <- dat %>% 
  filter(plantable, tree_pct > 0) %>% 
  mutate(
    prob = case_when(
      lulc == "Green space (other)" ~ 0.50,
      lulc == "Built up (other)"    ~ 0.30,
      lulc == "Barren"              ~ 0.30,
      lulc == "Parking"             ~ 0.30,
      lulc == "Public open space"   ~ 0.75,
      TRUE                          ~ NA_real_
    )
  ) %>%
  group_by(lulc) %>%
  summarise(
    target  = quantile(tree_pct, probs = first(prob), na.rm = TRUE),
    .groups = "drop"
  )

# --- Calculate tree areas, plantable area & albedo potentials ----------
dat <- dat %>% 
  left_join(percentiles, by = c("lulc")) %>% 
  mutate(target = replace_na(target, 0)) %>% 
  mutate(
    tree_area_ha       = area_ha * tree_pct,
    plantable_area_ha  = (area_ha * (1 - tree_pct)) * plantable,
    target_area_ha     = if_else(tree_area_ha > plantable_area_ha * target, tree_area_ha, plantable_area_ha * target),
    delta_tree_area_ha = target_area_ha - tree_area_ha,
    alb_potential      = case_when(
      lulc == "Buildings" & albedo < 0.62 ~ 0.62,
      .default                              = albedo
    )
  ) %>% 
  group_by(city, gid) %>% 
  summarize(
    tree_area_ha       = sum(tree_area_ha),
    plantable_area_ha  = sum(plantable_area_ha),
    delta_tree_area_ha = sum(delta_tree_area_ha),
    total_area_ha      = sum(area_ha),
    alb_existing       = sum(albedo * area_ha) / total_area_ha,
    alb_achievable     = sum(alb_potential * area_ha) / total_area_ha
  ) %>% 
  mutate(
    tree_pct_existing      = tree_area_ha / total_area_ha,
    pct_plantable          = plantable_area_ha / total_area_ha,
    tree_area_ha_achievable = tree_area_ha + delta_tree_area_ha,
    tree_pct_achievable    = tree_area_ha_achievable / total_area_ha,
    delta_tree_pct         = tree_pct_achievable - tree_pct_existing,
    delta_alb              = alb_achievable - alb_existing
  ) %>% 
  ungroup() 

# --- Attach summarised metrics back to grid -----------------------------
grid <- grid %>% 
  filter(gid %in% dat$gid) %>% 
  left_join(dat, by = "gid")

# --- Save grid with tree & albedo opportunity metrics ------------------
source(here("utils", "write-to-s3.R"))
write_s3(
  grid, 
  out_file_path = glue("wri-cities-heat/OpenUrban/{city_name}/opportunity/opportunity-grid.geojson")
)



# ======================================================================
# Rasterize tree and albedo opportunity
# ======================================================================

v <- vect(grid)

# Template raster: 100 m x 100 m over grid extent
tmpl <- rast(v, resolution = 100, crs = crs(v))
origin(tmpl) <- c(ext(v)$xmin, ext(v)$ymin)

# --- Tree opportunity (Δ tree cover) -----------------------------------
tree_zones <- rasterize(v, tmpl, field = "delta_tree_pct", touches = FALSE, background = NA)
write_s3(
  tree_zones, 
  out_file_path = glue("wri-cities-heat/OpenUrban/{city_name}/opportunity/tree_opportunity.tif")
)

# --- Cool roof / albedo opportunity (Δ albedo) -------------------------
alb_zones <- rasterize(v, tmpl, field = "delta_alb", touches = FALSE, background = NA)
write_s3(
  alb_zones, 
  out_file_path = glue("wri-cities-heat/OpenUrban/{city_name}/opportunity/cool-roof_opportunity.tif")
)
