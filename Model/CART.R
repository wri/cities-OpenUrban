

# READ ME -----------------------------------------------------------------



# This code trains and saves the classification model for roof slope to be
# applied in the LULC classification.

library(rpart)
library(rpart.plot)
library(tidymodels)
library(caret)
library(here)
library(tidyverse)
library(sf)


# V1 ----------------------------------------------------------------------


# Read buildings data -----------------------------------------------------
build.samp <- st_read(here("Model", "buildings-sample.geojson")) %>%
  rename(ULU.old = ULU) %>% 
  select(! Area_m)

# Read ANBH
anbh <- rast(here("data", "Los_Angeles", "anbh.tif"))

# Reproject buildings to match ANBH
build.samp <- build.samp %>% 
  st_transform(crs = st_crs(anbh))

# Extract average of pixel values to buildings
# Mean value of cells that intersect the polygon, 
# weighted by the fraction of the cell that is covered.
avg_ht_samp <- exactextractr::exact_extract(anbh, build.samp, 'mean')

build.samp <- build.samp %>% 
  add_column(ANBH = avg_ht_samp)

# Read ULU
ulu <- rast(here("data", "Los_Angeles", "ULU_5m.tif"))

# Reproject buildings to match ULU
build.samp <- build.samp %>% 
  st_transform(crs = st_crs(ulu))

# Extract values to buildings using exact_extract, as coverage fractions
# https://github.com/isciences/exactextractr
build_ulu_samp <- exactextractr::exact_extract(ulu, build.samp, 'frac') 

build_ulu_samp <- build_ulu_samp %>% 
  mutate(ID = row_number()) %>% 
  pivot_longer(cols = starts_with("frac"), 
               names_to = "ULU", 
               values_to = "coverage") %>% 
  mutate(ULU = as.integer(str_sub(ULU, start = -1))) %>% 
  group_by(ID) %>% 
  arrange(coverage, .by_group = TRUE) %>% 
  summarise(ULU = case_when(
    # When the majority class is not roads (3), return the majority class
    last(ULU) != 3 ~ last(ULU),
    # When the majority class is roads, return the second majority class
    last(ULU) == 3 & nth(coverage, -2L) != 0 ~ nth(ULU, -2L),
    # When the majority ULU is roads (3) & there is no minority class, use ULU 1 (non-res)
    last(ULU) == 3 & nth(coverage, -2L) == 0 ~ 1)) %>% 
  ungroup() 

build.samp <- build.samp %>% 
  add_column(ULU = build_ulu_samp$ULU)

# Reproject to local state plane and calculate area
build.samp <- build.samp %>% 
  st_transform(epsg) %>% 
  mutate(Area_m = as.numeric(st_area(.)))

###################
#### using R to extract max values from ULU results in some different values
##### changes tree
##### revisit
##### for now I'm using the old ULU values because I like the simple
##### intuitiveness of the tree
##### need to revist classification model

build.samp2 <- build.samp %>% 
  select(! ULU) %>%
  mutate(ULU = str_sub(ULU.old, -1L),
         ULU = case_when(ULU == 2 ~ 1,
                         ULU == 5 ~ 2),
         ULU = as_factor(ULU),
         Area_m = as.numeric(Area_m)) %>%
  filter(Area_m < 40000 & !is.na(Slope))  

# Split data into 70% for training and 30% for testing
set.seed(5511)
build_split <- build.samp2 %>% 
  initial_split(prop = 0.70)


# Extract the data in each split
build_train <- training(build_split)
build_test <- testing(build_split)

tree <- rpart(Slope ~ Area_m + ANBH + ULU, data = build_train, method = "class")
rpart.plot(tree)

saveRDS(tree, here("data", "model", "tree.rds"))


# V2 ----------------------------------------------------------------------

builds <- geojsonsf::geojson_sf(here("Model", "buildings-sample-tiers.geojson")) %>% 
  filter(!is.na(Slope)) %>% 
  # Convert area to meters
  # The areas were calculated in local state plane projs (ft) before
  # transforming to EPSG 4326
  ######### This is not correct
  ######### should be Area_m = Area_ft / 10.764
  mutate(Area_m = Area_ft / 3.281,
         ULU = as_factor(ULU),
         Slope = as_factor(Slope)) %>% 
  rename(ANBH = mean) %>% 
  select(Area_m, ULU, ANBH, Slope, sample_tier, strata) 

model_builds <- builds %>% 
  filter(sample_tier <= 4)

# Split data into 70% for training and 30% for testing
set.seed(5511)
build_split <- model_builds %>% 
  initial_split(prop = 0.70)


# Extract the data in each split
build_train <- training(build_split) %>% 
  mutate(Model = "training")
build_test <- testing(build_split) %>% 
  mutate(Model = "testing")

# Build tree
tree <- rpart(Slope ~ Area_m + ANBH + ULU, data = build_train, method = "class")
rpart.plot(tree)

# Prediction
# test data
build_test <- build_test %>% 
  mutate(pred = predict(tree, build_test, type = "class"))
# trainging data
build_train <- build_train %>% 
  mutate(pred = predict(tree, build_train, type = "class"))

# frequency table of prediction and actual, test data
test_table <- table(build_test$pred, build_test$Slope)

accuracy(test_table)
precision(test_table)
recall(test_table)

# frequency table of prediction and actual, test and training data
model_dat <- build_test %>% 
  bind_rows(build_train)

full_table <- table(model_dat$pred, model_dat$Slope)

accuracy(full_table)
precision(full_table)
recall(full_table)

model_dat %>% 
  pivot_longer(cols = c(Slope, pred)) %>% 
  ggplot() +
  geom_bar(aes(name, fill = value)) +
    facet_wrap(~ strata)

model_dat %>% 
  select(1, 2, 3, 4, 8, 7, 6, 5) %>% 
st_write(here("data", "V2-building-class-data.geojson"))

# Testing with additional buildings

test_builds <- builds %>% 
  filter(sample_tier %in% 5:6)

test_builds <- test_builds %>% 
  mutate(pred = predict(tree, test_builds, type = "class"))

test_table <- table(test_builds$pred, test_builds$Slope)

accuracy(test_table)
precision(test_table)
recall(test_table)

write_rds(tree, here("data", "V2-building-class-tree.rds"))
# tree <- read_rds(here("data", "V2-building-class-tree.rds"))

# # Los Angeles
# la.builds <- st_read(here("Model", "LA-buildings.shp"))
# 
# library(rgee)
# ee_Initialize("ejwesley", drive = TRUE, gcs = TRUE)
# 
# la.builds.ee <- sf_as_ee(la.builds, proj = "EPSG:26945")
# 
# bb <- st_bbox(la.builds)
# bb_ee <- sf_as_ee(st_as_sfc(bb))
# 
# anbh <- ee$ImageCollection("projects/wri-datalab/GHSL/GHS-BUILT-H-ANBH_R2023A")$
#   filterBounds(bb_ee)$ 
#   select('b1')$
#   mosaic()$
#   clip(bb_ee)
# 
# ulu <- ee$ImageCollection('projects/wri-datalab/cities/urban_land_use/V1')$ 
#   filterBounds(bb_ee)$
#   select('lulc')$
#   reduce(ee$Reducer$firstNonNull())$
#   rename('lulc')
# 
# FROM = c(0, 1, 2, 3, 4, 5)
# TO = c(0, 1, 2, 2, 2, 2)
# 
# ulu = ulu$remap(FROM, TO)
# 
# combo = ulu$addBands(anbh)
# 
# la.build.zs = combo$reduceRegions(
#   reducer = ee$Reducer$frequencyHistogram()$combine(reducer2 = ee$Reducer$mean(), sharedInputs = FALSE),
#   collection = la.builds.ee,
#   scale = 5
# )
# 
# la.builds <- ee_as_sf(la.build.zs) %>% 
#   select(-c(ULU, building)) %>% 
#   separate_longer_delim(histogram, delim = ",") %>% 
#   separate_wider_delim(histogram, delim = ":", names = c("ULU", "Freq"), too_few = "align_start") %>% 
#   filter(Slope != "NA") %>% 
#   mutate(across(c(ULU, Freq), ~ str_remove_all(.x, '["\\{\\}\\ ]')),
#          ULU = na_if(ULU, ""),
#          ULU = replace_na(ULU, "0"),
#          ULU = as_factor(ULU),
#          Slope = as_factor(Slope)) %>% 
#   group_by(ID) %>% 
#   slice_max(Freq) %>% 
#   filter(n() == 1) %>% 
#   rename(ANBH = mean) %>% 
#   select(- Freq) %>%
#   ungroup() %>% 
#   filter(ULU != "0",
#          Slope != "NA")
# 
# la_build_pred <- tibble(pred = predict(tree, la.builds, type = "class"),
#             actual = la.builds$Slope) %>% 
#   mutate(actual = as_factor(actual),
#          actual = fct_relevel(actual, "high"))
# 
# la.table <- table(la_build_pred)
# 
# accuracy(la.table)
# precision(la.table)
# recall(la.table)
