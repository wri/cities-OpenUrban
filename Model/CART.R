

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


# Read buildings data -----------------------------------------------------
build.samp <- st_read(here("data", "Model", "buildings-sample.geojson")) %>%
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

