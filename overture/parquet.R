library(sfarrow)
library(dplyr, warn.conflicts = FALSE)

box-buildings <- st_read_parquet(system.file("./box-buildings.parquet", package = "sfarrow"))
