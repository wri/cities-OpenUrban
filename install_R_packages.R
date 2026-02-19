# install_R_packages.R
# Installs lightweight CRAN packages that are not managed by conda in environment.yml.
# Run after: conda activate open-urban

cran_pkgs <- c(
  "glue",
  "here",
  "optparse",
  "stringr",
  "geoarrow",
  "sfarrow",
  "paws",
  "processx",
  "tidyverse",
  "dplyr",
  "fs",
  "readr",
  "tidyr",
  "reticulate"
)

# Base packages (utils, stats, tools) are part of R itself; do not install.
# Also NOT included here because conda provides them:
# s2, sf, lwgeom, arrow, terra, raster, exactextractr, sfarrow

installed <- rownames(installed.packages())
to_install <- setdiff(cran_pkgs, installed)

cat("CRAN packages requested:\n")
print(cran_pkgs)

cat("\nAlready installed:\n")
print(intersect(cran_pkgs, installed))

if (length(to_install) == 0) {
  cat("\nNothing to install.\n")
  quit(status = 0)
}

cat("\nInstalling:\n")
print(to_install)

install.packages(
  to_install,
  repos = "https://cloud.r-project.org"
)

cat("\nDone.\n")
