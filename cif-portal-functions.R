# ============================================================
# Setup ####
# ============================================================

Sys.setenv(PYTHONUNBUFFERED = "1")

s3 <- paws::s3()
bucket <- "wri-cities-tcm"
aws_http <- "https://wri-cities-tcm.s3.us-east-1.amazonaws.com"

source(here("utils", "write-s3.R"))
source(here("utils", "open-urban-helpers.R"))

add_urban_extent_cif <- function(city) {
  
  # check if exists tree, alb, worldpop, openurban
  
  args <- c(
    "run","--no-capture-output",
    "-n","open-urban",
    "python","-u","get_data.py", city
  )
  run_python_live(args)
  
  
  
}