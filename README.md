# cities-heat

The current files are:

* Global methods: [LULC_grid_global_V3.R](LULC_grid_global_V3.R)
* U.S. methods: [LULC_grid_SSC.R](LULC_grid_SSC.R)
* Slope classification: [USA-OpenUrban-slope-class-tree.rds](USA-OpenUrban-slope-class-tree.rds)


Setup
R packages
```
geojsonio
```

# Get data
## Setup
### Python environment
```
conda env create -f environment.yml
conda activate open-urban
```
### AWS + GEE authentication
NOTE: On ec2 instance you don't need to do this step.
```
export AWS_PROFILE=cities-data-dev
aws sso login
aws s3 ls # test you are logged in
gcloud auth application-default login
gcloud config set project citiesindicators
gcloud auth application-default set-quota-project citiesindicators
```

Run the `get_data.py` script to fetch the data for a specific city.

```bash
python get_data.py
```

Note: GEE uses `nearest-neighbor` when resample by default.  https://developers.google.com/earth-engine/apidocs/ee-image-resample

## Upload to GEE
### Requirements
* [uv](https://docs.astral.sh/uv/getting-started/installation/) has to be installed
* You need to have `GOOGLE_APPLICATION_USER` and `GOOGLE_APPLICATION_CREDENTIALS` in your `~/.Renviron` or project's `.Renviron` file.

### Usage
* Run `./upload_gee.py --help` to see the available options.
* Call from R using...
```
library(here)
Sys.getenv(c("GOOGLE_APPLICATION_USER","GOOGLE_APPLICATION_CREDENTIALS"))
  
gee_args <- c(
"--local-file", local_file,
"--collection-id", "projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC",
"--city_name", city_name,
"--gridcell_id", gridcell_id,
"--version", version,
"--overwrite"
)

system2(here("./upload_gee.py"), args = gee_args)
```



```bash

Disregard all else.
