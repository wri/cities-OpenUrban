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
```
conda env create -f environment.yml
conda activate open-urban
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


Disregard all else.
