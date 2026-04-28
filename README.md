# 🌍 cities-OpenUrban

Repository for generating **OpenUrban land use/land cover (LULC)** data and associated **heat-resilient infrastructure opportunity layers**.

This repository supports:

- Generation of OpenUrban LULC datasets
- Export to Google Earth Engine (GEE) and AWS S3
- Generation of tree and cool roof opportunity layers
- Batch processing of multiple cities via CLI

# About

OpenUrban is a high-resolution (1 m) LULC dataset with categories designed to be specifically relevant to the implementation of heat-resilient infrastructure, validated at 93 percent accuracy in the United States and 83 percent globally (16 cities across eight world regions and a range of city sizes). We chose these LULC categories to map the types of features (roads, parking lots, buildings, water, and public open spaces) that are modifiable for the purposes of heat mitigation. Additionally, we include three generic categories (green space (other), built-up (other), and barren) to fill the gaps between features of interest and ensure continuous coverage over urban areas. We use only free and globally available data—the spatial extent of the datasets covers all land areas worldwide—so that our methods can be implemented for any city, regardless of local data availability. Where available, OpenUrban could be supplemented with higher-resolution or locally sourced data to potentially improve accuracy. The OpenUrban dataset is continually expanding as we add coverage of more cities. These data serve as the foundation for generating scenarios, providing the spatial detail needed to identify where different forms of heat-resilient infrastructure can realistically be implemented. 

Excerpt from (**Mapping Scenarios and Estimating the Potential for Heat-Resilient Infrastructure in Cities**[https://www.wri.org/research/mapping-scenarios-and-estimating-potential-heat-resilient-infrastructure-cities], Wesley et al. 2026)

Data can be explored further at: [https://wri-datalab.earthengine.app/view/open-urban].
Public Earth Engine asset at: [https://code.earthengine.google.com/?asset=projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC] 

| Category | Subcategory | Sub-subcategory | Code |
|---|---|---|---|
| Green space (other) | | | 110 |
| Built up (other) | | | 120 |
| Barren | | | 130 |
| Public open space | | | 200 |
| Water | | | 300 |
| Parking | | | 400 |
| Roads | | | 500 |
| Building | Unclassified | | 600 |
| | | Low-slope | 601 |
| | | High-slope | 602 |
| | Residential | | 610 |
| | | Low-slope | 611 |
| | | High-slope | 612 |
| | Non-residential | | 620 |
| | | Low-slope | 621 |
| | | High-slope | 622 |

---

# 📁 Repository Structure

| Script | Purpose |
|--------|----------|
| `OpenUrban-workflow.R` | Main CLI entrypoint for running OpenUrban and/or opportunity layers |
| `OpenUrban-generation.R` | Generates OpenUrban LULC and uploads to GEE + S3 |
| `opportunity-layers.R` | Generates opportunity layers and uploads to S3 |

---

# ⚙️ Environment Setup

This project requires:

- R (≥ 4.2 recommended)
- Conda
- AWS + GCP authentication

All setup can be completed from the command line — no interactive R or Python sessions required.

---

## 1. Python Environment (Conda)

Create the conda environment:

```bash
conda env create -f open-urban.yml
conda activate open-urban
```

If the environment already exists:

```bash
conda env update -f open-urban.yml
```

The conda environment is used for CIF integration, Earth Engine utilities, and S3 workflows and also installs some R packages. 

> This repository does not use `reticulate`.

---

## 🟢 2. R Environment 

From the root of the repository, with the open-urban conda environment active:

```bash
Rscript install_R_packages.R

```

This installs all required R packages.




---


# 🔐 Authentication

> On EC2 instances with appropriate IAM roles, this step is not required.

---

## AWS

```bash
export AWS_PROFILE=cities-data-dev
aws sso login
aws s3 ls
```

---

## Google Cloud / Earth Engine

```bash
gcloud auth application-default login
gcloud config set project citiesindicators
gcloud auth application-default set-quota-project citiesindicators
```

You must also have Earth Engine access enabled for your account.

---

# 🚀 Running the Workflow

All commands should be run from the root of the repository.

---

## 🏙 Generate OpenUrban

### Required CIF datasets

- `UrbanExtents`, read from the `wri-cities-indicators` bucket in s3.

Run:

```bash
Rscript 1--OpenUrban-workflow.R \
  --city USA-Phoenix,BRA-Teresina \
  --openurban
```

Notes:

- Cities must use the standard `{ISO3}-{City_Name}` format
- Separate multiple cities with commas (no spaces)
- If processing fails for a city, execution continues to the next
- If you already have the data downloaded and just need to generate the tiles, use the flag `--openurban[g]`

### Download Only Selected Raw Layers

Use `get_data.py` when you want to fetch a subset of source data instead of running the full OpenUrban workflow.

Download only buildings for one city and merge them into `buildings_all.parquet`:

```bash
python get_data.py USA-Boston --layers buildings --skip-s3-upload
```

Download only roads for one city:

```bash
python get_data.py USA-Boston --layers roads --skip-s3-upload
```

---

## 🌳 Generate Opportunity Layers

Currently, the opportunity layers can **only** be generated for an urban extent. All layers are read from the `wri-cities-indicators` bucket in s3.

### Required CIF datasets

- `UrbanExtents`
- `WorldPop`
- `OpenUrban`
- `AlbedoCloudMasked__ZonalStats_median__NumSeasons_3` (dates specified in filename)
- `TreeCanopyHeight` (height threshold >= 3 meters)

> ⚠ If OpenUrban was just generated, you must publish it to CIF (via publish_layers.py in the [cities-cif-portal](https://github.com/wri/cities-cif-portal/tree/main) repository) before generating opportunity layers.

---

### Run all opportunity layers

```bash
Rscript 1--OpenUrban-workflow.R \
  --city BRA-Rio_de_Janeiro,IND-Bhopal,MEX-Mexico_City \
  --opportunity all
```

---

### Available Opportunity Options

| Option | Definition |
|--------|------------|
| `baseline__trees` | Baseline tree cover |
| `baseline__cool-roofs` | Baseline albedo |
| `trees__all-plantable` | Achievable tree cover applied to all plantable surfaces |
| `trees__all-pedestrian` | Achievable tree cover applied to street right-of-ways only |
| `cool-roofs__all-roofs` | Cool roof opportunity applied to all buildings |
| `all` | All scenarios |

---

### Run specific opportunity layers

In rare cases it may be necessary to generate a single opportunity layer for a city. This is possible via:

```bash
Rscript 1--OpenUrban-workflow.R \
  --city USA-Atlanta \
  --opportunity trees__all-plantable,cool-roofs__all-roofs
```

---

# 📦 Outputs

### OpenUrban LULC
- Exported to Google Earth Engine
- Uploaded to AWS S3

### Opportunity Layers
- Uploaded to AWS S3

Refer to log files for details on any failed opportunity layers including which
layers may be missing from CIF.

---

---

# 🖥 Running Long Jobs on EC2

OpenUrban and opportunity layer generation can run for hours depending on city size.  
When running on EC2, you should execute jobs in a way that survives SSH disconnects.

You have two supported options: `screen` (recommended) or `nohup`.

---

## Option 1: Using `screen` (Recommended)

`screen` keeps a persistent terminal session running even if you disconnect.

### Start a screen session

```bash
screen -S openurban
```

You are now inside a persistent session.

Run your workflow:

```bash
Rscript 1--OpenUrban-workflow.R \
  --city USA-Phoenix \
  --openurban
```

### Detach from the session (leave it running)

Press:

```
Ctrl + A, then D
```

You will return to your normal shell and the process will continue running.

### Reattach later

```bash
screen -ls
screen -r openurban
```

---

## Option 2: Using `nohup`

`nohup` runs a command in the background and writes output to a log file.

```bash
nohup Rscript 1--OpenUrban-workflow.R \
  --city USA-Phoenix \
  --openurban \
  > openurban.log 2>&1 &
```

- Output will be written to `openurban.log`
- The job will continue running after you disconnect

To monitor progress:

```bash
tail -f openurban.log
```

---

## Automatically Terminate EC2 on Completion

If running on EC2 and you want the instance to shut down automatically when the job completes, set the following environment variable:

```bash
EC2_TERMINATE_ON_COMPLETE=true \
Rscript 1--OpenUrban-workflow.R \
  --city USA-Phoenix \
  --openurban \
```

```bash
nohup env EC2_TERMINATE_ON_COMPLETE=true \
Rscript 1--OpenUrban-workflow.R \
  --city USA-Phoenix \
  --openurban \
  > openurban.log 2>&1 &
```

When the workflow finishes, the EC2 instance will terminate automatically.

> ⚠ Use this flag only when you are confident the job is configured correctly.

---

## Notes

- `screen` is recommended for interactive monitoring.
- `nohup` is useful for fire-and-forget runs.
- Large cities may require high-memory EC2 instances. These workflows have been
tested successfully on a `r7iz.2xlarge` instance. 
- Always verify authentication before starting long jobs.

---

# 🛠 Troubleshooting

### R issues

```bash
Rscript -e 'renv::restore()'
```

### Conda issues

```bash
conda env remove -n open-urban
conda env create -f environment.yml
```

### Authentication errors

- Verify AWS profile and SSO login
- Confirm active GCP project
- Confirm Earth Engine access

---

# 🧭 Notes

- Processing can be compute-intensive.
- Failed cities will not stop batch execution.
- Ensure required CIF layers are published before generating dependent outputs.
