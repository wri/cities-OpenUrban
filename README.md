# 🌍 cities-OpenUrban

Internal repository for generating **OpenUrban land use/land cover (LULC)** data and associated **heat-resilient infrastructure opportunity layers**.

This repository supports:

- Generation of OpenUrban LULC datasets
- Export to Google Earth Engine (GEE) and AWS S3
- Generation of tree and cool roof opportunity layers
- Batch processing of multiple cities via CLI

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
conda env create -f environment.yml
conda activate open-urban
```

If the environment already exists:

```bash
conda env update -f environment.yml
```

The conda environment is used for CIF integration, Earth Engine utilities, and S3 workflows and also installs some R packages. Make sure to do this before restoring the Renv.

> This repository does not use `reticulate`.

---

## 🟢 2. R Environment (renv)

From the root of the repository:

```bash
Rscript -e 'install.packages("renv")'
Rscript -e 'renv::restore()'
```

This installs all required R packages using the versions recorded in `renv.lock`.

To verify:

```bash
Rscript -e 'renv::status()'
```

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

- `UrbanExtents`

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

---

## 🌳 Generate Opportunity Layers

### Required CIF datasets

- `UrbanExtents`
- `WorldPop`
- `OpenUrban`
- `AlbedoCloudMasked__ZonalStats_median__NumSeasons_3` (dates specified in filename)
- `TreeCanopyHeight` (height threshold = 3 meters)

> ⚠ If OpenUrban was just generated, you must publish it to CIF (via publish_layers.py in the [cities-cif-portal](https://github.com/wri/cities-cif-portal/tree/main) repository) before generating opportunity layers.

---

### Run all opportunity layers

```bash
Rscript 1--OpenUrban-workflow.R \
  --city BRA-Rio_de_Janeiro,IND-Bhopal,MEX-Mexico_City \
  --opportunity all
```

---

### Run specific opportunity layers

```bash
Rscript 1--OpenUrban-workflow.R \
  --city USA-Atlanta \
  --opportunity trees__all-plantable,cool-roofs__all-roofs
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
