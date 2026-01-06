#!/usr/bin/env uv run
# /// script
# dependencies = [
#   "click",
#   "google-cloud-storage",
# ]
# ///
import os
import time
import subprocess
import click
from google.cloud import storage
from google.api_core import exceptions as gcs_exceptions

gee_project = "citiesindicators"

def setup_gee_gcs(gcs_bucket_name):
    service_account_email = os.getenv("GOOGLE_APPLICATION_USER", "")
    service_account_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not service_account_email:
        raise click.ClickException("GOOGLE_APPLICATION_USER is required.")
    if not service_account_json:
        raise click.ClickException("GOOGLE_APPLICATION_CREDENTIALS is required.")
    service_account_json = os.path.expanduser(service_account_json)
    if not os.path.isfile(service_account_json):
        raise click.ClickException(
            f"GOOGLE_APPLICATION_CREDENTIALS not found: {service_account_json}"
        )

    storage_client = storage.Client.from_service_account_json(service_account_json)
    
    EE_CMD = ["uvx", "--from", "earthengine-api", "earthengine",
    "--service_account_file", os.path.expanduser(service_account_json),
    "--project", gee_project]

    return storage_client.bucket(gcs_bucket_name), EE_CMD


def upload_to_gcs_until_success(bucket, local_path, blob_name):
    """Retries upload until successful"""
    blob = bucket.blob(blob_name)
    attempt = 1
    wait = 2
    
    while True:
        try:
            print(f"⬆️  Uploading to gs://{bucket.name}/{blob_name} (attempt {attempt})...")
            # Simple upload for smaller files, use resumable for very large ones
            blob.upload_from_filename(local_path)
            print(f"✅ Upload succeeded on attempt {attempt}")
            return f"gs://{bucket.name}/{blob_name}"
        except Exception as e:
            print(f"❌ Attempt {attempt} failed: {e}")
            print(f"⏳ Retrying in {wait}s...")
            time.sleep(wait)
            attempt += 1
            wait = min(wait * 1.5, 30)


def upload_gee_image(
    ee_cmd,
    bucket,
    local_path,
    asset_id,
    properties=None,
    overwrite=False,
    wait=None,
):
    if not overwrite:
        check_cmd = ee_cmd + ["asset", "info", asset_id]
        check_result = subprocess.run(check_cmd, capture_output=True, text=True)
        if check_result.returncode == 0:
            raise click.ClickException(
                "Asset already exists. Use --overwrite to replace it:\n"
                f"{asset_id}"
            )
    gs_uri = upload_to_gcs_until_success(bucket, local_path, os.path.basename(local_path))
    cmd = ee_cmd + ["upload", "image", "--asset_id", asset_id]
    for key, value in (properties or {}).items():
        cmd += ["--property", f"{key}={value}"]
    if overwrite:
        cmd.append("--force")
    if wait is not None:
        cmd += ["--wait", str(wait)]
    cmd.append(gs_uri)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise click.ClickException(
            "Earth Engine upload failed:\n"
            f"{result.stdout}\n{result.stderr}".strip()
        )
    return result.stdout.strip() or "Upload started."


@click.command(
    context_settings={"max_content_width": 120},
    epilog=(
        "\b\n"
        "Example:\n"
        "  uv run upload_gee.py \\\n"
        "    --gcs-bucket wri-cities-gee-imports \\\n"
        "    --city-name example_city \\\n"
        "    --gridcell-id 12345 \\\n"
        "    --version v1 \\\n"
        "    --local-file data/test-upload/LULC.tif \\\n"
        "    --collection-id projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC"
    ),
)
@click.option("--gcs-bucket", default="wri-cities-gee-imports", show_default=True, help="GCS bucket name")
@click.option("--city-name", default="example_city", show_default=True, help="City name property")
@click.option("--gridcell-id", default="12347", show_default=True, help="Grid cell ID property")
@click.option("--version", default="v1", show_default=True, help="Version property")
@click.option("--local-file", default="data/test-upload/LULC.tif", show_default=True, help="Local raster path")
@click.option(
    "--collection-id",
    default="projects/wri-datalab/chrowe/test-upload2",
    show_default=True,
    help="GEE image collection ID",
)
@click.option("--overwrite", is_flag=True, default=False, show_default=True, help="Overwrite existing asset")
@click.option(
    "--wait",
    default=None,
    type=int,
    help="Wait for the task to finish (seconds).",
)
def main(
    gcs_bucket,
    city_name,
    gridcell_id,
    version,
    local_file,
    collection_id,
    overwrite,
    wait,
):

    if not os.path.isfile(local_file):
        raise click.ClickException(f"Local file not found: {local_file}")

    bucket, EE_CMD = setup_gee_gcs(gcs_bucket)

    image_id = f"{collection_id}/{city_name}_{gridcell_id}"
    properties = {
        '(string)city': city_name,
        '(number)grid_cell': gridcell_id,
        '(string)version': version,
        '(string)start_time': time.strftime("%Y-%m-%d"),
    }
    task_id = upload_gee_image(
        EE_CMD,
        bucket,
        local_file,
        image_id,
        properties=properties,
        overwrite=overwrite,
        wait=wait,
    )

    print(f"🚀 Upload initiated. Task ID: {task_id}")
    print("Check status at: https://code.earthengine.google.com/tasks")


if __name__ == "__main__":
    main()
