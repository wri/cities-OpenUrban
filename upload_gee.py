#!/usr/bin/env uv run
# /// script
# dependencies = [
#   "click",
#   "google-cloud-storage",
#   "boto3",
# ]
# ///
import os
import time
import subprocess
import click
import boto3
from google.cloud import storage

gee_project = "citiesindicators"


def setup_gee_gcs(gcs_bucket_name: str):
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

    UVX = os.path.expanduser("~/.local/bin/uvx")
    EE_CMD = [
        UVX, "--from", "earthengine-api", "earthengine",
        "--service_account_file", os.path.expanduser(service_account_json),
        "--project", gee_project
    ]

    return storage_client.bucket(gcs_bucket_name), EE_CMD


def upload_local_to_gcs_until_success(gcs_bucket, local_path: str, gcs_blob_name: str) -> str:
    """Retries local->GCS upload until successful."""
    blob = gcs_bucket.blob(gcs_blob_name)
    attempt = 1
    wait_s = 2

    while True:
        try:
            print(f"⬆️  Uploading {local_path} → gs://{gcs_bucket.name}/{gcs_blob_name} (attempt {attempt})...")
            blob.upload_from_filename(local_path)
            print(f"✅ Upload succeeded on attempt {attempt}")
            return f"gs://{gcs_bucket.name}/{gcs_blob_name}"
        except Exception as e:
            print(f"❌ Attempt {attempt} failed: {e}")
            print(f"⏳ Retrying in {wait_s}s...")
            time.sleep(wait_s)
            attempt += 1
            wait_s = min(int(wait_s * 1.5), 30)


def upload_s3_to_gcs_until_success(gcs_bucket, s3_bucket: str, s3_key: str, gcs_blob_name: str) -> str:
    """Retries S3->GCS streaming upload until successful (no local temp file)."""
    s3 = boto3.client("s3")
    blob = gcs_bucket.blob(gcs_blob_name)

    attempt = 1
    wait_s = 2

    while True:
        try:
            print(f"⬆️  Streaming s3://{s3_bucket}/{s3_key} → gs://{gcs_bucket.name}/{gcs_blob_name} (attempt {attempt})...")
            obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            body = obj["Body"]  # streaming file-like object
            # rewind=True lets google-cloud-storage attempt to seek if possible; Body is stream, so it's OK
            blob.upload_from_file(body, rewind=True)
            print(f"✅ Stream upload succeeded on attempt {attempt}")
            return f"gs://{gcs_bucket.name}/{gcs_blob_name}"
        except Exception as e:
            print(f"❌ Attempt {attempt} failed: {e}")
            print(f"⏳ Retrying in {wait_s}s...")
            time.sleep(wait_s)
            attempt += 1
            wait_s = min(int(wait_s * 1.5), 30)


def ee_asset_exists(ee_cmd, asset_id: str) -> bool:
    check_cmd = ee_cmd + ["asset", "info", asset_id]
    check_result = subprocess.run(check_cmd, capture_output=True, text=True)
    return check_result.returncode == 0


def start_ee_upload(
    ee_cmd,
    gs_uri: str,
    asset_id: str,
    properties=None,
    overwrite: bool = False,
    wait: int | None = None,
) -> str:
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
    return (result.stdout.strip() or "Upload started.").strip()


@click.command(
    context_settings={"max_content_width": 120},
    epilog=(
        "\b\n"
        "Examples:\n"
        "  # Upload from local file\n"
        "  uv run upload_gee.py \\\n"
        "    --gcs-bucket wri-cities-gee-imports \\\n"
        "    --gcs-blob-name OpenUrban_LULC/LULC.tif \\\n"
        "    --city-name NLD-Rotterdam --gridcell-id 123 --version global \\\n"
        "    --local-file /path/to/LULC.tif \\\n"
        "    --collection-id projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC \\\n"
        "    --overwrite --wait 600\n"
        "\n"
        "  # Upload from S3 (streaming)\n"
        "  uv run upload_gee.py \\\n"
        "    --gcs-bucket wri-cities-gee-imports \\\n"
        "    --gcs-blob-name OpenUrban_LULC/LULC.tif \\\n"
        "    --city-name NLD-Rotterdam --gridcell-id 123 --version global \\\n"
        "    --s3-bucket wri-cities-tcm \\\n"
        "    --s3-key OpenUrban/NLD-Rotterdam/OpenUrban/NLD-Rotterdam_123.tif \\\n"
        "    --collection-id projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC \\\n"
        "    --overwrite --wait 600\n"
    ),
)
@click.option("--gcs-bucket", default="wri-cities-gee-imports", show_default=True, help="GCS bucket name")
@click.option(
    "--gcs-blob-name",
    default="OpenUrban_LULC/LULC.tif",
    show_default=True,
    help="Destination blob name in GCS. You can reuse/overwrite this each upload if running serially with --wait.",
)
@click.option("--city-name", required=True, help="City name")
@click.option("--gridcell-id", required=True, help="Grid cell ID")
@click.option("--version", required=True, help="Version string (e.g., global / USA)")
@click.option("--local-file", default=None, help="Local raster path (optional if using S3)")
@click.option("--s3-bucket", default=None, help="S3 bucket name (optional if using local-file)")
@click.option("--s3-key", default=None, help="S3 key (optional if using local-file)")
@click.option(
    "--collection-id",
    default="projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC",
    show_default=True,
    help="GEE image collection ID",
)
@click.option("--overwrite", is_flag=True, default=False, show_default=True, help="Overwrite existing asset")
@click.option(
    "--wait",
    default=None,
    type=int,
    help="Wait for the task to finish (seconds). REQUIRED if you reuse the same --gcs-blob-name across tiles.",
)
@click.option(
    "--cleanup-gcs",
    is_flag=True,
    default=False,
    show_default=True,
    help="Delete the staged GCS blob after the EE upload command completes (recommended when using --wait).",
)
def main(
    gcs_bucket,
    gcs_blob_name,
    city_name,
    gridcell_id,
    version,
    local_file,
    s3_bucket,
    s3_key,
    collection_id,
    overwrite,
    wait,
    cleanup_gcs,
):
    # Validate source
    using_local = local_file is not None and local_file != ""
    using_s3 = (s3_bucket is not None and s3_bucket != "") and (s3_key is not None and s3_key != "")

    if using_local and using_s3:
        raise click.ClickException("Provide either --local-file OR (--s3-bucket and --s3-key), not both.")
    if not using_local and not using_s3:
        raise click.ClickException("Provide either --local-file OR (--s3-bucket and --s3-key).")

    if using_local and not os.path.isfile(local_file):
        raise click.ClickException(f"Local file not found: {local_file}")

    gcs_bucket_obj, EE_CMD = setup_gee_gcs(gcs_bucket)

    asset_id = f"{collection_id}/{city_name}_{gridcell_id}"

    # Enforce overwrite/existence semantics
    if not overwrite and ee_asset_exists(EE_CMD, asset_id):
        raise click.ClickException(
            "Asset already exists. Use --overwrite to replace it:\n"
            f"{asset_id}"
        )

    # IMPORTANT: use stable property names that match EE filters
    properties = {
        "city": str(city_name),
        "grid_cell": int(gridcell_id) if str(gridcell_id).isdigit() else str(gridcell_id),
        "version": str(version),
        "start_time": time.strftime("%Y-%m-%d"),
    }

    # Stage to GCS (either from local or from S3)
    if using_local:
        gs_uri = upload_local_to_gcs_until_success(gcs_bucket_obj, local_file, gcs_blob_name)
    else:
        gs_uri = upload_s3_to_gcs_until_success(gcs_bucket_obj, s3_bucket, s3_key, gcs_blob_name)

    # Start (and optionally wait for) the EE upload
    task_out = start_ee_upload(
        EE_CMD,
        gs_uri=gs_uri,
        asset_id=asset_id,
        properties=properties,
        overwrite=overwrite,
        wait=wait,
    )

    print(f"🚀 Upload command finished. Output:\n{task_out}")
    print("Check status at: https://code.earthengine.google.com/tasks")

    # Optional cleanup of staged object
    if cleanup_gcs:
        try:
            gcs_bucket_obj.blob(gcs_blob_name).delete()
            print(f"🧹 Deleted staged GCS object gs://{gcs_bucket_obj.name}/{gcs_blob_name}")
        except Exception as e:
            print(f"⚠️ Could not delete staged GCS object: {e}")


if __name__ == "__main__":
    main()
