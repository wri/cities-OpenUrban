#!/usr/bin/env python

import argparse
import os
import time

import ee
import geemap
import boto3
from botocore.exceptions import ClientError
from google.cloud import storage as gcs_storage
from seasonal_albedo import seasonal_s2_albedo

# ---------------------------------------------------------------------
# CONFIG: set your GCS bucket here
# ---------------------------------------------------------------------
GCS_BUCKET = "wri-cities-lulc-gee" 
GCS_PROJECT = "nifty-memory-399115"

gcs_client = gcs_storage.Client(project=GCS_PROJECT)

# ---------------------------------------------------------------------
# Initialize Earth Engine
# ---------------------------------------------------------------------
ee.Initialize()


# ---------------------------------------------------------------------
# Helper: export flat → GCS (overwrite) → S3
# ---------------------------------------------------------------------
def export_flat_to_gcs_and_s3(flat,
                              city_name,
                              gcs_bucket,
                              gcs_project,
                              s3_bucket="wri-cities-heat"):
    """
    1. Export 'flat' as CSV to GCS via Export.table.toCloudStorage
       with a fixed fileNamePrefix (overwrite each run).
    2. Wait for completion.
    3. Copy CSV from GCS to S3 at:
         s3://wri-cities-heat/OpenUrban/{city_name}/opportunity/tree-albedo-grid.csv
    """

    flat_no_geom = flat.map(lambda f: ee.Feature(None, f.toDictionary()))

    gcs_prefix = "tree-albedo-grid"
    description = f"tree_albedo_grid_{city_name}"

    task = ee.batch.Export.table.toCloudStorage(
        collection=flat_no_geom,
        description=description,
        bucket=gcs_bucket,
        fileNamePrefix=gcs_prefix,
        fileFormat="CSV",
    )

    task.start()
    print(f"Started EE table export task: {description}")

    # Poll for completion
    while True:
        status = task.status()
        state = status.get("state")
        print(f"  Task state: {state}")
        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(30)

    if state != "COMPLETED":
        raise RuntimeError(f"Export failed or cancelled: {status}")

    # GCS blob path
    gcs_blob_name = gcs_prefix + ".csv"

    print(f"Downloading from gs://{gcs_bucket}/{gcs_blob_name} ...")
    gcs_client = gcs_storage.Client(project=gcs_project)
    bucket_gcs = gcs_client.bucket(gcs_bucket)
    blob = bucket_gcs.blob(gcs_blob_name)
    data = blob.download_as_bytes()

    s3_key = f"OpenUrban/{city_name}/opportunity/tree-albedo-grid.csv"
    print(f"Uploading to s3://{s3_bucket}/{s3_key} ...")

    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=data)
    print("  Upload to S3 complete")

    return {
        "gcs_uri": f"gs://{gcs_bucket}/{gcs_blob_name}",
        "s3_uri": f"s3://{s3_bucket}/{s3_key}",
    }



# ---------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------
def run_city(
    city,
    country,
    data_path
):
    """
    Run the full workflow for a given city/country and write grid outputs under:
      out_dir = data_path/data/{country}-{city}/potential

    Then:
      - export grid locally + upload to S3
      - export flat via EE → GCS (overwrite) → S3
    """

    city_name = f"{country}-{city}"

    # Local directory for outputs
    out_dir = os.path.join(data_path, "data", city_name, "potential")
    os.makedirs(out_dir, exist_ok=True)

    # Local file path for grid
    grid_local = os.path.join(out_dir, "worldpop_grid_100m.geojson")

    # S3 config
    bucket = "wri-cities-heat"
    s3_prefix = f"OpenUrban/{city_name}/opportunity"
    grid_key = f"{s3_prefix}/worldpop_grid_100m.geojson"

    # -----------------------------------------------------------------
    # 1. Extent (urban extent for city)
    # -----------------------------------------------------------------
    urban_extents = ee.FeatureCollection(
        "projects/wri-datalab/cities/urban_land_use/data/global_cities_Aug2024/urbanextents_unions_2020"
    )

    extent = (
        urban_extents.filter(
            ee.Filter.stringContains("city_name_large", city.replace("_", " "))
        )
        .filter(ee.Filter.eq("country_ISO", country))
    )

    # -----------------------------------------------------------------
    # 2. LULC image
    # -----------------------------------------------------------------
    lulc_img_col = (
        ee.ImageCollection("projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC")
        .filter(ee.Filter.eq("city", city_name))
        .filterBounds(extent)
    )

    proj = lulc_img_col.first().projection()
    proj_crs = proj.crs()
    
    lulc_img = lulc_img_col.max().toInt().rename("lulc")

    # Canopy height collection
    canopy_ht = ee.ImageCollection(
        "projects/meta-forest-monitoring-okw37/assets/CanopyHeight"
    )

    # -----------------------------------------------------------------
    # 3. WorldPop grid
    # -----------------------------------------------------------------
    worldpop_col = ee.ImageCollection("WorldPop/GP/100m/pop").filterBounds(extent)
    worldpop_proj = worldpop_col.first().projection()
    worldpop = worldpop_col.mosaic().clip(extent.geometry())

    # coveringGrid returns a FeatureCollection of grid cells covering the geometry
    grid = worldpop.geometry().coveringGrid(worldpop_proj)

    # Ensure a 'gid' property (use system:index if nothing else)
    def add_gid(feat):
        feat = ee.Feature(feat)
        return feat.set("gid", feat.get("system:index"))

    grid = grid.map(add_gid)

    # -----------------------------------------------------------------
    # 4. Sentinel-2 albedo
    # -----------------------------------------------------------------
    city_alb = seasonal_s2_albedo(extent.geometry())

    # -----------------------------------------------------------------
    # 5. Remap LULC and mask water
    # -----------------------------------------------------------------
    from_vals = [110, 120, 130, 200, 300, 400, 500, 600, 601, 602, 610, 611, 612, 620, 621, 622]
    to_vals   = [110, 120, 130, 200,   9, 400, 500, 600, 601, 602, 600, 601, 602, 600, 601, 602]

    plantable_lulc = lulc_img.remap(from_vals, to_vals).rename("lulc")

    # mask out water (lulc == 9)
    no_water_mask = plantable_lulc.neq(9)
    plantable_lulc = plantable_lulc.updateMask(no_water_mask)

    # -----------------------------------------------------------------
    # 6. Pixel area, tree cover, albedo
    # -----------------------------------------------------------------
    pixel_area = ee.Image.pixelArea().rename("area")

    tree_cover = (
        canopy_ht.filterBounds(extent)
        .mosaic()
        .gte(3)
        .rename("tree")
    )

    # -----------------------------------------------------------------
    # 7. Grouped stats: mean(tree), mean(alb), sum(area) by LULC
    # -----------------------------------------------------------------
    reducer = (
        ee.Reducer.mean()
        .repeat(2)
        .combine(reducer2=ee.Reducer.sum(), sharedInputs=False)
        .group(groupField=3, groupName="lulc")
    )

    stacked = ee.Image.cat(
        [
            tree_cover.rename("tree"),
            city_alb.rename("alb"),
            pixel_area,
            plantable_lulc.toInt(),
        ]
    )

    grouped = stacked.clip(extent.geometry()).reduceRegions(
        collection=grid,
        reducer=reducer,
        scale=1,
        crs=proj,
    )

    # -----------------------------------------------------------------
    # 8. Flatten rows: (gid, lulc, tree_pct, albedo, area)
    # -----------------------------------------------------------------
    def expand_groups(f):
        f = ee.Feature(f)
        gid = ee.String(f.get("gid"))

        groups = ee.List(
            ee.Algorithms.If(
                ee.Algorithms.IsEqual(f.get("groups"), None),
                ee.List([]),
                f.get("groups"),
            )
        )

        def make_feat(g):
            g = ee.Dictionary(g)
            means = ee.List(g.get("mean"))
            tree_pct = means.get(0)
            albedo   = means.get(1)
            return ee.Feature(
                None,
                {
                    "gid": gid,
                    "lulc": g.get("lulc"),
                    "tree_pct": tree_pct,
                    "albedo": albedo,
                    "area": g.get("sum"),
                },
            )

        feats = groups.map(make_feat)
        return ee.FeatureCollection(feats)

    flat = ee.FeatureCollection(grouped.map(expand_groups)).flatten()
    
    # Add CRS as a property 
    flat = flat.map(lambda f: ee.Feature(f).set("crs", proj_crs))

    # -----------------------------------------------------------------
    # 9. Export grid to local GeoJSON
    # -----------------------------------------------------------------
    print(f"Exporting grid to {grid_local} ...")
    geemap.ee_export_vector(grid, filename=grid_local)

    # -----------------------------------------------------------------
    # 10. Upload grid to S3 (small, so direct upload is fine)
    # -----------------------------------------------------------------
    s3 = boto3.client("s3")  # uses AWS_* env vars set in R

    def upload(local_path, key):
        print(f"Uploading {local_path} to s3://{bucket}/{key}")
        try:
            s3.upload_file(local_path, bucket, key)
            print("  Success")
        except ClientError as e:
            print(f"  Error uploading {local_path}: {e}")

    upload(grid_local, grid_key)

    # -----------------------------------------------------------------
    # 11. Export flat via EE → GCS → S3 (overwrites each run)
    # -----------------------------------------------------------------
    export_info = export_flat_to_gcs_and_s3(
        flat       = flat,
        city_name  = city_name,
        gcs_bucket = GCS_BUCKET,
        gcs_project = GCS_PROJECT,
        s3_bucket  = bucket,
    )

    print("Flat export completed:")
    print("  GCS:", export_info["gcs_uri"])
    print("  S3 :", export_info["s3_uri"])

    print("Done.")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--city", required=True, help="City name (e.g., Cape_Town)")
    p.add_argument("--country", required=True, help="Country ISO code (e.g., ZAF)")
    p.add_argument(
        "--data_path",
        required=True,
        help="Base project path; outputs go under data/{country}-{city}/potential",
    )
    p.add_argument("--date_start", default="2024-12-01")
    p.add_argument("--date_end", default="2025-02-28")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_city(
        city=args.city,
        country=args.country,
        data_path=args.data_path,
        date_start=args.date_start,
        date_end=args.date_end,
    )
