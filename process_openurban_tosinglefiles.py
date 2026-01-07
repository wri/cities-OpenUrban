# merge_openurban_to_geoparquet_boto3.py
# Requires: geopandas>=0.14, shapely>=2.0, pyogrio, pyarrow, boto3, pandas

import io
import os
import sys
import hashlib
import tempfile
from typing import List, Optional, Dict

import boto3
import botocore
import pandas as pd
import geopandas as gpd
from shapely.validation import make_valid
from shapely import force_2d, to_wkb

BUCKET = "wri-cities-heat"
ROOT_PREFIX = "OpenUrban"  # s3://wri-cities-heat/OpenUrban/...
LAYERS = ["buildings", "openspace", "parking", "roads"]

# If degrees: 7 ≈ ~1 cm at equator; if meters: 3 ≈ 1 mm.
ROUNDING_PRECISION = 7


def s3_client():
    # Will honor AWS_PROFILE / SSO / env vars
    return boto3.client("s3")


def list_city_prefixes(s3) -> List[str]:
    """Return city names under OpenUrban/ (e.g., 'USA-Boston')."""
    paginator = s3.get_paginator("list_objects_v2")
    cities: List[str] = []
    prefix = f"{ROOT_PREFIX}/"
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            p = cp["Prefix"]  # e.g., 'OpenUrban/USA-Boston/'
            city = p.split("/")[1]
            cities.append(city)
    return sorted(cities)


def list_layer_keys(s3, city: str, layer: str) -> List[str]:
    """All .geojson keys under OpenUrban/<CITY>/<LAYER>/"""
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{ROOT_PREFIX}/{city}/{layer}/"
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.lower().endswith(".geojson"):
                keys.append(k)
    # Deterministic order so 'keep=first' is stable
    return sorted(keys)


def read_geojson_from_s3(s3, key: str) -> gpd.GeoDataFrame:
    """Download one GeoJSON into memory and read with GeoPandas (pyogrio backend)."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    by = obj["Body"].read()
    gdf = gpd.read_file(io.BytesIO(by))
    gdf["__source_key"] = key  # to keep a stable order
    return gdf


def read_many_geojsons(s3, keys: List[str]) -> gpd.GeoDataFrame:
    if not keys:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    gdfs = []
    for k in keys:
        gdfs.append(read_geojson_from_s3(s3, k))
    if len(gdfs) == 1:
        return gdfs[0]
    # Align schemas with outer-join on columns
    all_cols = sorted(set().union(*[set(df.columns) for df in gdfs]))
    gdfs = [df.reindex(columns=all_cols) for df in gdfs]
    out = pd.concat(gdfs, ignore_index=True)
    return gpd.GeoDataFrame(out, geometry="geometry", crs=gdfs[0].crs)


def geom_hash(geom) -> Optional[str]:
    """Stable hash after validity-fix, 2D, and rounded WKB."""
    if geom is None or geom.is_empty:
        return None
    try:
        g = make_valid(geom)
        g = force_2d(g)
        wkb = to_wkb(g, rounding_precision=ROUNDING_PRECISION)
        return hashlib.sha1(wkb).hexdigest()
    except Exception:
        return None


def dedupe_keep_first_row(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Drop duplicate geometries; keep the ENTIRE first row."""
    if gdf.empty:
        return gdf
    gdf = gdf.copy()
    gdf["__row_id"] = range(len(gdf))
    gdf.sort_values(["__source_key", "__row_id"], inplace=True, kind="mergesort")
    gdf["__ghash"] = [geom_hash(g) for g in gdf.geometry]
    gdf = gdf[~gdf["__ghash"].isna()].copy()
    gdf = gdf.drop_duplicates(subset="__ghash", keep="first")
    gdf = gdf.drop(columns=["__ghash", "__row_id"], errors="ignore")
    return gdf


def upload_parquet(s3, local_path: str, dest_key: str):
    s3.upload_file(local_path, BUCKET, dest_key)
    print(f"    uploaded s3://{BUCKET}/{dest_key}")


def process_city(s3, city: str):
    print(f"\n=== {city} ===")
    for layer in LAYERS:
        keys = list_layer_keys(s3, city, layer)
        if not keys:
            print(f"  - {layer}: no tiles, skipping.")
            continue

        print(f"  - {layer}: reading {len(keys)} tiles...")
        gdf = read_many_geojsons(s3, keys)

        # If you ever see mixed CRSs per layer, force one here:
        # gdf = gdf.to_crs("EPSG:XXXX")

        print("    de-duplicating by geometry (keep first whole row)...")
        gdf2 = dedupe_keep_first_row(gdf)
        print(f"    {len(gdf)} → {len(gdf2)} features")

        # Write to a temp GeoParquet and upload
        out_key = f"{ROOT_PREFIX}/{city}/{layer}/{layer}_all.parquet"
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            gdf2.columns = [str(c) for c in gdf2.columns]
            gdf2.to_parquet(tmp_path, index=False)  # GeoParquet via pyarrow metadata
            upload_parquet(s3, tmp_path, out_key)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    print(f"Done {city}.")


def main(only_cities: Optional[List[str]] = None):
    s3 = s3_client()
    cities = only_cities or list_city_prefixes(s3)
    if not cities:
        print("No city folders found under OpenUrban/")
        return
    for city in cities:
        process_city(s3, city)


if __name__ == "__main__":
    # Optionally pass city names as CLI args:
    #   AWS_PROFILE=cities-data-dev python merge_openurban_to_geoparquet_boto3.py USA-Boston ZAF-Cape_Town
    if len(sys.argv) > 1:
        main(sys.argv[1:])
    else:
        main()
