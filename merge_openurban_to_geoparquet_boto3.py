# merge_openurban_to_geoparquet_boto3.py
# Requires: geopandas>=0.14, shapely>=1.8 (2.x OK), pyogrio, pyarrow>=12, boto3, pandas, numpy, packaging, pyproj

import io
import os
import sys
import hashlib
import tempfile
from collections import Counter
from typing import List, Optional

import boto3
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
from packaging.version import Version
from shapely.validation import make_valid
from shapely import force_2d, to_wkb
from shapely.ops import transform as shp_transform
from pyproj import CRS  # IMPORTANT for embedding CRS metadata in GeoParquet

# ----------------------- CONFIG -----------------------
BUCKET = "wri-cities-heat"
ROOT_PREFIX = "OpenUrban"                 # s3://wri-cities-heat/OpenUrban/<CITY>/<LAYER>/*.geojson
LAYERS = ["buildings", "openspace", "parking", "roads"]

# If your tiles lack CRS, set the default here (or via env OPENURBAN_DEFAULT_CRS)
DEFAULT_CRS = CRS.from_user_input(os.environ.get("OPENURBAN_DEFAULT_CRS", "EPSG:4326"))

# Optional: hard-code city defaults so roads inherit city UTM even if tiles are missing CRS
CITY_DEFAULTS = {
    # "ZAF-Cape_Town": CRS.from_epsg(32734),  # uncomment if desired
}

# Rounding used to stabilize tile-edge jitters (degrees: 6–7; meters: 3)
ROUNDING_PRECISION = 6

PARQUET_COMPRESSION = "snappy"
USE_MAKE_VALID = True
# -----------------------------------------------------

_HAS_WKB_ROUNDING = Version(shapely.__version__) >= Version("2.0.0")

# Cache: remember a city's CRS once chosen from any layer (so roads can reuse it)
CITY_CRS_CACHE: dict[str, CRS] = {}


def s3_client():
    # Honors AWS_PROFILE / SSO / env vars
    return boto3.client("s3")


def list_city_prefixes(s3) -> List[str]:
    paginator = s3.get_paginator("list_objects_v2")
    cities: List[str] = []
    prefix = f"{ROOT_PREFIX}/"
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            parts = [x for x in cp["Prefix"].split("/") if x]
            if len(parts) >= 2:
                cities.append(parts[1])
    return sorted(set(cities))


def list_layer_keys(s3, city: str, layer: str) -> List[str]:
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{ROOT_PREFIX}/{city}/{layer}/"
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.lower().endswith(".geojson"):
                keys.append(k)
    return sorted(keys)


def read_geojson_from_s3(s3, key: str) -> Optional[gpd.GeoDataFrame]:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        by = obj["Body"].read()
        gdf = gpd.read_file(io.BytesIO(by))  # pyogrio backend if installed (faster)
        if gdf is None or len(gdf) == 0 or "geometry" not in gdf.columns:
            return None
        gdf["__source_key"] = key  # stable tie-break info
        return gdf
    except Exception as e:
        print(f"      ! Failed to read {key}: {e}")
        return None


def _choose_target_crs(gdfs: List[gpd.GeoDataFrame], city: str, layer: str) -> CRS:
    """
    Pick a single CRS for this city's layer:
      1) Most common non-null CRS among tiles.
      2) Otherwise the per-city default in CITY_DEFAULTS.
      3) Otherwise a previously cached CRS from another layer (CITY_CRS_CACHE).
      4) Otherwise DEFAULT_CRS.
    Cache the chosen CRS for the city.
    """
    crs_candidates = [CRS.from_user_input(df.crs) for df in gdfs if df is not None and df.crs]
    if crs_candidates:
        top = Counter([c.to_string() for c in crs_candidates]).most_common(1)[0][0]
        target = CRS.from_user_input(top)
        if len(set([c.to_string() for c in crs_candidates])) > 1:
            print(f"    ! Mixed CRS across tiles; reprojecting all to {target.to_string()}")
        CITY_CRS_CACHE.setdefault(city, target)
        return target

    if city in CITY_DEFAULTS:
        target = CITY_DEFAULTS[city]
        print(f"    ! No CRS on tiles; using CITY_DEFAULTS[{city}] = {target.to_string()}")
        CITY_CRS_CACHE.setdefault(city, target)
        return target

    if city in CITY_CRS_CACHE:
        target = CITY_CRS_CACHE[city]
        print(f"    ! No CRS on tiles; using cached city CRS {target.to_string()}")
        return target

    print(f"    ! No CRS found anywhere; using DEFAULT_CRS = {DEFAULT_CRS.to_string()}")
    CITY_CRS_CACHE.setdefault(city, DEFAULT_CRS)
    return DEFAULT_CRS


def read_many_geojsons(s3, keys: List[str], city: str, layer: str) -> gpd.GeoDataFrame:
    if not keys:
        return gpd.GeoDataFrame(geometry=[], crs=DEFAULT_CRS)

    gdfs = []
    for k in keys:
        gdf = read_geojson_from_s3(s3, k)
        if gdf is not None and len(gdf) > 0 and "geometry" in gdf.columns:
            gdfs.append(gdf)

    if not gdfs:
        return gpd.GeoDataFrame(geometry=[], crs=DEFAULT_CRS)

    target_crs = _choose_target_crs(gdfs, city, layer)

    reproj = []
    for df in gdfs:
        df2 = df.copy()
        if not df2.crs:
            # Assign (don't transform) when source CRS missing; we assume tiles are already in target_crs
            df2 = df2.set_crs(target_crs, allow_override=True)
        else:
            # Normalize CRS to pyproj.CRS
            df2 = df2.set_crs(CRS.from_user_input(df2.crs), allow_override=True)
            if df2.crs != target_crs:
                df2 = df2.to_crs(target_crs)
        reproj.append(df2)

    # Align schemas with outer-join on columns
    all_cols = sorted(set().union(*[set(df.columns) for df in reproj]))
    reproj = [df.reindex(columns=all_cols) for df in reproj]
    out = pd.concat(reproj, ignore_index=True)

    # Ensure GeoDataFrame carries a pyproj.CRS
    return gpd.GeoDataFrame(out, geometry="geometry", crs=target_crs)


def _round_geom_coords(g, ndigits):
    if g is None or g.is_empty:
        return g
    factor = 10.0 ** ndigits

    def _rounder(x, y, z=None):
        return (round(x * factor) / factor, round(y * factor) / factor)

    return shp_transform(_rounder, g)


def _best_effort_hash(geom):
    # validity -> 2D
    g = geom
    try:
        if USE_MAKE_VALID:
            g = make_valid(g)
    except Exception:
        g = geom
    if g is None or getattr(g, "is_empty", True):
        g = geom
    try:
        g = force_2d(g)
    except Exception:
        pass
    if g is None or getattr(g, "is_empty", True):
        return None

    # Shapely >= 2: WKB with rounding
    if _HAS_WKB_ROUNDING:
        try:
            wkb = to_wkb(g, rounding_precision=ROUNDING_PRECISION)
            return hashlib.sha1(wkb).hexdigest()
        except Exception:
            pass
    # WKB without rounding
    try:
        wkb = to_wkb(g)
        return hashlib.sha1(wkb).hexdigest()
    except Exception:
        pass
    # Round via transform -> WKB
    try:
        g2 = _round_geom_coords(g, ROUNDING_PRECISION)
        wkb = to_wkb(g2)
        return hashlib.sha1(wkb).hexdigest()
    except Exception:
        pass
    # Last resort: WKT
    try:
        return hashlib.sha1(g.wkt.encode("utf-8")).hexdigest()
    except Exception:
        return None


def _empty_like(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cols_to_drop = {"__ghash", "__row_id", "__na_count", "__source_key"}
    base = gdf.drop(columns=list(cols_to_drop & set(gdf.columns)), errors="ignore")
    empty = base.iloc[0:0].copy()
    return gpd.GeoDataFrame(empty, geometry="geometry", crs=gdf.crs or DEFAULT_CRS)


def dedupe_keep_min_na(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Keep exactly ONE whole row per duplicate geometry:
      - choose the row with the FEWEST NAs across non-geometry columns
      - tie-break by source key (lexicographic), then original order
    """
    if gdf.empty:
        return gdf

    gdf = gdf.copy()
    if not gdf.crs:
        gdf.set_crs(DEFAULT_CRS, allow_override=True, inplace=True)

    gdf["__row_id"] = np.arange(len(gdf))
    if "__source_key" not in gdf.columns:
        gdf["__source_key"] = ""

    # Diagnostics
    n_null_geom = int(gdf["geometry"].isna().sum())
    n_empty_geom = int(sum(getattr(geom, "is_empty", False) for geom in gdf.geometry if geom is not None))
    print(f"    geometries: {len(gdf)} total | {n_null_geom} null | {n_empty_geom} empty")

    # Hash
    gdf["__ghash"] = [_best_effort_hash(g) for g in gdf.geometry]
    n_null_hash = int(gdf["__ghash"].isna().sum())
    print(f"    hash stats: {len(gdf)} total | {n_null_hash} null hashes")

    gdf = gdf[~gdf["__ghash"].isna()].copy()
    if gdf.empty:
        print("    ! all hashes are null after fallback hashing — returning empty schema")
        return _empty_like(gdf)

    # Fewest NAs wins (exclude helper cols & geometry)
    exclude = {"geometry", "__ghash", "__row_id", "__source_key"}
    cols = [c for c in gdf.columns if c not in exclude]
    gdf["__na_count"] = gdf[cols].isna().sum(axis=1) if cols else 0

    gdf.sort_values(["__ghash", "__na_count", "__source_key", "__row_id"],
                    inplace=True, kind="mergesort")

    kept = gdf.drop_duplicates(subset="__ghash", keep="first").copy()
    kept.drop(columns=["__ghash", "__row_id", "__na_count", "__source_key"],
              inplace=True, errors="ignore")
    kept = gpd.GeoDataFrame(kept, geometry="geometry", crs=gdf.crs)
    return kept


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
        gdf = read_many_geojsons(s3, keys, city, layer)
        print(f"    target CRS for this layer: {gdf.crs.to_string() if gdf.crs else 'NONE'}")

        print("    de-duplicating by geometry (fewest NAs wins)...")
        gdf2 = dedupe_keep_min_na(gdf)
        print(f"    {len(gdf)} → {len(gdf2)} features | CRS={gdf2.crs.to_string() if gdf2.crs else 'NONE'}")

        out_key = f"{ROOT_PREFIX}/{city}/{layer}/{layer}_all.parquet"
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            # Belt & suspenders: ensure CRS is a pyproj.CRS before write
            if not isinstance(gdf2.crs, CRS):
                gdf2 = gdf2.set_crs(CRS.from_user_input(gdf2.crs or DEFAULT_CRS), allow_override=True)
            gdf2.columns = [str(c) for c in gdf2.columns]
            gdf2.to_parquet(tmp_path, index=False, compression=PARQUET_COMPRESSION)
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
    # Examples:
    #   aws sso login --profile cities-data-dev
    #   AWS_PROFILE=cities-data-dev python merge_openurban_to_geoparquet_boto3.py
    #   OPENURBAN_DEFAULT_CRS=EPSG:32734 AWS_PROFILE=cities-data-dev python merge_openurban_to_geoparquet_boto3.py ZAF-Cape_Town
    if len(sys.argv) > 1:
        main(sys.argv[1:])
    else:
        main()
