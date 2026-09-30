"""
Fetch the inputs for opportunity layers over a custom boundary (AOI).

Everything is written under data/{city}/aoi/{aoi_name}/ and, via utils.upload.to_s3,
to s3://wri-cities-tcm/OpenUrban/{city}/aoi/{aoi_name}/:

  boundaries/aoi.geojson               dissolved boundary (EPSG:4326)
  city_grid/city_grid.geojson          only when OpenUrban is fetched from CIF
  inputs/WorldPop/worldpop.tif         100 m output grid
  inputs/OpenUrban/{tile}.tif          only when OpenUrban is fetched from CIF
  inputs/TreeCanopyHeight/{tile}.tif   1 m, height >= 3 m
  inputs/AlbedoCloudMasked/{tile}.tif  10 m (cool roofs only)
  inputs/tiles.geojson                 per-tile paths read by 4--opportunity-layers-aoi.R
  inputs/manifest.json                 sources and parameters used

OpenUrban source ("auto"): the GEE OpenUrban collection is queried for the tiles that
cover the boundary. If a single generated city covers it and its tiles exist in
wri-cities-tcm, those tiles are read in place ("tcm"); otherwise OpenUrban is fetched
from the GEE asset with CIF ("cif"). If the boundary is not covered, the script stops
before downloading anything.
"""
import argparse
import inspect
import json
import os
from datetime import datetime, timezone

import boto3
import ee
import geopandas as gpd
import numpy as np
from botocore.exceptions import ClientError
from shapely.geometry import box, shape
from shapely.ops import unary_union

from city_metrix.metrix_model import GeoExtent
from city_metrix.metrix_tools import get_utm_zone_from_latlon_point
from city_metrix.layers import (
    AlbedoCloudMasked,
    OpenUrban,
    TreeCanopyHeight,
    WorldPop,
)

from utils.download import _retry, _output_ready, _write_raster_atomic
from utils.grid import create_grid_for_city
from utils.upload import to_s3
from get_data import _safe_call, _FAILURES, _report_and_exit

OPENURBAN_COLLECTION = "projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC"
TCM_BUCKET = "wri-cities-tcm"
TCM_HTTP = f"https://{TCM_BUCKET}.s3.us-east-1.amazonaws.com"

# Share of the boundary allowed to fall outside OpenUrban coverage
COVERAGE_TOLERANCE = 0.01
# Buffer around the boundary for fetching; covers WorldPop cells that straddle the edge
FETCH_BUFFER_M = 200


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _utm_crs(geom_4326):
    return get_utm_zone_from_latlon_point(geom_4326.centroid)


def _uncovered_share(aoi_utm, footprints_utm):
    """Share of aoi_utm (a shapely geometry) outside the union of footprints_utm."""
    if aoi_utm.area == 0:
        return 1.0
    if len(footprints_utm) == 0:
        return 1.0
    covered = unary_union(list(footprints_utm))
    return aoi_utm.difference(covered).area / aoi_utm.area


def _s3_exists(s3, key):
    try:
        s3.head_object(Bucket=TCM_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def _check_cif_version():
    if "version" not in inspect.signature(WorldPop.__init__).parameters:
        raise SystemExit(
            "The installed city_metrix has no WorldPop version option. Reinstall it with:\n"
            '  pip install -U --force-reinstall "city-metrix @ git+https://github.com/wri/cities-cif"'
        )


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
def load_boundary(boundary, aoi_path):
    """Read, repair and dissolve the boundary; save it as boundaries/aoi.geojson."""
    boundaries_file = f"{aoi_path}/boundaries/aoi.geojson"
    gdf = gpd.read_file(boundary)
    if gdf.crs is None:
        raise SystemExit(f"Boundary has no CRS: {boundary}")
    gdf = gdf.to_crs("EPSG:4326")
    geom = unary_union(gdf.geometry.make_valid())
    aoi = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:4326")

    os.makedirs(os.path.dirname(boundaries_file), exist_ok=True)
    aoi.to_file(boundaries_file, driver="GeoJSON")
    return aoi, boundaries_file


def query_openurban_tiles(aoi):
    """Return a GeoDataFrame (EPSG:4326) of OpenUrban GEE images touching the boundary."""
    minx, miny, maxx, maxy = aoi.total_bounds
    rect = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

    coll = ee.ImageCollection(OPENURBAN_COLLECTION).filterBounds(rect)
    fc = coll.map(
        lambda im: ee.Feature(im.geometry(), {"city": im.get("city"), "grid_cell": im.get("grid_cell")})
    )
    info = _retry(lambda: fc.getInfo(), "OpenUrban GEE footprint query")

    rows = [
        {
            "city": f["properties"].get("city"),
            "grid_cell": f["properties"].get("grid_cell"),
            "geometry": shape(f["geometry"]),
        }
        for f in info.get("features", [])
    ]
    if not rows:
        return gpd.GeoDataFrame(columns=["city", "grid_cell", "geometry"], geometry="geometry", crs="EPSG:4326")
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


def choose_lulc_source(aoi, footprints, lulc_source):
    """
    Coverage check + OpenUrban source choice. Downloads no rasters.

    Returns (source, source_city, uncovered_share); exits if the boundary is not covered.
    """
    utm = _utm_crs(aoi.geometry.iloc[0])
    aoi_utm = aoi.to_crs(utm).geometry.iloc[0]
    fp_utm = footprints.to_crs(utm) if len(footprints) else footprints

    overall = _uncovered_share(aoi_utm, fp_utm.geometry if len(fp_utm) else [])
    print(f"OpenUrban coverage: {100 * (1 - overall):.1f}% of the boundary")
    if overall > COVERAGE_TOLERANCE:
        raise SystemExit(
            f"{100 * overall:.1f}% of the boundary has no OpenUrban data in {OPENURBAN_COLLECTION}. "
            "Generate OpenUrban for this area before running opportunity layers."
        )

    # Best single generated city
    per_city = {}
    for city_name, grp in fp_utm.groupby("city"):
        per_city[city_name] = _uncovered_share(aoi_utm, grp.geometry)
    best_city = min(per_city, key=per_city.get) if per_city else None
    best_share = per_city.get(best_city, 1.0)

    for c, s in sorted(per_city.items(), key=lambda kv: kv[1]):
        print(f"  {c}: {100 * (1 - s):.1f}% coverage")

    if lulc_source == "cif":
        return "cif", None, overall

    single_city_ok = best_city is not None and best_share <= COVERAGE_TOLERANCE
    if lulc_source == "tcm" and not single_city_ok:
        raise SystemExit(
            "--lulc-source tcm: no single generated city covers the boundary "
            f"(best: {best_city}, {100 * (1 - best_share):.1f}%). Use --lulc-source cif or auto."
        )

    if single_city_ok:
        return "tcm", best_city, best_share
    print("No single generated city covers the boundary; fetching OpenUrban from CIF.")
    return "cif", None, overall


def _with_tile_name(grid):
    if "tile_name" not in grid.columns:
        grid["tile_name"] = grid["ID"].astype(int).astype(str).str.zfill(5).radd("tile_")
    return grid


def build_tcm_tiles(aoi_buf, footprints, source_city, s3):
    """Tiles = source city's grid cells near the boundary, with LULC read in place from wri-cities-tcm."""
    grid = gpd.read_file(f"{TCM_HTTP}/OpenUrban/{source_city}/city_grid/city_grid.geojson").to_crs("EPSG:4326")
    grid = _with_tile_name(grid)

    cells = set(
        int(g) for g in footprints.loc[footprints["city"] == source_city, "grid_cell"] if g is not None
    )
    grid = grid[grid["ID"].astype(int).isin(cells) & grid.intersects(aoi_buf)].copy()

    lulc_keys = [f"OpenUrban/{source_city}/OpenUrban/{source_city}_{int(i)}.tif" for i in grid["ID"]]
    missing = [k for k in lulc_keys if not _s3_exists(s3, k)]
    if missing:
        return None, missing

    grid["lulc_path"] = [f"{TCM_HTTP}/{k}" for k in lulc_keys]
    return grid[["ID", "tile_name", "lulc_path", "geometry"]].reset_index(drop=True), []


def build_cif_tiles(city_ns, aoi, aoi_buf, data_path, copy_to_s3):
    """Tiles = new 15 km grid over the boundary; LULC fetched from CIF into inputs/OpenUrban/."""
    grid = create_grid_for_city(city_ns, aoi, data_path=data_path, copy_to_s3=copy_to_s3)
    grid = _with_tile_name(grid.to_crs("EPSG:4326"))
    grid = grid[grid.intersects(aoi_buf)].copy()
    grid["lulc_path"] = [
        f"{TCM_HTTP}/OpenUrban/{city_ns}/inputs/OpenUrban/{t}.tif" for t in grid["tile_name"]
    ]
    return grid[["ID", "tile_name", "lulc_path", "geometry"]].reset_index(drop=True)


def fetch_bbox(tile_geom, aoi_buf_4326):
    """Fetch extent for a tile: tile ∩ buffered boundary bbox."""
    clip = tile_geom.intersection(box(*aoi_buf_4326.bounds))
    return GeoExtent(bbox=clip.bounds, crs="EPSG:4326")


def fetch_layer_tile(layer, label, bbox, out_file, data_path, copy_to_s3, **get_kwargs):
    """Fetch one raster tile with CIF and save it; skip if already present."""
    if _output_ready(out_file):
        print(f"{label} already exists at {out_file}, skipping fetch.")
        return
    data = _retry(lambda: layer.get_data(bbox, **get_kwargs), label)
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    _write_raster_atomic(data, out_file)
    if copy_to_s3:
        to_s3(out_file, data_path)


def openurban_nodata_share(tile_files, aoi):
    """Share of boundary pixels with no OpenUrban class, from downloaded CIF tiles.

    Valid OpenUrban codes are all >= 110, so NaN, nodata and 0 all count as missing.
    Tiles overlap slightly (fetch buffer), which is fine for a share.
    """
    import rasterio
    from rasterio.features import geometry_mask

    total = 0
    missing = 0
    for f in tile_files:
        if not os.path.exists(f):
            continue
        with rasterio.open(f) as src:
            geom = aoi.to_crs(src.crs).geometry.iloc[0]
            inside = geometry_mask([geom], out_shape=(src.height, src.width),
                                   transform=src.transform, invert=True)
            if not inside.any():
                continue
            vals = src.read(1).astype("float64")
            if src.nodata is not None:
                vals[vals == src.nodata] = np.nan
            vals = vals[inside]
            total += vals.size
            missing += int(np.sum(np.isnan(vals) | (vals < 110)))
    return 1.0 if total == 0 else missing / total


def run_parallel(tasks):
    """Run (fn, label, args, kwargs) tasks on a small local dask cluster; record failures."""
    from dask.distributed import Client, LocalCluster
    from dask import delayed
    import dask

    cluster = LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        memory_limit="12GB",
        dashboard_address=":0",
        local_directory=f"/tmp/dask-spill-{os.getuid()}",
    )
    client = Client(cluster)
    try:
        jobs = [delayed(_safe_call)(fn, label, *args, **kwargs) for fn, label, args, kwargs in tasks]
        results = dask.compute(*jobs)
        _FAILURES.extend(r for r in results if r is not None)
    finally:
        client.close()
        cluster.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def get_aoi_data(
    city,
    aoi_name,
    boundary,
    output_base=".",
    lulc_source="auto",
    layers=("trees",),
    worldpop_version=2,
    albedo_start=None,
    albedo_end=None,
    copy_to_s3=True,
):
    _check_cif_version()
    _FAILURES.clear()

    data_path = os.path.join(output_base, "data")
    city_ns = f"{city}/aoi/{aoi_name}"
    aoi_path = f"{data_path}/{city_ns}"
    inputs_path = f"{aoi_path}/inputs"
    need_tree = "trees" in layers
    need_cool = "cool-roofs" in layers

    # 1) Boundary
    aoi, boundaries_file = load_boundary(boundary, aoi_path)
    if copy_to_s3:
        to_s3(boundaries_file, data_path)

    utm = _utm_crs(aoi.geometry.iloc[0])
    aoi_buf_4326 = aoi.to_crs(utm).buffer(FETCH_BUFFER_M).to_crs("EPSG:4326").geometry.iloc[0]

    # 2) Coverage check + OpenUrban source (no raster downloads)
    footprints = query_openurban_tiles(aoi)
    source, source_city, uncovered = choose_lulc_source(aoi, footprints, lulc_source)

    # 3) Tile grid
    s3 = boto3.client("s3")
    if source == "tcm":
        tiles, missing = build_tcm_tiles(aoi_buf_4326, footprints, source_city, s3)
        if tiles is None:
            if lulc_source == "tcm":
                raise SystemExit(f"Missing OpenUrban tiles in wri-cities-tcm: {missing[:5]} ...")
            print(f"{len(missing)} {source_city} tiles missing in wri-cities-tcm; fetching OpenUrban from CIF.")
            source, source_city = "cif", None
    if source == "cif":
        tiles = build_cif_tiles(city_ns, aoi, aoi_buf_4326, data_path, copy_to_s3)
    print(f"Using OpenUrban source: {source}" + (f" ({source_city} tiles)" if source_city else ""))
    print(f"{len(tiles)} tiles cover the boundary.")

    per_tile = [(row.tile_name, fetch_bbox(row.geometry, aoi_buf_4326)) for row in tiles.itertuples()]

    # 4a) OpenUrban from CIF first, and verify it before fetching anything else
    if source == "cif":
        ou_files = {t: f"{inputs_path}/OpenUrban/{t}.tif" for t, _ in per_tile}
        run_parallel([
            (fetch_layer_tile, f"OpenUrban/{t}", (OpenUrban(), f"OpenUrban {city_ns}/{t}", bb, ou_files[t], data_path, copy_to_s3), {})
            for t, bb in per_tile
        ])
        if _FAILURES:
            _report_and_exit(city_ns)
        nodata = openurban_nodata_share(ou_files.values(), aoi)
        print(f"OpenUrban nodata inside boundary: {100 * nodata:.1f}%")
        if nodata > COVERAGE_TOLERANCE:
            raise SystemExit(
                f"{100 * nodata:.1f}% of the boundary has no OpenUrban values. "
                "Generate OpenUrban for this area before running opportunity layers."
            )

    # 4b) Tree canopy height and albedo per tile
    tasks = []
    if need_tree:
        tree_layer = TreeCanopyHeight(height=3)
        tiles["tree_path"] = [f"{TCM_HTTP}/OpenUrban/{city_ns}/inputs/TreeCanopyHeight/{t}.tif" for t in tiles["tile_name"]]
        for t, bb in per_tile:
            out = f"{inputs_path}/TreeCanopyHeight/{t}.tif"
            tasks.append((fetch_layer_tile, f"TreeCanopyHeight/{t}",
                          (tree_layer, f"TreeCanopyHeight {city_ns}/{t}", bb, out, data_path, copy_to_s3), {}))

    if need_cool:
        if albedo_start is None or albedo_end is None:
            from city_metrix.layers.albedo import get_albedo_default_date_range
            aoi_extent = GeoExtent(bbox=tuple(aoi.total_bounds), crs="EPSG:4326")
            albedo_start, albedo_end = get_albedo_default_date_range(aoi_extent)
        print(f"Albedo date range: {albedo_start} to {albedo_end}")
        albedo_layer = AlbedoCloudMasked(start_date=albedo_start, end_date=albedo_end,
                                         zonal_stats="median", num_seasons=3)
        tiles["albedo_path"] = [f"{TCM_HTTP}/OpenUrban/{city_ns}/inputs/AlbedoCloudMasked/{t}.tif" for t in tiles["tile_name"]]
        for t, bb in per_tile:
            out = f"{inputs_path}/AlbedoCloudMasked/{t}.tif"
            tasks.append((fetch_layer_tile, f"AlbedoCloudMasked/{t}",
                          (albedo_layer, f"AlbedoCloudMasked {city_ns}/{t}", bb, out, data_path, copy_to_s3), {}))

    if tasks:
        print(f"Running {len(tasks)} fetch tasks...")
        run_parallel(tasks)

    # 5) WorldPop over the buffered boundary (defines the 100 m output grid)
    wp_file = f"{inputs_path}/WorldPop/worldpop.tif"
    wp_extent = GeoExtent(bbox=tuple(aoi.total_bounds), crs="EPSG:4326").buffer_utm_bbox(500)
    result = _safe_call(
        fetch_layer_tile,
        "WorldPop",
        WorldPop(version=worldpop_version),
        f"WorldPop v{worldpop_version} {city_ns}",
        wp_extent,
        wp_file,
        data_path,
        copy_to_s3,
        spatial_resolution=100,
    )
    if result is not None:
        _FAILURES.append(result)

    if _FAILURES:
        _report_and_exit(city_ns)

    # 6) Tile index + manifest
    tiles_file = f"{inputs_path}/tiles.geojson"
    for col in ("tree_path", "albedo_path"):
        if col not in tiles.columns:
            tiles[col] = None
    tiles[["ID", "tile_name", "lulc_path", "tree_path", "albedo_path", "geometry"]].to_file(
        tiles_file, driver="GeoJSON"
    )

    manifest = {
        "city": city,
        "aoi_name": aoi_name,
        "boundary_source": str(boundary),
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "lulc_source": source,
        "lulc_source_city": source_city,
        "openurban_uncovered_share": round(float(uncovered), 5),
        "worldpop_version": worldpop_version,
        "worldpop_path": f"{TCM_HTTP}/OpenUrban/{city_ns}/inputs/WorldPop/worldpop.tif",
        "tree_height_threshold_m": 3 if need_tree else None,
        "albedo_start": albedo_start if need_cool else None,
        "albedo_end": albedo_end if need_cool else None,
        "layers": sorted(layers),
        "n_tiles": int(len(tiles)),
    }
    manifest_file = f"{inputs_path}/manifest.json"
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=2)

    if copy_to_s3:
        to_s3(tiles_file, data_path)
        to_s3(manifest_file, data_path)

    _report_and_exit(city_ns)


def _parse_args():
    parser = argparse.ArgumentParser(description="Fetch opportunity-layer inputs for a custom boundary.")
    parser.add_argument("city", help="City identifier for the output folder, e.g. USA-Oakland")
    parser.add_argument("--aoi-name", required=True, help="Name for this boundary, e.g. city-limits")
    parser.add_argument("--boundary", required=True, help="Boundary file or URL readable by geopandas")
    parser.add_argument("--output-base", default=".", help="Base directory for local data (default: .)")
    parser.add_argument("--lulc-source", choices=["auto", "tcm", "cif"], default="auto",
                        help="Where to read OpenUrban from (default: auto)")
    parser.add_argument("--layers", default="trees",
                        help="Comma-separated: trees, cool-roofs (default: trees)")
    parser.add_argument("--worldpop-version", type=int, default=2, help="WorldPop version (default: 2)")
    parser.add_argument("--albedo-start", default=None, help="Albedo start date YYYY-MM-DD (default: CIF)")
    parser.add_argument("--albedo-end", default=None, help="Albedo end date YYYY-MM-DD (default: CIF)")
    parser.add_argument("--skip-s3-upload", action="store_true", help="Skip uploading files to S3.")
    return parser.parse_args()


def main():
    args = _parse_args()
    layers = {x.strip() for x in args.layers.split(",") if x.strip()}
    bad = layers - {"trees", "cool-roofs"}
    if bad:
        raise SystemExit(f"Unknown --layers value(s): {', '.join(sorted(bad))}")
    get_aoi_data(
        args.city,
        args.aoi_name,
        args.boundary,
        output_base=args.output_base,
        lulc_source=args.lulc_source,
        layers=layers,
        worldpop_version=args.worldpop_version,
        albedo_start=args.albedo_start,
        albedo_end=args.albedo_end,
        copy_to_s3=not args.skip_s3_upload,
    )


if __name__ == "__main__":
    main()
