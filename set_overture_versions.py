#!/usr/bin/env python3
"""
Set overture_release_version on each OpenUrban LULC image in GEE.

For each image, finds the most recent Overture Maps release that existed
at the time the image was created (based on start_time), then calls
`earthengine asset set` to write that version string to the asset.

Usage:
    python set_overture_versions.py            # live run
    python set_overture_versions.py --dry-run  # print only, no updates
"""

import argparse
import re
import subprocess
from datetime import datetime, timezone

import ee

COLLECTION_ID = "projects/wri-datalab/cities/OpenUrban/OpenUrban_LULC"

# The changelog folder on the official Overture S3 bucket contains the full
# building data for every release (partitioned by change_type=unchanged/added/removed)
# and is never pruned, unlike the release/ folder which only keeps the two most recent.
OVERTURE_CHANGELOG_PREFIX = "s3://overturemaps-us-west-2/changelog/"


def get_releases():
    """List all Overture releases from the official Overture S3 changelog (no credentials needed)."""
    result = subprocess.run(
        ["aws", "s3", "ls", "--no-sign-request", OVERTURE_CHANGELOG_PREFIX],
        capture_output=True, text=True, check=True
    )

    releases = []
    for line in result.stdout.splitlines():
        # Lines look like: "                           PRE 2024-09-18.0/"
        match = re.search(r"PRE (\d{4}-\d{2}-\d{2}[^/]*)/", line)
        if not match:
            continue
        version = match.group(1)  # already in standard format, e.g. "2024-09-18.0"
        date = datetime.strptime(version[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        releases.append((date, version))

    return sorted(releases)


def find_version_for_date(image_date, releases):
    """Return the most recent Overture release on or before image_date."""
    applicable = [(d, v) for d, v in releases if d <= image_date]
    return max(applicable, key=lambda x: x[0])[1] if applicable else None


def parse_start_time(raw):
    """Parse start_time — handles milliseconds-since-epoch (int) or YYYY-MM-DD (string)."""
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
    if isinstance(raw, str):
        return datetime.strptime(raw[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    raise ValueError(f"Unrecognized start_time format: {raw!r}")


def main(dry_run=False):
    releases = get_releases()
    print(f"Loaded {len(releases)} Overture releases: {releases[0][1]}  →  {releases[-1][1]}\n")

    # List all images in the collection
    ee.Initialize()
    collection = ee.ImageCollection(COLLECTION_ID)
    images = collection.toList(collection.size()).getInfo()
    print(f"Found {len(images)} images in {COLLECTION_ID}\n")

    for image_info in images:
        asset_id = image_info["id"]
        raw_start = image_info.get("properties", {}).get("start_time")

        if raw_start is None:
            print(f"SKIP  {asset_id}  (no start_time property)\n")
            continue

        image_date = parse_start_time(raw_start)
        version = find_version_for_date(image_date, releases)

        if version is None:
            print(f"SKIP  {asset_id}  (no Overture release exists before {image_date.date()})\n")
            continue

        print(f"{asset_id}")
        print(f"  start_time:               {image_date.date()}")
        print(f"  overture_release_version: {version}")

        if dry_run:
            print("  [dry run — skipping update]\n")
        else:
            subprocess.run(
                ["earthengine", "asset", "set",
                 "--property", f"overture_release_version={version}",
                 asset_id],
                check=True,
            )
            print("  ✓ updated\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without updating any assets")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
