# seasonal_albedo.py

import ee
from datetime import datetime, date

# Cloud Score+ threshold
CLEAR_THRESHOLD = 0.60

# ---- Helper: Bonafoni albedo band ----
def _add_albedo(image):
    image = ee.Image(image)
    albedo = image.expression(
        "B*Bw + G*Gw + R*Rw + NIR*NIRw + SW1*SW1w + SW2*SW2w",
        {
            "B":   image.select("B2"),
            "G":   image.select("B3"),
            "R":   image.select("B4"),
            "NIR": image.select("B8"),
            "SW1": image.select("B11"),
            "SW2": image.select("B12"),
            "Bw":  0.2266,
            "Gw":  0.1236,
            "Rw":  0.1573,
            "NIRw": 0.3417,
            "SW1w": 0.1170,
            "SW2w": 0.0338,
        },
    ).rename("albedo")
    return image.addBands(albedo)


# ---- Helper: most recent complete season window (JJA or DJF) ----
def _season_window_for(geom):
    """
    Compute most recent *complete* JJA (north) or DJF (south) window
    using the geometry's latitude and today's UTC date.
    Returns a dict with ee.Date start/end + hemisphere string.
    """
    g = ee.Geometry(geom)

    # Get centroid latitude on the server, then bring back to client
    centroid = g.centroid(1).coordinates().getInfo()
    lat = centroid[1]
    is_north = lat >= 0

    today = datetime.utcnow().date()
    yr = today.year

    if is_north:
        # JJA: [Jun 1, Sep 1) of most recent complete year
        jja_start_this = date(yr, 6, 1)
        jja_end_this   = date(yr, 9, 1)
        if today >= jja_end_this:
            start = jja_start_this
            end   = jja_end_this
        else:
            start = date(yr - 1, 6, 1)
            end   = date(yr - 1, 9, 1)
        hemi = "north"
    else:
        # DJF: [Dec 1, Mar 1) of most recent complete season
        mar1_this = date(yr, 3, 1)
        if today >= mar1_this:
            # last complete DJF is Dec(yr-1)–Mar(yr)
            start = date(yr - 1, 12, 1)
            end   = date(yr, 3, 1)
        else:
            # last complete DJF is Dec(yr-2)–Mar(yr-1)
            start = date(yr - 2, 12, 1)
            end   = date(yr - 1, 3, 1)
        hemi = "south"

    start_ee = ee.Date(start.isoformat())
    end_ee   = ee.Date(end.isoformat())

    return {
        "start": start_ee,
        "end":   end_ee,
        "hemisphere": hemi,
    }


# ---- Public function: seasonal S2 albedo ----
def seasonal_s2_albedo(geom):
    """
    Returns a Sentinel-2 seasonal albedo median composite for a geometry.
    - Input: geom (ee.Geometry or ee.Feature/FeatureCollection)
    - Output: ee.Image with band 'albedo' and properties:
        {season_start, season_end, hemisphere}
    """
    # Normalize geometry input
    if isinstance(geom, ee.feature.Feature):
        g = geom.geometry()
    elif isinstance(geom, ee.featurecollection.FeatureCollection):
        g = geom.geometry()
    else:
        g = ee.Geometry(geom)

    win = _season_window_for(g)
    start = win["start"]
    end   = win["end"]
    hemi  = win["hemisphere"]

    # ImageCollections
    S2   = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    S2CS = ee.ImageCollection("GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED")

    # Cloud Score+ masking and scaling to [0,1]
    s2_masked = (
        S2.filterBounds(g)
          .filterDate(start, end)
          .linkCollection(S2CS, ["cs"])
          .map(
              lambda img: ee.Image(img)
              .updateMask(ee.Image(img).select("cs").gte(CLEAR_THRESHOLD))
              .divide(10000)
          )
    )

    # Add albedo band and select it
    albedo_ic = s2_masked.map(_add_albedo).select("albedo")

    # Median composite or empty masked image if no data
    alb = ee.Image(
        ee.Algorithms.If(
            albedo_ic.size().gt(0),
            albedo_ic.median(),
            ee.Image(0).rename("albedo").updateMask(ee.Image(0)),
        )
    )

    # Clip to geometry bounds
    alb = alb.clip(g.bounds(1))

    # Cap [0,1]
    alb = alb.where(alb.gte(1), 1).where(alb.lte(0), 0)

    # Attach season metadata
    alb = alb.set({
        "season_start": start.format("YYYY-MM-dd"),
        "season_end":   end.format("YYYY-MM-dd"),
        "hemisphere":   hemi,
    })

    return alb
