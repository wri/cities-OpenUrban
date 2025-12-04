
#' Create summertime Sentinel-2 albedo image
#'
#' @param city      ee$Geometry or ee$Feature/FeatureCollection AOI
#' @param date_start character, start date (YYYY-MM-DD)
#' @param date_end   character, end date (YYYY-MM-DD)
#' @param clear_threshold numeric, cloud-score+ threshold (default 0.6)
#' @param clip_to_city logical, clip final image to city geometry bounds
#'
#' @return ee$Image (median albedo over the time window)
make_s2_albedo <- function(city_geom,
                           date_start = "2023-06-01",
                           date_end   = "2023-08-31",
                           clear_threshold = 0.60,
                           clip_to_city = FALSE) {
  
  # ImageCollections --------------------------------------------------------
  S2   <- ee$ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
  S2CS <- ee$ImageCollection("GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED")
  
  # Helper: add albedo band using Bonafoni-style coefficients
  add_albedo <- function(image) {
    image <- ee$Image(image)
    
    albedocalc <- image$expression(
      "((B*Bw)+(G*Gw)+(R*Rw)+(NIR*NIRw)+(SWIR1*SWIR1w)+(SWIR2*SWIR2w))",
      list(
        B      = image$select("B2"),
        G      = image$select("B3"),
        R      = image$select("B4"),
        NIR    = image$select("B8"),
        SWIR1  = image$select("B11"),
        SWIR2  = image$select("B12"),
        Bw     = 0.2266,
        Gw     = 0.1236,
        Rw     = 0.1573,
        NIRw   = 0.3417,
        SWIR1w = 0.1170,
        SWIR2w = 0.0338
      )
    )
    
    image$addBands(albedocalc$rename("albedo"))
  }
  
  # Cloud-score+ filtering and scaling
  S2filtered <- S2$
    filterBounds(city_geom)$
    filterDate(date_start, date_end)$
    linkCollection(S2CS, list("cs"))$
    map(
      ee_utils_pyfunc(function(img) {
        img <- ee$Image(img)
        img$updateMask(img$select("cs")$gte(clear_threshold))$
          divide(10000)
      })
    )
  
  # Add albedo band and keep only albedo
  S2alb_ic <- S2filtered$
    map(ee_utils_pyfunc(add_albedo))$
    select("albedo")
  
  # Median albedo over the time window
  S2alb <- S2alb_ic$median()
  
  # Optional: clip to city bounds
  if (clip_to_city) {
    S2alb <- S2alb$clip(city_geom$geometry()$bounds())
  }
  
  # Cap extreme values at 1, as in your JS
  albMask <- S2alb$gte(1)
  S2alb   <- S2alb$where(albMask, 1)
  
  S2alb
}
