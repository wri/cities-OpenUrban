# ---- allowed opportunity keys (shared by the workflow entry scripts) ----
OPP_KEYS <- c("baseline__trees",
              "baseline__cool-roofs",
              "trees__all-plantable",
              "trees__pedestrian",
              "cool-roofs__all-roofs",
              "all")

parse_opportunity_keys <- function(x) {
  if (is.null(x) || is.na(x) || !nzchar(x)) return(character(0))
  keys <- unlist(str_split(x, "\\s*,\\s*"))
  keys <- keys[nzchar(keys)]
  bad <- setdiff(keys, OPP_KEYS)
  if (length(bad) > 0) {
    stop(glue(
      "Unknown --opportunity key(s): {paste(bad, collapse = ', ')}.\n",
      "Allowed: {paste(OPP_KEYS, collapse = ', ')}"
    ), call. = FALSE)
  }
  unique(keys)
}
