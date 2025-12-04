# install.packages(c("jsonlite", "paws"))  # if needed
library(jsonlite)

sso_useenv <- function(profile = "cities-data-dev", region = "us-east-1", refresh_margin_secs = 300) {
  # Ensure PATH includes Homebrew bin (macOS)
  if (.Platform$OS.type == "unix" && dir.exists("/opt/homebrew/bin") &&
      !grepl("/opt/homebrew/bin", Sys.getenv("PATH"), fixed = TRUE)) {
    Sys.setenv(PATH = paste("/opt/homebrew/bin", Sys.getenv("PATH"), sep = ":"))
  }
  
  # Always ok to call; it's a no-op if already logged in
  system2("aws", c("sso", "login", "--profile", profile))
  
  # Get JSON creds from SSO
  out <- system2("aws", c("sso", "credential-process", "--profile", profile),
                 stdout = TRUE, stderr = TRUE)
  txt <- paste(out, collapse = "\n")
  if (!nzchar(txt) || grepl("^\\s*usage: aws\\b", txt)) {
    stop("Failed to mint SSO creds. Make sure CLI v2 is on PATH for R and the profile exists.")
  }
  
  j <- fromJSON(txt)
  # Set env vars for this R session
  Sys.unsetenv(c("AWS_PROFILE","AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY","AWS_SESSION_TOKEN"))
  Sys.setenv(
    AWS_ACCESS_KEY_ID     = j$AccessKeyId,
    AWS_SECRET_ACCESS_KEY = j$SecretAccessKey,
    AWS_SESSION_TOKEN     = j$SessionToken,
    AWS_REGION            = region,
    AWS_DEFAULT_REGION    = region
  )
  
  # Optional: return an auto-refresh closure you can call before long ops
  exp <- as.POSIXct(j$Expiration, tz = "UTC")
  function() {
    if (difftime(exp, Sys.time(), units = "secs") < refresh_margin_secs) {
      sso_useenv(profile = profile, region = region, refresh_margin_secs = refresh_margin_secs)
    } else {
      invisible(TRUE)
    }
  }
}

# ---- Use it ----
# refresh <- sso_useenv("cities-data-dev", "us-east-1")
# 
# # Verify with STS then use S3 (works with paws *and* aws.s3)
# library(paws)
# sts <- paws::sts()
# print(sts$get_caller_identity())
# 
# s3 <- paws::s3()
# print(s3$list_buckets())

# If you’re running a long job, call `refresh()` right before critical API calls:
# refresh()
# ... your code ...
