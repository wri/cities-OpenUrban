# Fresh R session
install.packages("reticulate")  # if needed
library(reticulate)

env <- "open-urban"

# Ensure Conda is available
conda_bin <- tryCatch(conda_binary(), error = function(e) NULL)
if (is.null(conda_bin) || !nzchar(conda_bin)) {
  install_miniconda()
  conda_bin <- conda_binary()
}

# Create env if missing, then install deps
if (!any(conda_list()$name == env)) {
  conda_create(envname = env, python_version = "3.12")
  conda_install(envname = env, packages = c("pip", "dask", "graphviz"), channel = "conda-forge")
  # EE API + anything else you need
  py_install(c("earthengine-api"), envname = env, pip = TRUE)
  # If your code imports city_metrix, install it if it’s a package:
  # py_install("git+https://github.com/wri/city_metrix.git", envname = env, pip = TRUE)
  # You already mentioned cities-cif in your YAML; install that too if needed:
  # py_install("git+https://github.com/wri/cities-cif.git", envname = env, pip = TRUE)
}

# Point this R session at that env (do this before importing Python modules)
use_condaenv(env, required = TRUE)

# Authenticate Earth Engine in THIS env (browser flow), then set the default project.
# You'll do these two commands once per machine/account:
# system2(conda_bin, c("run","-n", env, "earthengine", "authenticate"))
# system2(conda_bin, c("run","-n", env, "earthengine", "set_project", "wri-earthengine"))
# 
# py_run_string("
# import boto3
# _real = boto3.Session
# def _patched_Session(*args, **kwargs):
#     kwargs.pop('profile_name', None)  # ignore hard-coded profiles
#     return _real(*args, **kwargs)
# boto3.Session = _patched_Session
# ")
# 
# py_run_file("get_data.py")
# 

# Run in python
# conda activate open-urban
# earthengine authenticate --auth_mode=notebook
# 
# earthengine set_project wri-earthengine
# export EARTHENGINE_PROJECT=wri-earthengine

# conda <- conda_binary()
# system2(conda, c("run", "-n", "open-urban", "python", "~/Documents/github/cities-OpenUrban/get_data.py"))
