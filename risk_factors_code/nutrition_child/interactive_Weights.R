os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "J:/"
} else {
  jpath <- "/home/j/"
}

# set directories

# source scripts
source("FILEPATH/fit_submit.r")

# install packages

############################################################################################

fit_ensemble_weights("wasting", project="proj_ensemble_models", slots = 48)
fit_ensemble_weights("stunting", project = "proj_ensemble_models", slots = 48)
fit_ensemble_weights("underweight", project = "proj_custom_models", slots = 48)