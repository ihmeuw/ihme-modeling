library(data.table)
library(parallel)
library(reshape2)
library(
  "ensemble.density",
  lib.loc = 
)

# Input validation --------------------------------------------------------

args = commandArgs(trailingOnly = TRUE)

rei_id = as.integer(args[1])
location_id = as.integer(args[2])
version_id = as.integer(args[3])

# High FPG, high SBP are the only two risks this script is expected to be
# used for
edensity_rei_ids <- c(105, 107)
if (!(rei_id %in% edensity_rei_ids)) {
  stop("Unexpected rei_id given: ", rei_id)
}

# UPDATE IF FILE PATTERNS EVER CHANGE
output_dir <- 
exposure_file <- 
weights_file <- 
output_file <- 

# Calculate risk prevalence -----------------------------------------------

exposure <- fread(exposure_file)

this_rei_id <- rei_id
weights <- fread(weights_file)[rei_id == this_rei_id]

# Function is largely copied from original R version
calc_prevalence <- function(i, weights, exposure) {
  i_exposure <- exposure[i, ]
  i_weights <- weights[
    age_group_id == i_exposure$age_group_id & sex_id == i_exposure$sex_id,
  ][, c("location_id", "age_group_id", "sex_id") := NULL][1, ]
  exposure_threshold <- i_exposure$threshold
  dens <- ensemble.density::get_edensity(
    i_weights, i_exposure$exp_mean, i_exposure$exp_sd
  )

  # Calculate area above threshold (assuming exposure isn't protective)
  return(pracma::trapz(dens$x[dens$x > exposure_threshold], dens$fx[dens$x > exposure_threshold]))
}

calc_prevalence_compiled <- compiler::cmpfun(calc_prevalence)
prevalence <- parallel::mclapply(
  1:nrow(exposure), calc_prevalence_compiled, weights = weights,
  exposure = exposure, mc.cores = 2
)

# Format and save ---------------------------------------------------------

prevalence <- cbind(exposure, prevalence = unlist(prevalence))[
  , .(location_id, year_id, age_group_id, sex_id, draw, prevalence)
]

prevalence <- reshape2::dcast(
  prevalence, location_id + year_id + age_group_id + sex_id ~ draw, value.var = "prevalence"
)
readr::write_csv(prevalence, output_file)
