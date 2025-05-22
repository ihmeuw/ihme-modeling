################################################################################
## Purpose:   Prepare RhoD sales data as RhoD sales to live births proportion
##            to be used as RhoD modeling input
## Input:     RhoD sales from Marketing Research Bureau (NID:539703)
##            Bundle ID: 4715, Bundle Version ID: 43365
## Output:    FILEPATH
################################################################################

# Set up -----------------------------------------------------------------------
`%>%` <- magrittr::`%>%`
library('openxlsx')
library('data.table')
library('ggplot2')

# read in bundle ---------------------------------------------------------------

bundle <- ihme::get_bundle_version(bundle_version_id = 43365)
bundle <- bundle[, .(location_id, year_id, sum_vials, Estimate, nid)]

# pull live births -------------------------------------------------------------

live_births <- ihme::get_covariate_estimates(
  nch::id_for("covariate", "Live Births (thousands)"),
  sex_id = nch::id_for("sex", "Both"),
  year_id = bundle$year_id,
  location_id = bundle$location_id,
  release_id = 16
)[, .(location_id, year_id, sex_id, mean_value)]

data.table::setnames(live_births, 'mean_value', 'births')

live_births[, births := births * 1000] #multiply by 1000 because covariate 60 is in thousands

# calculate rhod to live births proportion -------------------------------------

rhod_prop <- merge(
  bundle,
  live_births,
  by = c("location_id", "year_id"),
  all.x = TRUE,
  all.y = FALSE
)

rhod_prop <- rhod_prop %>%
  dplyr::mutate(rhod_livebirths_prop = sum_vials / births)

# save csv ---------------------------------------------------------------------
data.table::fwrite(
  rhod_prop,
  fs::path(
    "FILEPATH"
  )
)
