
# Meta --------------------------------------------------------------------

# Description: Prep meta.yaml for 02b step
# Steps:
#   1. Uses input data to determine which age groups and locations to use
#   2. Uses config file to determine "col_data", "time_unit", "time_start",
#       "time_end_0", and "time_end_1"
# Inputs:
#   * Data (location specific)
#   * Config
#   * Meta default
# Outputs:
#   * Meta prepped


# Load libraries ----------------------------------------------------------

library(argparse)
library(data.table)
library(fs)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
parser$add_argument(
  "--ts", type = "integer", required = !interactive(),
  help = "tail size in months"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0(main_dir, "/covid_em_detailed.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# convert tail size in months to weeks
tail_size_month <- ts
tail_size_week <- round(tail_size_month * (52/12))


# Inputs ------------------------------------------------------------------

# load location mapping
process_locations <- fread(paste0(main_dir, "/inputs/process_locations.csv"))
ihme_locs <- process_locations[(is_estimate_1), ihme_loc_id]

# load config
meta <- yaml::read_yaml(fs::path(code_dir, "meta.yaml"))

# load data
data <- assertable::import_files(
  filenames = paste0(ihme_locs, ".csv"),
  folder = fs::path(main_dir, "/inputs/data_all_cause/")
)
data <- data[!is.na(deaths)]


# Prep location-specific age group info -----------------------------------

# for each location, add age and sex groups to meta
for (loc in ihme_locs) {

  loc_id <- process_locations[ihme_loc_id == loc, location_id]

  ages <- unique(data[location_id == loc_id, age_name])
  if (loc %in% c("LUX", "ISL")) {
    ages <- setdiff(ages, c("0 to 1", "1 to 5", "0 to 5", "5 to 10", "10 to 15"))
  }
  if (loc %in% c("MLT")) {
    ages <- setdiff(ages, c("10 to 15"))
  }
  meta[[(loc)]]$age_groups <- ages

  sexes <- unique(data[location_id == loc_id, sex])
  meta[[(loc)]]$sex_groups <- sexes

}

# for each location, add location-specific time cutoff
if (use_loc_specific_time_cutoff) {
  for (loc in ihme_locs) {

    loc_id <- process_locations[ihme_loc_id == loc, location_id]
    year_cutoff <- unique(data[location_id == loc_id, year_cutoff])
    time_cutoff <- unique(data[location_id == loc_id, time_cutoff])

    meta[[loc]]$time_end_0$year <- year_cutoff
    meta[[loc]]$time_end_0$detailed <- time_cutoff

  }
}


# Prep other meta.yaml variables ------------------------------------------

# default time units
meta$default$time_end_0$year <- min(covid_years)
meta$default$time_end_1$year <- max(covid_years)
if (run_time_units == "month") {
  meta$default$time_unit <- "month"
  meta$default$time_end_0$detailed <- 2
  meta$default$time_end_1$detailed <- 13
  meta$default$tail_size <- tail_size_month

} else if (run_time_units == "week") {
  meta$default$time_unit <- "week"
  meta$default$time_end_0$detailed <- 8
  meta$default$time_end_1$deatiled <- 53
  meta$default$tail_size <- tail_size_week

} else if (run_time_units == "auto") {
  # in this case, drop monthly data for now
  meta$default$time_unit <- "week"
  meta$default$time_end_0$detailed <- 8
  meta$default$time_end_1$detailed <- 53
  meta$default$tail_size <- tail_size_week

  monthly_loc_ids <- unique(data[time_unit == "month", location_id])
  monthly_ihme_locs <- process_locations[location_id %in% monthly_loc_ids, ihme_loc_id]
  for (loc in monthly_ihme_locs) {
    meta[[(loc)]]$time_end_1$year <- max(covid_years)
    meta[[(loc)]]$time_end_1$detailed <- 13
    meta[[(loc)]]$time_unit <- "month"
    meta[[(loc)]]$tail_size <- tail_size_month
  }
}

# knots per year
meta$default$knots_per_year <- knots_per_year


# Save --------------------------------------------------------------------

yaml::write_yaml(
  meta,
  paste0(main_dir, "/inputs/data_all_cause/meta_", ts, ".yaml")
)
