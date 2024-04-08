
# Meta --------------------------------------------------------------------

# Description: Downloads/compiles all inputs needed to run backend steps of
#   the covid EM pipeline, like diagnostics. Primarily includes comparators.
# Steps:
#   1. Downloads comparators (previous results)
# Inputs:
#   * Detailed configuration file.
# Outputs:
#   * Comparators
#   * GBD death number

# Load libraries ----------------------------------------------------------

library(argparse)
library(assertable)
library(data.table)
library(demInternal)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
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

process_locations <- fread(paste0(main_dir, "/inputs/process_locations.csv"))


# Comparators -------------------------------------------------------------

gbd <- demInternal::get_dem_outputs(
  process_name = "with shock death number estimate",
  run_id = run_id_with_shock_death_number_estimate,
  gbd_year = gbd_year,
  location_ids = process_locations[(is_estimate_2), location_id],
  year_ids = estimation_year_start:estimation_year_end,
  estimate_stage_ids = 6, # final w/ shocks
  name_cols = T,
  odbc_section = odbc_section,
  odbc_dir = odbc_dir
)

setnames(gbd, "sex_name", "sex")

gbd <- gbd[, .SD,
           .SDcols = c(setdiff(id_cols, c("time_start", "time_unit")),
                       "mean", "lower", "upper")
           ]
gbd[, type := "gbd"]

readr::write_csv(
  gbd, file = paste0(main_dir, "/inputs/gbd_death_number.csv")
)


# Previous results ---------------------------------------------------------

# get from database
previous_results <- rbindlist(lapply(compare_run_ids, function(id) {
  dir <- paste0(base_dir, "/", id, "/outputs/summaries/")
  dt <- assertable::import_files(filenames = list.files(dir), folder = dir)
  dt[, type := paste0(id, " ", model_type)]
  dt <- dt[, .SD, .SDcols = c(id_cols, "type", "mean")]
  return(dt)
}))

# save
readr::write_csv(
  x = previous_results,
  file = paste0(main_dir, "/inputs/comparators.csv")
)

# External excess mortality estimates ------------------------------------------

# get from covid data run id
ext_dir <- paste0(base_dir_data, external_id_covid_em_data, "/outputs/external")
ext_em <- assertable::import_files(
  filenames = list.files(ext_dir, pattern = "*.csv"),
  folder = ext_dir
)

readr::write_csv(
  ext_em, file = paste0(main_dir, "/inputs/external_em.csv")
)
