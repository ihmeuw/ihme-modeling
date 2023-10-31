#' download the full sets of results for COVID and OPRM draws for 2020:2021
#' Format and save these.

# Load packages -----------------------------------------------------------

library(data.table)

# load CC functions
source("FILEPATH")
source("FILEPATH")


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "Output directory for em adjustment"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Load Data ---------------------------------------------------------------


# indirect causes, covid, and positive OPRM
cause_ids <- c(322, 339, 341, 1048, 1058)

# Load Central comp inputs: covid deaths, oprm
cc_best_versions <- get_best_model_versions(
  entity = "cause",
  ids = cause_ids,
  gbd_round_id = 7,
  decomp_step = "iterative"
) |> setDT()

# # HOTFIX:
# # measles contains 2 different model types, exlcude the one not from CC
cc_best_versions <- cc_best_versions[!(cause_id == 341 & model_version_type_id == 5)]

dt_cc_draws <- lapply(1:nrow(cc_best_versions), function(i) {
  get_draws(
    gbd_id_type = "cause_id",
    gbd_id = cc_best_versions[i, cause_id],
    location_id = loc_id,
    year_id = current_year,
    version_id = cc_best_versions[i, model_version_id],
    source="codem",
    gbd_round_id = 7,
    decomp_step = "iterative"
  )
}) |> rbindlist()

