
## Meta ------------------------------------------------------------------------

# Description: Compile VR, CBH, SIBS, and Census_Survey datasets for nonstandard ages

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in, format, and compile CBH, SBH, SIBS, & Census_Survey data
#   4. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)

# Command line arguments -------------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Define the main directory here
if (interactive()) {
  main_dir <- "INSERT_PATH_HERE"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}

# Setup ------------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0("FILEPATH"),
  use_parent = FALSE
)
list2env(config$default, .GlobalEnv)

Sys.unsetenv("PYTHONPATH")

source(fs::path("FILEPATH"))

# Read in internal inputs ------------------------------------------------------

age_map_extended <- fread(fs::path("FILEPATH"))

# Read in, format, and compile CBH, SBH, SIBS, & Census_Survey data ------------

cbh_nonstandard <- fread(fs::path("FILEPATH"))
sbh_nonstandard <- fread(fs::path("FILEPATH"))
sibs_nonstandard <- fread(fs::path("FILEPATH"))
census_survey_nonstandard <- fread(fs::path("FILEPATH"))

# compile
survey_nonstandard <- rbind(
  cbh_nonstandard,
  sbh_nonstandard,
  sibs_nonstandard,
  census_survey_nonstandard,
  fill = TRUE
)

survey_nonstandard <- merge(
  survey_nonstandard,
  age_map_extended,
  by = "age_group_id",
  all.x = TRUE
)

survey_nonstandard[is.na(outlier), outlier := 0]

# source type and source type id are for vrp surveys only
assertable::assert_values(
  survey_nonstandard,
  colnames = colnames(
    survey_nonstandard[, !c("underlying_nid", "deaths", "source_type", "source_type_id", "extraction_source", "outlier_note")]
  ),
  test = "not_na"
)

# compare mx to previous version
prev_version <-  fread(
  fs::path_norm(
    fs::path(
      "FILEPATH"
    )
  )
)

mx_comparison <- quick_mx_compare(
  test_file = survey_nonstandard,
  compare_file = prev_version,
  id_cols = c(
    "location_id", "age_group_id", "age_start", "age_end", "nid", "underlying_nid",
    "sex_id", "year_id", "source_type_name", "source_type_id", "source_type",
    "outlier", "extraction_source"
  ),
  warn_only = TRUE
)

readr::write_csv(
  mx_comparison,
  fs::path(
    "FILEPATH"
  )
)

# Save -------------------------------------------------------------------------

readr::write_csv(
  survey_nonstandard,
  fs::path("FILEPATH")
)
