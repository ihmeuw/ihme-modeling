
## Meta ------------------------------------------------------------------------

# Description: Create SIBS dataset with only GBD nonstandard groups

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and format SIBS data from 45q15
#   4. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(tidyr)
library(tidyverse)

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

source(fs::path("FILEPATH"))

# Read in internal inputs ------------------------------------------------------

age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

# read in files for sibs sample size & standard error calculations
adult_data_meta <- fread(
  fs::path("FILEPATH")
)

# read in gbd ages SIBS
sibs_gbd_ages <- fread(
  fs::path("FILEPATH")
)

# Read in and format SIBS data from 45q15 --------------------------------------

adult_ns_data <- mortdb::get_mort_outputs(
  model_name = "45q15",
  model_type = "data",
  run_id = data_45q15_version,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  demographic_metadata = TRUE
)

adult_ns_data <- adult_ns_data[outlier != 1]

sibs_ns_data_orig <- adult_ns_data[method_name == "Sibs"]
sibs_ns_data <- sibs_ns_data_orig[adjustment == 0 & !(nid %in% sibs_gbd_ages$nid)]

# check for and append on missing data by location
sibs_gbd_ages[, nid_loc := paste(nid, location, sep = "_")]
sibs_ns_data_orig[, nid_loc := paste(nid, location_id, sep = "_")]
sibs_ns_data[, nid_loc := paste(nid, location_id, sep = "_")]

sibs_ns_data_missing_locs <- sibs_ns_data_orig[
  !(nid_loc %in% unique(c(sibs_gbd_ages$nid_loc, sibs_ns_data$nid_loc))) &
    adjustment == 0
]

sibs_ns_data <- unique(rbind(sibs_ns_data, sibs_ns_data_missing_locs))
sibs_ns_data[, nid_loc := NULL]

sibs_ns_data <- sibs_ns_data[location_id %in% locs$location_id]

setnames(sibs_ns_data, c("mean", "method_name"), c("qx", "source_type_name"))

sibs_ns_data[, source_type_name := stringr::str_to_upper(source_type_name)]

sibs_ns_data[, age_length := age_group_years_end - age_group_years_start]
sibs_ns_data[, mx := demCore::qx_to_mx(qx, age_length - 1)]

sibs_ns_data <- sibs_ns_data[, c(
  "location_id", "year_id", "sex_id", "age_group_id", "nid", "source_type_name", "mx"
)]

# read in and clean metadata
sibs_ns_data_meta <- adult_data_meta[
  category == "sibs",
  c("category", "location_id", "year", "sex", "mort", "stderr")
]
setnames(sibs_ns_data_meta, c("mort", "stderr"), c("qx", "qx_se"))

sibs_ns_data_meta[sex == "male", sex_id := 1]
sibs_ns_data_meta[sex == "female", sex_id := 2]

sibs_ns_data_meta[, year_id := floor(year)]

sibs_ns_data_meta <- sibs_ns_data_meta[, !c("category", "sex", "year")]

sibs_ns_data_meta[, qx := mean(qx), by = c("location_id", "year_id", "sex_id")]
sibs_ns_data_meta[, qx_se := mean(qx_se), by = c("location_id", "year_id", "sex_id")]

sibs_ns_data_meta <- unique(sibs_ns_data_meta)

sibs_ns_data_meta_glob <- copy(sibs_ns_data_meta)

sibs_ns_data_meta_glob[, qx := mean(qx), by = c("year_id", "sex_id")]
sibs_ns_data_meta_glob[, qx_se := mean(qx_se), by = c("year_id", "sex_id")]

sibs_ns_data_meta_glob <- unique(sibs_ns_data_meta_glob[, !c("location_id")])

sibs_ns_data_meta_glob_locs <- data.table()
for (loc in unique(sibs_ns_data$location_id)) {

  sibs_ns_data_meta_glob[, location_id := loc]

  sibs_ns_data_meta_glob_locs <- rbind(
    sibs_ns_data_meta_glob_locs,
    sibs_ns_data_meta_glob
  )

}

for (yy in 2018:2021) {

  sibs_ns_data_meta_glob_locs_temp <- sibs_ns_data_meta_glob_locs[year_id == 2017]

  sibs_ns_data_meta_glob_locs_temp[, year_id := yy]

  sibs_ns_data_meta_glob_locs <- rbind(
    sibs_ns_data_meta_glob_locs,
    sibs_ns_data_meta_glob_locs_temp
  )

}

sibs_ns_data_meta[, loc_yr_sex := paste0(location_id, "_", year_id, "_", sex_id)]
sibs_ns_data_meta_glob_locs[, loc_yr_sex := paste0(location_id, "_", year_id, "_", sex_id)]

sibs_ns_data_meta_glob_locs <- sibs_ns_data_meta_glob_locs[!(loc_yr_sex %in% sibs_ns_data_meta$loc_yr_sex)]

sibs_ns_data_meta[, loc_yr_sex := NULL]
sibs_ns_data_meta_glob_locs[, loc_yr_sex := NULL]

sibs_ns_data_meta <- rbind(
  sibs_ns_data_meta,
  sibs_ns_data_meta_glob_locs
)
sibs_ns_data <- merge(
  sibs_ns_data,
  sibs_ns_data_meta,
  by = c("location_id", "sex_id", "year_id"),
  all.x = TRUE
)

sibs_ns_data[, sample_size := (qx - qx^2) / qx_se^2]
sibs_ns_data[, mx_se := sqrt((mx * (1 - mx)) / sample_size)]
sibs_ns_data[, underlying_nid := NA]

sibs_ns_data <- sibs_ns_data[, !c("qx", "qx_se")]

demUtils::assert_is_unique_dt(
  sibs_ns_data,
  id_cols = setdiff(names(sibs_ns_data), c("mx", "mx_se", "sample_size"))
)

# Save -------------------------------------------------------------------------

# compare mx to previous version
prev_version <-  fread(
  fs::path_norm(
    fs::path(
      "FILEPATH"
    )
  )
)

test_file <- copy(sibs_ns_data)
mx_comparison <- quick_mx_compare(
  test_file = test_file[, outlier := 0],
  compare_file = prev_version[, outlier := 0],
  id_cols = c(
    "location_id", "year_id", "nid", "underlying_nid", "sex_id", "age_group_id",
    "source_type_name"
  ),
  warn_only = TRUE
)

readr::write_csv(
  mx_comparison,
  fs::path(
    "FILEPATH"
  )
)

readr::write_csv(
  sibs_ns_data,
  fs::path("FILEPATH")
)
