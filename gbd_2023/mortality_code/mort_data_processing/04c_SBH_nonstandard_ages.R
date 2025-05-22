
## Meta ------------------------------------------------------------------------

# Description: Create SBH dataset with only GBD nonstandard groups

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and format SBH data from 5q0
#   4. Save

# Load libraries ---------------------------------------------------------------

library(assertable)
library(data.table)
library(demInternal)
library(dplyr)
library(mortdb)
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

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# read in ax to inform 5q0 to 5m0 calculation
lt_ax <- demInternal::get_dem_outputs(
  "no shock life table estimate",
  run_id = nslt_version,
  gbd_year = 2023,
  age_group_ids = c(2, 3, 388, 389, 238, 34),
  estimate_stage_id = 5,
  life_table_parameter_ids = 2,
  name_cols = TRUE
)
setnames(lt_ax, "mean", "ax")
lt_ax[, age_ax := age_start + ax]

lt_ax <- merge(
  lt_ax[, c("location_id", "year_id", "sex_id", "age_group_id", "ax", "age_ax")],
  pop[, c("location_id", "year_id", "sex_id", "age_group_id", "population")],
  c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

lt_ax[, ax_avg := weighted.mean(age_ax, population), by = c("location_id", "year_id", "sex_id")]

# Read in and format SBH data from 5q0 -----------------------------------------

child_ns_data <- mortdb::get_mort_outputs(
  model_name = "5q0",
  model_type = "data",
  run_id = data_5q0_version,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  demographic_metadata = TRUE
)

child_ns_data_adj <- mortdb::get_mort_outputs(
  model_name = "5q0 bias adjustment",
  model_type = "estimate",
  run_id = data_5q0_bias_adj_version,
  gbd_year = gbd_year,
  demographic_metadata = TRUE
)

child_ns_data <- child_ns_data[outlier != 1]

# subset to SBH data
sbh_ns_data <- child_ns_data[method_name %like% "SBH"]
setnames(sbh_ns_data, c("mean", "method_name"), c("qx", "source_type_name"))

sbh_ns_data[, source_type_name := "SBH"]

sbh_ns_data <- sbh_ns_data[, c(
  "location_id", "year_id", "sex_id", "age_group_id", "nid", "underlying_nid",
  "source_type_name", "qx", "source", "upload_5q0_data_id"
)]

sbh_ns_data <- sbh_ns_data[order(nid, underlying_nid, source, location_id, year_id)]

sbh_ns_data[, id := paste0(nid, "_", underlying_nid, "_", location_id, "_", year_id)]
sbh_ns_data[, source := NULL]

sbh_ns_data_dups <- sbh_ns_data[duplicated(sbh_ns_data$id),]
sbh_ns_data <- sbh_ns_data[!duplicated(sbh_ns_data$id),]
sbh_ns_data[, id := NULL]

# replace with bias adjusted qx
sbh_ns_data <- merge(
  sbh_ns_data,
  child_ns_data_adj[, c("upload_5q0_data_id", "adjust_mean", "variance")],
  by = "upload_5q0_data_id",
  all.x = TRUE
)

# drop 165 rows of SBH with no adjustment
sbh_ns_data <- sbh_ns_data[!is.na(adjust_mean)]

# drop old ap since it's not in pop and ax below
sbh_ns_data <- sbh_ns_data[location_id != 44849]

setnames(sbh_ns_data, "adjust_mean", "qx_adj")
sbh_ns_data[, diff := qx_adj - qx]
sbh_ns_data[, per_diff := (qx_adj - qx) / qx * 100]

readr::write_csv(
  sbh_ns_data,
  fs::path("FILEPATH")
)

sbh_ns_data[location_id == 164 & nid == 9230, qx_adj := qx] # prevent bias adjustment to NPL 1976 WFS
sbh_ns_data[location_id == 169 & nid == 140201 & underlying_nid == 2224, qx_adj := qx] # prevent bias adjustment to CAF 1975 Census
sbh_ns_data[location_id == 206 & nid == 140201 & underlying_nid == 3938, qx_adj := qx] # prevent bias adjustment to GMB 1973 Census

sbh_ns_data <- sbh_ns_data[, !c("qx", "diff", "per_diff")]
setnames(sbh_ns_data, c("qx_adj", "variance"), c("qx", "qx_se"))

sbh_ns_data[, sample_size := (qx - qx^2) / qx_se^2]

# fix nids and underlying_nids
sbh_ns_data[nid == 5360, nid := 5376] # IDN 2005 SUSENAS
sbh_ns_data[nid == 5379, nid := 5401] # IDN 2006 SUSENAS
sbh_ns_data[nid == 6827, nid := 6842] # IDN 2001 SUSENAS
sbh_ns_data[nid == 6956, nid := 6970] # IDN 2007 SUSENAS
sbh_ns_data[nid == 43518, nid := 43526] # IDN 2008 SUSENAS
sbh_ns_data[nid == 43549, nid := 43552] # IDN 2009 SUSENAS

sbh_ns_data[
  location_id == 51 & underlying_nid == 11036,
  underlying_nid := 11013
] # POL 1970 Census
sbh_ns_data[
  location_id == 298 & nid == 140201 & underlying_nid == 140201,
  underlying_nid := NA
] # ASM

# check for and outlier duplicates
sbh_ns_data[, outlier := 0]
sbh_ns_data[nid == 1175, outlier := 1] # BFA 2005 Census
sbh_ns_data[nid == 105262 & underlying_nid == 8687, outlier := 1] # FSM 1994 Census

nid_overlap <- sbh_ns_data[
  underlying_nid %in% unique(sbh_ns_data[outlier == 0]$nid) & outlier == 0
]
if (nrow(nid_overlap) > 0) {

  nid_overlap_test <- sbh_ns_data[nid %in% nid_overlap$underlying_nid]

  nid_overlap[, nid_loc := paste0(underlying_nid, "_", location_id)]
  nid_overlap_test[, nid_loc := paste0(nid, "_", location_id)]

  if (nrow(nid_overlap[nid_loc %in% nid_overlap_test$nid_loc]) != 0) {
    stop("Review nids with duplicate data assigned to same underlying_nid.")
  }

}

# cap sample size
sbh_ns_data <- merge(
  sbh_ns_data,
  pop[, c("location_id", "year_id", "sex_id", "age_group_id", "population")],
  by = c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

sbh_ns_data[sample_size > population * 0.0375, sample_size := population * 0.0375]
sbh_ns_data[, population := NULL]

sbh_ns_data[, qx_se := sqrt((qx * (1 - qx)) / sample_size)]

# calculate mx using ax values
sbh_ns_data <- merge(
  sbh_ns_data,
  unique(lt_ax[, c("year_id", "location_id", "sex_id", "ax_avg")]),
  by = c("year_id", "location_id", "sex_id"),
  all.x = TRUE
)
sbh_ns_data[, mx := demCore::qx_ax_to_mx(qx, ax_avg, 5)]
sbh_ns_data[, mx_se := sqrt((mx * (1 - mx)) / sample_size)]

sbh_ns_data[, mx_no_ax := demCore::qx_to_mx(qx, 5)]

sbh_ns_data[, diff := mx - mx_no_ax]
sbh_ns_data[, per_diff := (mx - mx_no_ax) / mx_no_ax * 100]

sbh_ns_data <- sbh_ns_data[,
  !c("upload_5q0_data_id", "qx_se")
]

readr::write_csv(
  sbh_ns_data,
  fs::path("FILEPATH")
)
sbh_ns_data <- sbh_ns_data[,
  !c("qx", "ax_avg", "mx_no_ax", "diff", "per_diff")
]

demUtils::assert_is_unique_dt(
  sbh_ns_data,
  id_cols = setdiff(names(sbh_ns_data), c("mx", "mx_se", "sample_size"))
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

mx_comparison <- quick_mx_compare(
  test_file = sbh_ns_data,
  compare_file = prev_version,
  id_cols = c(
    "location_id", "year_id", "nid", "sex_id", "age_group_id", "source_type_name",
    "underlying_nid"
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
  sbh_ns_data,
  fs::path("FILEPATH")
)
