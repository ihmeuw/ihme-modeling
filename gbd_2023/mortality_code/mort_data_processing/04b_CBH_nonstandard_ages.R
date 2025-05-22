
## Meta ------------------------------------------------------------------------

# Description: Create CBH dataset with only GBD nonstandard groups

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and format CBH data from 5q0
#   4. Read in and format CBH 5q0 data for 16-25 years before survey from prep
#   5. Save

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

# read in neonatal CBH
cbh_nn_ages <- fread(
  fs::path("FILEPATH")
)

# read in enn and lnn CBH
cbh_enn_lnn <- fread(
  fs::path("FILEPATH")
)

# read in gbd ages CBH
cbh_gbd_ages <- fread(
  fs::path("FILEPATH")
)

# read in gbd ages Census_Survey
census_survey_gbd_ages <- fread(
  fs::path("FILEPATH")
)

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

# Read in and format CBH data from 5q0 -----------------------------------------

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

# subset to CBH
cbh_ns_data_orig <- child_ns_data[method_name == "CBH"]

# compile missing data by age_group_id
cbh_ns_data_nn <- cbh_ns_data_orig[
  !(nid %in% cbh_gbd_ages[age_group_id %in% 2:3 & outlier == 0]$nid)
]
cbh_ns_data_older_ages <- cbh_ns_data_orig[
  !(nid %in% cbh_gbd_ages[!(age_group_id %in% 2:3) & outlier == 0]$nid)
]
cbh_ns_data <- unique(
  rbind(cbh_ns_data_nn, cbh_ns_data_older_ages)
)

# append on cbh reports for missing nids
cbh_ns_data_rep <- child_ns_data[
  method_name == "Dir-Unadj" & !(source_name %in% c("VR", "SRS", "DSP", "DSS", "MCCD", "Census")) &
    !(nid %in% unique(c(cbh_gbd_ages$nid, census_survey_gbd_ages$nid, cbh_ns_data$nid)))
]

cbh_ns_data_rep <- cbh_ns_data_rep[
  !(nid %in% c(
    28010, 56554, 93395, 118426, 121015, 131249, 136660, 138539, 162330:162333,
    148298, 260556, 317832, 399470, 450996, 465152
  )) & !(underlying_nid %in% c(148325, 312183))
] # drop "Other" data that's not surveys

cbh_ns_data_rep <- cbh_ns_data_rep[
  !(nid %in% c(24006, 404407))
] # MED ENADID

cbh_ns_data_rep <- cbh_ns_data_rep[
  !(nid == 27338 & source == "cdc rhs statcompiler")
] # AZE 2001 RHS duplicate

cbh_ns_data_rep[
  nid == 308316, location_id := 53617
] # PAK G-B MICS

cbh_ns_data_rep[, method_name := "CBH"]

cbh_ns_data <- rbind(
  cbh_ns_data,
  cbh_ns_data_rep
)

# check for and append on missing data by location
cbh_gbd_ages[, nid_loc := paste(nid, location, sep = "_")]
cbh_ns_data_orig[, nid_loc := paste(nid, location_id, sep = "_")]
cbh_ns_data[, nid_loc := paste(nid, location_id, sep = "_")]

cbh_ns_data_missing_locs <- cbh_ns_data_orig[
  !(nid_loc %in% unique(c(cbh_gbd_ages$nid_loc, cbh_ns_data$nid_loc)))
]

cbh_ns_data <- unique(rbind(cbh_ns_data, cbh_ns_data_missing_locs))
cbh_ns_data[, nid_loc := NULL]

# subset to only locations in hierarchy
cbh_ns_data <- cbh_ns_data[location_id %in% locs$location_id]

setnames(cbh_ns_data, c("mean", "method_name"), c("qx", "source_type_name"))

# replace with bias adjusted qx
cbh_ns_data <- merge(
  cbh_ns_data[, !c("outlier", "outlier_comment", "variance", "adjustment_re_fe", "reference", "adjustment")],
  child_ns_data_adj[, c("upload_5q0_data_id", "adjust_mean", "variance")],
  by = "upload_5q0_data_id",
  all.x = TRUE
)

setnames(cbh_ns_data, "adjust_mean", "qx_adj")
cbh_ns_data[, diff := qx_adj - qx]
cbh_ns_data[, per_diff := (qx_adj - qx) / qx * 100]

readr::write_csv(
  cbh_ns_data,
  fs::path("FILEPATH")
)

cbh_ns_data <- cbh_ns_data[, !c("qx", "sd", "diff", "per_diff")]
setnames(cbh_ns_data, c("qx_adj", "variance"), c("qx", "qx_se"))

cbh_ns_data[, sample_size := (qx - qx^2) / qx_se^2]

# cap sample size
cbh_ns_data <- merge(
  cbh_ns_data,
  pop[, c("location_id", "year_id", "sex_id", "age_group_id", "population")],
  by = c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

cbh_ns_data[sample_size > population * 0.05, sample_size := population * 0.05]
cbh_ns_data[, population := NULL]

cbh_ns_data[, qx_se := sqrt((qx * (1 - qx)) / sample_size)]

# calculate mx using ax values
cbh_ns_data <- merge(
  cbh_ns_data,
  unique(lt_ax[, c("year_id", "location_id", "sex_id", "ax_avg")]),
  by = c("year_id", "location_id", "sex_id"),
  all.x = TRUE
)

cbh_ns_data[, mx := demCore::qx_ax_to_mx(qx, ax_avg, 5)]
cbh_ns_data[, mx_se := sqrt((mx * (1 - mx)) / sample_size)]

cbh_ns_data[, mx_no_ax := demCore::qx_to_mx(qx, 5)]

cbh_ns_data[, diff := mx - mx_no_ax]
cbh_ns_data[, per_diff := (mx - mx_no_ax) / mx_no_ax * 100]

cbh_ns_data <- cbh_ns_data[,
  !c("upload_5q0_data_id", "qx_se")
]

readr::write_csv(
  cbh_ns_data,
  fs::path("FILEPATH")
)

cbh_ns_data <- cbh_ns_data[, c(
  "location_id", "year_id", "sex_id", "age_group_id", "source_type_name",
  "nid", "underlying_nid", "mx", "mx_se", "sample_size"
)]

# add on neonatal age group data from prep
cbh_ns_data_nn <- cbh_nn_ages[
  dem_measure_id %in% c(19, 13) & !(nid %in% unique(cbh_enn_lnn$nid))
]

cbh_ns_data_nn[, source_type_name := "CBH"]
cbh_ns_data_nn[, year_id := as.numeric(substr(date_start, 1, 4))]

setnames(cbh_ns_data_nn, "location", "location_id")

cbh_ns_data_nn <- cbh_ns_data_nn[,
  c("nid", "location_id", "year_id", "sex_id", "age_group_id",
    "source_type_name", "dem_measure_id", "value", "uncertainty_value")
]
cbh_ns_data_nn[, underlying_nid := NA]

cbh_ns_data_nn_qx <- cbh_ns_data_nn[dem_measure_id == 19]
setnames(
  cbh_ns_data_nn_qx,
  c("value", "uncertainty_value"),
  c("mx", "mx_se")
)

cbh_ns_data_nn_pys <- cbh_ns_data_nn[dem_measure_id == 13]
setnames(
  cbh_ns_data_nn_pys,
  c("value", "uncertainty_value"),
  c("sample_size", "sample_size_se")
)

cbh_ns_data_nn_final <- merge(
  cbh_ns_data_nn_qx[, !c("dem_measure_id")],
  cbh_ns_data_nn_pys[, !c("dem_measure_id", "sample_size_se")],
  by = c(
    "nid", "underlying_nid", "location_id", "year_id", "sex_id", "age_group_id",
    "source_type_name"
  ),
)

cbh_ns_data <- rbind(
  cbh_ns_data,
  cbh_ns_data_nn_final
)

# Read in and format 5q0 data for 16-25 years before survey from prep ----------

prep_ds_onemod_cbh_extended <- arrow::open_dataset(
  sources = fs::path("FILEPATH")
)

cbh_data_extended <- prep_ds_onemod_cbh_extended |>
  dplyr::filter(dem_measure_id %in% c(19, 20, 13)) |> # nmx, nqx, p-ys
  dplyr::filter(collection_method_id == 1) |> # cbh
  dplyr::filter(date_format != "years_preceding_collection") |>
  dplyr::filter(location_type == "location_id") |>
  dplyr::collect() |>
  data.table()

cbh_data_extended <- cbh_data_extended[, -c("other", "other_type", "other_granularity", "reg_day_missing", "event")]

cbh_data_extended <- merge(
  cbh_data_extended,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)

cbh_data_extended <- cbh_data_extended[age_group_id == 1] # subset to under-5
cbh_data_extended <- cbh_data_extended[!(series_title %like% "ENADID")] # drop MEX ENADID

cbh_data_mx_extended <- cbh_data_extended[
  dem_measure_id == 19,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value", "uncertainty_value")
]
cbh_data_qx_extended <- cbh_data_extended[
  dem_measure_id == 20,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value", "uncertainty_value")
]
cbh_data_pys_extended <- cbh_data_extended[
  dem_measure_id == 13,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value")
]

setnames(cbh_data_mx_extended, "value", "mx")
setnames(cbh_data_qx_extended, "value", "qx")
setnames(cbh_data_pys_extended, "value", "sample_size")

setnames(cbh_data_mx_extended, "uncertainty_value", "mx_se")
setnames(cbh_data_qx_extended, "uncertainty_value", "qx_se")

cbh_data_wide_extended <- merge(
  cbh_data_mx_extended,
  cbh_data_qx_extended,
  by = c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end"),
  all = TRUE
)

cbh_data_wide_extended <- merge(
  cbh_data_wide_extended,
  cbh_data_pys_extended,
  by = c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end"),
  all = TRUE
)

cbh_data_wide_extended <- cbh_data_wide_extended[nid != 153563]
cbh_data_wide_extended <- cbh_data_wide_extended[!(is.na(mx) & is.na(qx) & sample_size == 0)] # drop rows where sample size is 0
cbh_data_wide_extended <- cbh_data_wide_extended[sex_id %in% 1:2] # drop both sexes combined rows

cbh_data_wide_extended[, location_id := as.numeric(location)]

cbh_data_wide_extended <- merge(
  cbh_data_wide_extended[location_id %in% locs$location_id],
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

cbh_data_wide_extended[, `:=` (date_start = as.numeric(substr(date_start, 1, 4)), date_end = as.numeric(substr(date_end, 1, 4)))]
cbh_data_wide_extended[, year_id := floor((date_start + date_end) / 2)]

cbh_data_wide_extended[, source_type_name := "CBH"]
cbh_data_wide_extended[, underlying_nid := NA]

cbh_data_wide_extended <- cbh_data_wide_extended[,
  c("location_id", "year_id", "sex_id", "age_group_id", "source_type_name", "nid", "underlying_nid", "mx", "mx_se", "sample_size")
]

readr::write_csv(
  cbh_data_wide_extended,
  fs::path("FILEPATH")
)

cbh_data_wide_extended[, max_year := max(year_id), by = c("nid", "location_id")]
cbh_data_wide_extended <- cbh_data_wide_extended[year_id < max_year - 15]
cbh_data_wide_extended[, max_year := NULL]

readr::write_csv(
  cbh_data_wide_extended,
  fs::path("FILEPATH")
)

years_16_25_missing <- cbh_gbd_ages[!(nid %in% cbh_data_wide_extended$nid)]
if (nrow(years_16_25_missing) > 0) {
  years_16_25_missing_nids <- as.data.table(sort(unique(years_16_25_missing$nid)))
  setnames(years_16_25_missing_nids, "V1", "nid")
  years_16_25_missing_nids <- demInternal::merge_ghdx_record_fields(years_16_25_missing_nids)
  readr::write_csv(
    years_16_25_missing_nids,
    paste0("FILEPATH"),
    na = ""
  )
  warning(paste0("These nids have cbh for more recent years but not 5q0 for 16-25 years ago: ", list(sort(unique(years_16_25_missing_nids$nid)))))
}

assertable::assert_values(
  data = cbh_data_wide_extended,
  colnames = "nid",
  test = "in",
  test_val = unique(cbh_gbd_ages$nid)
)

cbh_ns_data <- rbind(
  cbh_ns_data,
  cbh_data_wide_extended
)

demUtils::assert_is_unique_dt(
  cbh_ns_data,
  id_cols = setdiff(names(cbh_ns_data), c("mx", "mx_se", "sample_size"))
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

test_file <- copy(cbh_ns_data)
mx_comparison <- quick_mx_compare(
  test_file = test_file[, outlier := 0],
  compare_file = prev_version[, outlier := 0],
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
  cbh_ns_data,
  fs::path("FILEPATH")
)
