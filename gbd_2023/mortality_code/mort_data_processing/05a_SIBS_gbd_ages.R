
## Meta ------------------------------------------------------------------------

# Description: Create SIBS dataset with only GBD age groups

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and format SIBS data from prep
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

# sibs recall bias coeffs
recall_bias_coefs <- fread(
  fs::path("FILEPATH")
)

# Read in and format SIBS data from prep ---------------------------------------

prep_ds_onemod <- arrow::open_dataset(
  sources = fs::path("FILEPATH")
)

sibs_data <- prep_ds_onemod |>
  dplyr::filter(dem_measure_id %in% c(19, 20, 13)) |> # nmx, nqx, p-ys
  dplyr::filter(collection_method_id == 4) |> # sibs
  dplyr::filter(date_format != "years_preceding_collection") |>
  dplyr::filter(location_type == "location_id") |>
  dplyr::collect() |>
  data.table()

sibs_data <- sibs_data[, -c("other", "other_type", "other_granularity", "reg_day_missing", "event")]

sibs_data <- merge(
  sibs_data,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)

# subset to most detailed age groups to prevent overlap
sibs_data <- sibs_data[age_group_id %in% age_map_gbd$age_group_id]

sibs_data[, value := round(value, 8)]
sibs_data[, uncertainty_value := round(uncertainty_value, 8)]
sibs_data <- unique(sibs_data)

sibs_data_mx <- sibs_data[
  dem_measure_id == 19,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "calculation_method_id", "value", "uncertainty_value")
]
sibs_data_qx <- sibs_data[
  dem_measure_id == 20,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "calculation_method_id", "value", "uncertainty_value")
]
sibs_data_pys <- sibs_data[
  dem_measure_id == 13,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "calculation_method_id", "value")
]

sibs_data_mx_wide <- dcast(
  sibs_data_mx,
  age_start + age_end + age_group_id + location + nid + sex_id + date_start + date_end ~ calculation_method_id,
  value.var = c("value", "uncertainty_value")
)
setnames(sibs_data_mx_wide, c("value_8", "value_11"), c("mx", "mx_semi_adj"))
setnames(sibs_data_mx_wide, c("uncertainty_value_8", "uncertainty_value_11"), c("mx_se", "mx_se_semi_adj"))

sibs_data_qx_wide <- dcast(
  sibs_data_qx,
  age_start + age_end + age_group_id + location + nid + sex_id + date_start + date_end ~ calculation_method_id,
  value.var = c("value", "uncertainty_value")
)
setnames(sibs_data_qx_wide, c("value_8", "value_11"), c("qx", "qx_semi_adj"))
setnames(sibs_data_qx_wide, c("uncertainty_value_8", "uncertainty_value_11"), c("qx_se", "qx_se_semi_adj"))

sibs_data_pys_wide <- dcast(
  sibs_data_pys,
  age_start + age_end + age_group_id + location + nid + sex_id + date_start + date_end ~ calculation_method_id,
  value.var = "value"
)
setnames(sibs_data_pys_wide, c("8", "11"), c("sample_size", "sample_size_semi_adj"))

sibs_data_wide <- merge(
  sibs_data_mx_wide,
  sibs_data_qx_wide,
  by = c("age_group_id", "location", "nid", "sex_id", "age_start", "age_end", "date_start", "date_end"),
  all = TRUE
)
sibs_data_wide <- merge(
  sibs_data_wide,
  sibs_data_pys_wide,
  by = c("age_group_id", "location", "nid", "sex_id", "age_start", "age_end", "date_start", "date_end"),
  all = TRUE
)

sibs_data_wide <- unique(
  sibs_data_wide[!(is.na(mx) | is.na(qx) | is.na(sample_size))]
)

sibs_data_wide <- merge(
  sibs_data_wide,
  recall_bias_coefs[, c("sex", "coeffs")],
  by.x = "sex_id",
  by.y = "sex"
)

sibs_data_wide[, period := max(as.numeric(substr(date_start, 1, 4))) - as.numeric(substr(date_start, 1, 4)) + 1, by = c("nid", "location", "sex_id", "age_group_id")]
sibs_data_wide[, coeffs_adj := exp(coeffs * period)]
sibs_data_wide[, qx_adj := qx_semi_adj * coeffs_adj]
sibs_data_wide[qx > 0.99, qx := 0.99]
sibs_data_wide[qx_semi_adj > 0.99, qx_semi_adj := 0.99]
sibs_data_wide[qx_adj > 0.99, qx_adj := 0.99]

sibs_data_wide[, mx_adj := demCore::qx_to_mx(qx_adj, age_length = age_end - age_start)]

sibs_data_wide[, mx_se_adj := mx_se_semi_adj]
sibs_data_wide[, qx_se_adj := qx_se_semi_adj]

sibs_data_wide[, sample_size_adj := sample_size_semi_adj]

# drop 50-54 and 55-59
sibs_data_wide <- sibs_data_wide[!(age_group_id %in% 15:16)]

# add outliers
sibs_data_wide[, outlier := 0]
sibs_data_wide[mx_adj >= 1, outlier := 1]
sibs_data_wide[location == 185 & date_start %in% c("1993-01-01", "1994-01-01"), outlier := 1] # RWA

assertable::assert_values(
  sibs_data_wide,
  colnames = colnames(sibs_data_wide),
  test = "not_na"
)

# check for duplicates
demUtils::assert_is_unique_dt(
  sibs_data_wide,
  c("nid", "location", "sex_id", "age_group_id", "date_start", "date_end")
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
# adjust data types
prev_version[, ':=' (location = as.character(location), date_start = as.character(date_start))]

mx_comparison <- quick_mx_compare(
  test_file = sibs_data_wide,
  compare_file = prev_version,
  id_cols = c(
    "location", "date_start", "nid", "sex_id", "age_group_id"
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
  sibs_data_wide,
  fs::path("FILEPATH")
)
