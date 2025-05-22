## Meta ------------------------------------------------------------------------

# Description: Survey data preparation and creation of the 'pattern' data
# to be used in survey sex and age splitting. The pattern is essentially age-sex
# specific mx values that are serve as the reference values for sex and age
# splitting with pyDisagg.

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Prep survey data to be split
#   4. Pattern selection
#   5. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(dplyr)
library(tidyr)

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

# Read in internal inputs ------------------------------------------------------

# GBD mx values used in pattern selection
gbd_mx <- demInternal::get_dem_outputs(
  "with shock life table estimate",
  wslt_version,
  gbd_year = 2023,
  life_table_parameter_ids = 1
)

# Age pattern handoff file
handoff_1 <- arrow::read_parquet(fs::path("FILEPATH"))

# Raw pattern from MSCA
msca_mx <- arrow::read_parquet(mx_predictions_path)

# Read in non-standard survey data
survey <- fread(fs::path("FILEPATH"))

# Standard objects
# age maps
age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

# population estimates
pop <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

# Prep survey data for splitting -----------------------------------------------

# Filter to only non-outliered data
assertable::assert_values(survey, "outlier", test = "not_na")
survey <- survey[outlier == 0]

# Drop rows where mx = 0
survey <- survey[mx != 0]

# Initial checks
stopifnot(sum(is.na(survey$age_group_id)) == 0)
stopifnot(sum(is.na(survey$mx)) == 0)
stopifnot(sum(is.na(survey$mx_se)) == 0)

# Check for any duplicates
demUtils::assert_is_unique_dt(
  survey,
  c("location_id", "year_id", "sex_id", "age_group_id", "source_type_name", "nid", "underlying_nid")
)

# Pattern selection ------------------------------------------------------------

# Add in age map
msca_mx <- msca_mx |>
  left_join(age_map_gbd, by = "age_group_id")

# Get loc-year-sex that have at least 10 years of unoutliered vr data
vr_unoutliered <- handoff_1 |>
  filter(source_type_name %in% c("VR", "SRS", "DSP")) |>
  filter(outlier == 0) |>
  group_by(location_id, sex_id, age_group_id) |>
  mutate(count = n()) |>
  filter(count >= 10) |> # At least 10 years of data
  ungroup() |>
  select(location_id, sex_id, age_start, age_end) |>
  distinct()

# If we have a match (or more than one), we want to use kreg for the mx column
kreg_df <- survey |>
  filter(outlier == 0) |>
  left_join(vr_unoutliered, by = c("location_id", "sex_id")) |>
  filter(age_start.x <= age_end.y & age_end.x >= age_start.y) |>
  pull(location_id)

msca_mx <- msca_mx |>
  mutate(mx_column = ifelse(location_id %in% kreg_df, "kreg", "spxmod"))

# Manual overrides where we specifically want to use spxmod
use_spxmod <- c("BGD", "KGZ", "MNG")
use_spxmod_loc_id <- locs |> filter(ihme_loc_id %in% use_spxmod) |> pull(location_id)

msca_mx <- msca_mx |>
  mutate(mx_column = ifelse(location_id %in% use_spxmod_loc_id, "spxmod", mx_column))

# Manual overrides where we specifically want to use kreg
use_kreg <- c("CHN_354", "VNM")
use_kreg_loc_id <- locs |> filter(ihme_loc_id %in% use_spxmod) |> pull(location_id)

msca_mx <- msca_mx |>
  mutate(mx_column = ifelse(location_id %in% use_kreg_loc_id, "kreg", mx_column))

# Bring in gbd mx for replacing into msca_mx
gbd_mx <- gbd_mx |>
  rename(gbd_mx = mean) |>
  mutate(gbd_mx_se = upper - gbd_mx) |>
  select(location_id, sex_id, year_id, age_group_id, gbd_mx, gbd_mx_se)

# Filter down to locations and age_group_ids that are desired
gbd_mx_location_ids <- locs |>
  filter(ihme_loc_id == "AZE") |>
  pull(location_id)

gbd_mx_ages <- msca_mx |>
  filter(age_end <= 5) |>
  pull(age_group_id)

gbd_mx <- gbd_mx |>
  filter(location_id %in% gbd_mx_location_ids) |>
  filter(age_group_id %in% gbd_mx_ages)

msca_mx <- msca_mx |>
  left_join(gbd_mx, by = c("location_id", "sex_id", "year_id", "age_group_id"))

# Create the pred_rate and pred_rate_sd columns
msca_mx <- msca_mx |>
  mutate(pred_rate = case_when(
    location_id == 34 ~ spxmod,
    location_id == 161 ~ spxmod,
    mx_column == "kreg" ~ kreg,
    mx_column == "spxmod" ~ kreg,
    mx_column == "gbd_mx" ~ kreg
  )) |>
  mutate(pred_rate_sd = 0)

# Save the mx_column for plotting
save_msca_mx <- msca_mx |>
  select(kreg, spxmod, location_id, year_id, age_group_id, pred_rate, sex_id) |>
  pivot_longer(cols = -c(pred_rate, location_id, year_id, age_group_id, sex_id), names_to = "column", values_to = "value") |>
  filter(pred_rate == value) |>
  distinct(pred_rate, .keep_all = TRUE) |>
  select(pred_rate, mx_column = column, location_id, year_id, age_group_id, sex_id)

stopifnot(sum(is.na(msca_mx$age_start)) == 0)
stopifnot(sum(is.na(msca_mx$age_end)) == 0)

msca_age_groups <- msca_mx |> select(age_group_id, age_start, age_end) |> distinct()

pop_short <- pop |>
  select(location_id, sex_id, year_id, age_group_id, population)

# Remove any rows from survey that don't have a location-year match from msca_mx
survey <- survey |>
  semi_join(msca_mx, by = c("location_id", "year_id"))

# sex split sex_id 3 data
survey <- survey |>
  group_by(location_id, year_id, age_group_id, source_type_name, nid, underlying_nid) |>
  mutate(all_sex_ids_present = ifelse(3 %in% sex_id && all(c(1,2) %in% sex_id), TRUE, FALSE)) |>
  filter(!(sex_id == 3 & all_sex_ids_present)) |>
  select(-all_sex_ids_present) |>
  ungroup()

survey <- survey |>
  group_by(location_id, year_id, age_group_id, source_type_name, nid, underlying_nid) |>
  mutate(sex_id_3_present = any(sex_id == 3)) |>
  filter(!(sex_id %in% c(1, 2) & sex_id_3_present)) |>
  select(-sex_id_3_present) |>
  ungroup()

survey_sex_3 <- survey |>
  filter(sex_id == 3)

# Quick check to make sure the age_end is more than age_start
stopifnot(all(survey_sex_3$age_start < survey_sex_3$age_end))

# Before sex splitting we need deaths and population columns in the msca_mx data
msca_mx <- msca_mx |>
  left_join(pop_short, by = c("location_id", "sex_id", "year_id", "age_group_id"))

stopifnot(sum(is.na(msca_mx$population)) == 0)

msca_mx <- msca_mx |>
  mutate(deaths = pred_rate * population)

# Save -------------------------------------------------------------------------

# Investigate file of msca_mx (with pattern selection column)
readr::write_csv(
  save_msca_mx,
  fs::path("FILEPATH")
)

# msca_mx file for use in 09b and 09c
readr::write_csv(
  msca_mx,
  fs::path("FILEPATH")
)

# 9b uses both the survey_sex_3 and survey objects
# Data for sex splitting
readr::write_csv(
  survey_sex_3,
  fs::path("FILEPATH")
)

# All survey data
readr::write_csv(
  survey,
  fs::path("FILEPATH")
)
