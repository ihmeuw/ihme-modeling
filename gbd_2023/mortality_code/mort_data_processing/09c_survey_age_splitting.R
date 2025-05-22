## Meta ------------------------------------------------------------------------

# Description: Age split nonstandard survey data
# Output from this script is final dataset of survey splitting

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Age split survey data
#   4. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(dplyr)
library(reticulate)

PATH <- "FILEPATH"
Sys.setenv(RETICULATE_PYTHON = PATH)
Sys.setenv(PYTHONPATH = PATH)

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

# Read in internal inputs ------------------------------------------------------

# Bring in pyDisagg
splitter <- reticulate::import("FILEPATH")

# Adapted pattern
msca_mx <- fread(fs::path("FILEPATH"))

msca_mx <- msca_mx |> select(location_id, age_group_id, sex_id, year_id, pred_rate, pred_rate_sd, age_start, age_end)

# Survey data
survey <- fread(fs::path("FILEPATH"))

# Standard objects
# age maps
age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

# population estimates
pop <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# Age split survey data --------------------------------------------------------

# build a pop data object
pop_data <- msca_mx |>
  select(age_group_id, age_start, age_end, sex_id, location_id, year_id) |>
  distinct()

pop_short <- pop |>
  select(location_id, sex_id, year_id, age_group_id, population)

pop_data <- pop_data |>
  left_join(pop_short, by = c("age_group_id", "sex_id", "location_id", "year_id"))

stopifnot(sum(is.na(pop_data$population)) == 0)

# Add in id for pyDisagg
survey <- survey |>
  mutate(id = row_number())

# Remove 999 data from survey
survey <- survey |>
  filter(age_start != 999)

# Check that all age_start are less than age_end
stopifnot(all(survey$age_start < survey$age_end))

# Set up the pyDisagg configs:

# Data config is survey data to be split
data_config <- splitter$AgeDataConfig(
  index = c("sex_id", "location_id", "year_id", "id"),
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "mx",
  val_sd = "mx_se"
)

# MSCA data is the pattern config
pattern_config <- splitter$AgePatternConfig(
  by = list("sex_id", "location_id", "year_id"),
  age_key = "age_group_id",
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "pred_rate",
  val_sd = "pred_rate_sd"
)

# From pop object
pop_config <- splitter$AgePopulationConfig(
  index = c("age_group_id", "location_id", "year_id", "sex_id"),
  val = "population"
)

age_splitter <- splitter$AgeSplitter(
  data = data_config,
  pattern = pattern_config,
  population = pop_config
)

# Final command of Pydisagg
result <- age_splitter$split(
  data = survey,
  pattern = msca_mx,
  population = pop_data,
  model = "rate",
  output_type = "rate",
  propagate_zeros = TRUE
)

# Save result object without changes
readr::write_csv(
  result,
  fs::path("FILEPATH")
)

# Simplifying the result
result_simplified <- result |>
  select(
    id, sex_id, location_id, year_id, age_start = pat_age_start, age_end = pat_age_end,
    mx = age_split_result, mx_se = age_split_result_se, population = pop_population
  )

# Check for NAs
stopifnot(sum(is.na(result_simplified$mx)) == 0)
stopifnot(sum(is.na(result_simplified$mx_se)) == 0)

# Add in desired information columns
info_cols <- survey |>
  select(nid, underlying_nid, source_type_name, source_type, source_type_id, id, age_start_orig = age_start, age_end_orig = age_end)

result_simplified <- result_simplified |>
  left_join(info_cols, by = "id")

msca_age_groups <- msca_mx |>
  select(age_group_id, age_start, age_end) |>
  distinct()

# Spot check mx values before and after split
random_rows <- sample(1:nrow(survey), size = 250, replace = FALSE)
for (i in 1:length(random_rows)) {

  print(i/length(random_rows) * 100)

  spot_check_survey <- survey[i,]

  pop_check <- pop_short |>
    filter(sex_id == spot_check_survey$sex_id) |>
    filter(year_id == spot_check_survey$year_id) |>
    filter(location_id == spot_check_survey$location_id) |>
    left_join(age_map_extended, by = "age_group_id") |>
    filter(age_start >= spot_check_survey$age_start & age_end <= spot_check_survey$age_end) |>
    filter(age_group_id %in% msca_age_groups$age_group_id)

  surv <- spot_check_survey$mx * sum(pop_check$population)

  spot_check_split <- result_simplified |>
    filter(id == spot_check_survey$id)

  split <- sum(spot_check_split$mx * spot_check_split$population)

  stopifnot(abs(surv - split) / ((surv + split) / 2) * 100 <= 1)

}

# Replace pop with emp_pop_split where applicable

# Prep emp_pop_split_short
emp_pop_split_short <- emp_pop_split_no_vr |>
  select(age_group_id, location_id, year_id, sex_id, census_pop, source_type_name = source_type)

# Add in age_group_id
result_simplified <- result_simplified |>
  left_join(age_map_extended, by = c("age_start", "age_end"))

result_census_survey <- result_simplified |>
  filter(source_type_name %in% c("Census", "Survey"))

result_cbh_sbh_sibs <- result_simplified |>
  filter(source_type_name %in% c("CBH", "SBH", "SIBS"))

# Apply the operations only to the Census/Survey data
result_census_survey <- result_census_survey |>
  left_join(emp_pop_split_short, by = c("location_id", "year_id", "sex_id", "age_group_id", "source_type_name"))

sum(!is.na(result_census_survey$census_pop))

result_census_survey <- result_census_survey |>
  mutate(population = ifelse(is.na(census_pop), population, census_pop)) |>
  select(-census_pop)

sum(!is.na(result_census_survey$census_pop))

# Combine the modified and unmodified parts back together
result_simplified <- rbind(result_census_survey, result_cbh_sbh_sibs)

stopifnot(sum(is.na(result_simplified$population)) == 0)

# Finished with data processing

# Check for duplicate rows
demUtils::assert_is_unique_dt(
  setDT(result_simplified),
  c("location_id", "year_id", "sex_id", "age_group_id", "source_type_name", "nid", "underlying_nid", "age_start_orig", "age_end_orig")
)

# Assert no NAs in multiple columns
assertable::assert_values(
  result_simplified,
  colnames = c("mx", "population", "sex_id", "location_id", "year_id", "nid"),
  test = "not_na"
)

result_simplified <- result_simplified |>
  mutate(original_age_length = age_start_orig - age_end_orig) |>
  group_by(nid, age_group_id, location_id, sex_id, year_id, source_type, source_type_name) |>
  slice_min(order_by = original_age_length, with_ties = TRUE) |>
  select(-original_age_length)

# Save -------------------------------------------------------------------------

readr::write_csv(
  result_simplified,
  fs::path("FILEPATH")
)
