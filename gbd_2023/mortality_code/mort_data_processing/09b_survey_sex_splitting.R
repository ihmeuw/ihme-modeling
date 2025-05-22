## Meta ------------------------------------------------------------------------

# Description: Sex split nonstandard survey data

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Sex split survey data
#   4. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(dplyr)

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

# msca_mx file for use in 09b and 09c
msca_mx <- fread(fs::path("FILEPATH"))

# Data for sex splitting
survey_sex_3 <- fread(fs::path("FILEPATH"))

survey <- fread(fs::path("FILEPATH"))

# Standard objects
# age maps
age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

# population estimates
pop <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

# add data.table trick for piping
p <- `[`

# Sex split survey data --------------------------------------------------------

index_cols <- c(
  "location_id",
  "nid",
  "year_id",
  "sex_id",
  "age_start",
  "age_end",
  "source_type",
  "source_type_name",
  "source_type_id",
  "underlying_nid",
  "outlier"
)
pattern_cols <- c("location_id", "year_id", "age_start", "age_end")

assert_colnames(survey_sex_3, index_cols, only_colnames = FALSE)
assert_colnames(msca_mx, pattern_cols, only_colnames = FALSE)

# Make a shorter pop object for join
pop_merge_pydisagg <- merge(pop, age_map_extended, by = "age_group_id")

assert_colnames(pop_merge_pydisagg, pattern_cols, only_colnames = FALSE)

data_config <- splitter$SexDataConfig(
  index = index_cols,
  val = "mx",
  val_sd = "mx_se"
)
pattern_config <- splitter$SexPatternConfig(
  by = list("location_id", "year_id", "age_start", "age_end"),
  val = "pred_rate",
  val_sd = "pred_rate_sd"
)
pop_config <- splitter$SexPopulationConfig(
  index = c("location_id", "year_id", "age_start", "age_end"),
  sex = "sex_id",
  sex_m = 1,
  sex_f = 2,
  val = "population"
)
sex_splitter <- splitter$SexSplitter(
  data = data_config,
  pattern = pattern_config,
  population = pop_config,
)

setDT(survey_sex_3)
setDT(msca_mx)

msca_match <- function(sex_3_dt, msca_dt){
  # Step 1: merge msca_mx and survey to get age group upper+lower bounds
  msca_mx_matched <- msca_dt |> merge(unique(sex_3_dt[, .(location_id, year_id, age_start, age_end)]), by = c("location_id", "year_id"), all.x = T, allow.cartesian = T)
  # filter so that msca age groups are within bounds
  msca_mx_matched <- msca_mx_matched[age_start.x >= age_start.y & age_end.x <= age_end.y]

  validate_msca_match <- msca_mx_matched[, .(age_start.x, age_end.x, age_start.y, age_end.y)] |> unique() |>
    p(, .(msca_range = sum((age_end.x - age_start.x)), survey_range = age_end.y - age_start.y), by = c("age_start.y", "age_end.y"))
  validate_msca_match[, abs_diff := round(abs(survey_range - msca_range), 4)]
  assertable::assert_values(validate_msca_match, "abs_diff", test = "equal", test_val = 0)

  # save unique survey age groups
  names(msca_mx_matched) <- gsub("\\.y$", "", names(msca_mx_matched))

  # Step 2: aggregate population and deaths within wider age groups to get large msca_mx for larger age groups
  msca_mx_matched <- msca_mx_matched[, lapply(.SD, sum), .SDcols = c("population", "deaths"), by = c("location_id", "year_id", "sex_id", "age_start", "age_end")]
  msca_mx_matched[, pred_rate := deaths / population]

  # Step 3: set msca_mx to cumulative sex for pydisagg by finding female:male ratio
  msca_mx_matched[, pred_rate := pred_rate[sex_id == 2] / pred_rate[sex_id == 1], by = c("location_id", "year_id", "age_start", "age_end")]
  msca_mx_matched[, `:=` (sex_id =3, pred_rate_sd = 0)]

  # Step 4: remove population and deaths values and save unique values
  sex_split_msca <- msca_mx_matched[, c(setdiff(names(msca_mx_matched), c("population", "deaths"))), with = F] |> unique()
  return(sex_split_msca)
}
msca_mx_sex_split <- msca_match(survey_sex_3, msca_mx)

# reassign NA values for splititng
survey_sex_3 <- survey_sex_3 |>
  mutate(across(c(source_type, extraction_source), ~if_else(is.na(.), "TEMP_NA", .)))

# Now do sex-splitting
survey_sex_3_matched <- sex_splitter$split(
  data = survey_sex_3,
  pattern = msca_mx_sex_split,
  population = pop_merge_pydisagg,
  model = "rate",
  output_type = "rate"
)

# go back to na values
survey_sex_3_matched <- survey_sex_3_matched |>
  mutate(across(c(source_type, extraction_source), ~if_else(. == "TEMP_NA", NA, .)))

ratio <- nrow(survey_sex_3_matched) / nrow(survey_sex_3)
# Check if the ratio is within the tolerance range of 1.9 to 2.1
stopifnot(ratio >= 1.9 && ratio <= 2.1)

# Check that we have mx and mx_se values for all rows
stopifnot(sum(is.na(survey_sex_3_matched$mx)) == 0)
stopifnot(sum(is.na(survey_sex_3_matched$mx_se)) == 0)

# set up sex-split data to have the same columns as survey
survey_sex_3_merger <- as.data.table(survey_sex_3)
survey_sex_3_matched <- merge(
  survey_sex_3_matched,
  age_map_extended,
  by = c("age_start", "age_end")
)

survey_sex_3_matched <- survey_sex_3_matched |>
  rename(age_group_id = age_group_id.x) |>
  select(-age_group_id.y)

survey_sex_3_matched <- merge(
  survey_sex_3_matched,
  survey_sex_3_merger[, .(age_group_id, year_id, nid, mx, location_id, source_type_name, underlying_nid, outlier)],
  by = c("outlier", "age_group_id", "mx","underlying_nid", "nid", "year_id", "location_id", "source_type_name"),
  all.x = TRUE
)

setDT(survey_sex_3_matched)
survey_sex_3_matched[, c("mx", "mx_se", "pred_rate", "pred_rate_sd", "m_pop", "f_pop") := NULL]
names(survey_sex_3_matched) <- gsub("sex_split_result", "mx", names(survey_sex_3_matched))

# Bring all sex-split data together again
survey <- survey |>
  filter(sex_id %in% c(1, 2)) |>
  bind_rows(survey_sex_3_matched)

stopifnot(all(survey$sex_id) %in% c(1, 2))

# Save -------------------------------------------------------------------------

readr::write_csv(
  survey,
  fs::path("FILEPATH")
)
