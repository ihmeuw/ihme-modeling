## Meta ------------------------------------------------------------------------

# Description: Sex split nonstandard VR data

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Sex split VR data
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

source(fs::path("FILEPATH"))

# Read in internal inputs ------------------------------------------------------

# Bring in pyDisagg
splitter <- reticulate::import("FILEPATH")

# Data for sex splitting
needs_sex_split <- fread(fs::path("FILEPATH"))

# Already sex-specific data
df_1_2 <- fread(fs::path("FILEPATH"))

# Adapted pattern
msca_mx <- fread(fs::path("FILEPATH"))

# Standard objects
# age maps
age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

# population estimates
pop <- fread(fs::path("FILEPATH"))

pop_short_msca_mx <- fread(fs::path("FILEPATH"))

# locations
locs <- fread(fs::path("FILEPATH"))

# empirical pop
emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# add data.table trick for piping
p <- `[`

# Sex split VR data ------------------------------------------------------------

# For 999 age groups, assign them to the all-ages group (22)
needs_sex_split <- needs_sex_split |>
  mutate(age_group_id = ifelse(age_start == 999, 22, age_group_id)) |>
  mutate(age_start = ifelse(age_start == 999, 0, age_start),
         age_end = ifelse(age_end == 999, 125, age_end))

# Coordinate age groups between msca and vr datasets
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

  # Step 2: aggregate population and deaths within wider age groups
  msca_mx_matched <- msca_mx_matched[, lapply(.SD, sum), .SDcols = c("population", "deaths"), by = c("location_id", "year_id", "sex_id", "age_start", "age_end")]
  msca_mx_matched[, pred_rate := deaths / population]

  # Step 3: set msca_mx to cumulative sex for pydisagg by finding female:male ratio
  msca_mx_matched[, pred_rate := pred_rate[sex_id == 2] / pred_rate[sex_id == 1], by = c("location_id", "year_id", "age_start", "age_end")]
  assertable::assert_values(msca_mx_matched, c("population", "deaths", "pred_rate"))
  msca_mx_matched[, `:=` (sex_id = 3, pred_rate_sd = 0)]

  # Step 4: remove population and deaths values and save unique values
  sex_split_msca <- msca_mx_matched[, c(setdiff(names(msca_mx_matched), c("population", "deaths"))), with = F] |> unique()
  return(sex_split_msca)
}

msca_mx_sex_split <- msca_match(needs_sex_split, msca_mx)

# first identify gbd starting/ending ages
pop_short_msca_mx <- pop_short_msca_mx |> merge(age_map_extended, by = "age_group_id")
uniq_starts <- pop_short_msca_mx[, age_start] |> unique()
uniq_ends <- pop_short_msca_mx[, age_end] |> unique()

# combine datasets to identify invalid starting/ending age combos
revise_pop_ages <- pop_short_msca_mx |>
  merge(unique(msca_mx_sex_split[, .(location_id, year_id, age_start, age_end)]),
        by = c("location_id", "year_id", "age_start", "age_end"), all.y = T)
revise_pop_ages <- revise_pop_ages[is.na(population), .(location_id, year_id, age_start, age_end)]

# check if age start and age end are among gbd ages
revise_pop_ages[, gbd_ages := (age_start %in% uniq_starts & age_end %in% uniq_ends)]

assertable::assert_nrows(revise_pop_ages[!(gbd_ages)], 0)

revise_pop_ages <- pop_short_msca_mx |>
  merge(revise_pop_ages, by = c("location_id", "year_id"), all.x = T, allow.cartesian = T)
revise_pop_ages <- revise_pop_ages[age_start.x >= age_start.y & age_end.x <= age_end.y]

# skipping the overshoot validation for now
revise_pop_ages[, c("age_start.x", "age_end.x") := NULL]
names(revise_pop_ages) <- gsub("\\.y$", "", names(revise_pop_ages))
revise_pop_ages[, population := sum(population), by = c("location_id", "year_id", "sex_id", "age_start", "age_end")]

revise_pop_ages[, "gbd_ages" := NULL]
pop_short_msca_mx <- rbind(pop_short_msca_mx, revise_pop_ages)

# correct age group id
pop_short_msca_mx[, age_group_id := NULL]
pop_short_msca_mx <- merge(
  pop_short_msca_mx,
  age_map_extended,
  by = c("age_start", "age_end")
)
copy_pop <- copy(pop_short_msca_mx)
pop_short_msca_mx <- unique(pop_short_msca_mx)

#' Pydisagg

index_cols <- c(
  "location_id",
  "nid",
  "year_id",
  "sex_id",
  "age_start",
  "age_end",
  "source_type",
  "source_type_id",
  "underlying_nid",
  "outlier",
  "extraction_source",
  "sex_id_4",
  "has_999",
  "has_0_125",
  "subgroup"
)
pattern_cols <- c("location_id", "year_id", "age_start", "age_end")

assert_colnames(needs_sex_split, index_cols, only_colnames = FALSE)
assert_colnames(msca_mx_sex_split, pattern_cols, only_colnames = FALSE)
assert_colnames(pop_short_msca_mx, pattern_cols, only_colnames = FALSE)

mortcore::check_multiple_data_points(needs_sex_split, index_cols)
mortcore::check_multiple_data_points(msca_mx_sex_split, pattern_cols)
mortcore::check_multiple_data_points(pop_short_msca_mx, c(pattern_cols, "sex_id"))

data_config <- splitter$SexDataConfig(
  index = index_cols,
  val = "mx",
  val_sd = "mx_se"
)
pattern_config <- splitter$SexPatternConfig(
  by = as.list(pattern_cols),
  val = "pred_rate",
  val_sd = "pred_rate_sd"
)
pop_config <- splitter$SexPopulationConfig(
  index = pattern_cols,
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

# reassign NA values for splititng
needs_sex_split <- needs_sex_split |>
  mutate(across(c(outlier_note, underlying_title), ~if_else(is.na(.), "TEMP_NA", .))) |>
  mutate(across(c(underlying_nid, age_group_id), ~if_else(is.na(.), 9999999999, .)))

needs_sex_split_matched <- sex_splitter$split(
  data = needs_sex_split,
  pattern = msca_mx_sex_split,
  population = pop_short_msca_mx,
  model = "rate",
  output_type = "rate"
)

# reassign to NA as appropriate
needs_sex_split_matched <- needs_sex_split_matched |>
  mutate(across(c(outlier_note, underlying_title), ~if_else(. == "TEMP_NA", NA, .))) |>
  mutate(across(c(underlying_nid, age_group_id), ~if_else(. == 9999999999, NA, .)))

# remove unnecessary columns and specify `sex_split_result` as `mx` column
setDT(needs_sex_split_matched)
needs_sex_split_matched <- needs_sex_split_matched[, c(index_cols, "sex_split_result", "sex_split_result_se"), with = F]
names(needs_sex_split_matched) <- gsub("sex_split_result", "mx", names(needs_sex_split_matched))

# Re-add age_group_id
readd_age_group_id <- needs_sex_split[
  , -c("date_start", "date_end", "sex_id", "mx", "mx_se")
]

needs_sex_split_matched <- merge(
  needs_sex_split_matched,
  readd_age_group_id,
  by = setdiff(index_cols, "sex_id"),
  all.x = TRUE
)

nrow_before <- nrow(needs_sex_split_matched)

# Recalculate mx_se for empirical population rows
emp_pop_split_no_vr_check <- emp_pop_split_no_vr |>
  transmute(location_id, year_id, sex_id, source_type, match = TRUE) |>
  distinct()

needs_sex_split_matched <- needs_sex_split_matched |>
  left_join(emp_pop_split_no_vr_check, by = c("location_id", "year_id", "sex_id", "source_type"))

emp_pop_match <- needs_sex_split_matched |>
  filter(match == TRUE)

emp_pop_split_no_vr_gbd <- emp_pop_split_no_vr |>
  filter(age_group_id %in% age_map_gbd$age_group_id)

# Set mx_se based on emp pop
count_for_progress <- nrow(emp_pop_match)
for (i in 1:nrow(emp_pop_match)) {
  cat(paste0(count_for_progress-i, ' of ', count_for_progress, ': ', i/count_for_progress*100, '% \n'));flush.console()
  row <- emp_pop_match[i, ]

  emp_pop_rows <- emp_pop_split_no_vr_gbd |>
    filter(location_id == row$location_id) |>
    filter(year_id == row$year_id) |>
    filter(sex_id == row$sex_id)

  stopifnot(nrow(emp_pop_rows) == 25)

  emp_pop_rows |>
    filter(age_start >= row$age_start & age_end <= row$age_end)

  if (nrow(emp_pop_rows) == 0) {stop()}

  pop_sum <- sum(emp_pop_rows$census_pop)

  row$mx_se <- sqrt(ifelse(row$mx >= 0.99, 0.99, row$mx) * (1 - ifelse(row$mx >= 0.99, 0.99, row$mx)) / pop_sum)

  emp_pop_match[i,] <- row
}

# Add emp_pop mx_se rows back into needs_sex_split_matched
needs_sex_split_matched <- needs_sex_split_matched |>
  filter(is.na(match)) |>
  bind_rows(emp_pop_match) |>
  select(-match)

stopifnot(nrow(needs_sex_split_matched) == nrow_before)

assertable::assert_values(df_1_2, "sex_id", test = "in", test_val = c(1, 2))
assertable::assert_values(needs_sex_split_matched, "sex_id", test = "in", test_val = c(1, 2))

# Need to make this 999 change to df_1_2
df_1_2 <- df_1_2 |>
  mutate(age_start = ifelse(age_start == 999, 0, age_start),
         age_end = ifelse(age_end == 999, 125, age_end))

# Prefer df_1_2 data over sex split data
needs_sex_split_matched <- needs_sex_split_matched |>
  anti_join(
    df_1_2,
    by = c(
      "location_id", "year_id", "sex_id", "age_start", "age_end", "nid",
      "underlying_nid", "source_type", "extraction_source"
    )
  )

needs_sex_split_matched <- needs_sex_split_matched |>
  select(-age_group_id) |>
  left_join(age_map_gbd, by = c("age_start", "age_end"))

pop_short <- pop |>
  select(location_id, sex_id, year_id, age_group_id, population)

# Add in new population for sex-specific data
needs_sex_split_matched <- needs_sex_split_matched |>
  left_join(pop_short, by = c("location_id", "sex_id", "year_id", "age_group_id"))

needs_sex_split_matched <- needs_sex_split_matched |>
  mutate(population = ifelse(is.na(population.y), population.x, population.y)) |>
  mutate(sample_size = ifelse(is.na(population.y), sample_size, population.y)) |>
  select(-population.x, -population.y)

sex_split_VR <- bind_rows(df_1_2, needs_sex_split_matched)

# Check for NAs
stopifnot(sum(is.na(sex_split_VR$mx)) == 0)
stopifnot(sum(is.na(sex_split_VR$mx_se)) == 0)

# Save -------------------------------------------------------------------------

readr::write_csv(
  sex_split_VR,
  fs::path("FILEPATH")
)
