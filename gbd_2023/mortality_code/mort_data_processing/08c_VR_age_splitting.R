## Meta ------------------------------------------------------------------------

# Description: Age split nonstandard VR data

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Age split VR data
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

# Data for age-splitting
df <- fread(fs::path("FILEPATH"))

# Adapted pattern
msca_mx <- fread(fs::path("FILEPATH"))

msca_mx <- msca_mx |> select(location_id, age_group_id, sex_id, year_id, pred_rate, pred_rate_sd, age_start, age_end)

# SRS pop scalars
srs_pop_scalars <- fread(fs::path("FILEPATH"))

# Standard objects
# age maps
age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

# population estimates
pop <- fread(fs::path("FILEPATH"))

pop_short_msca_mx <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# add data.table trick for piping
p <- `[`

# Age split VR data ------------------------------------------------------------

# Add on ihme_loc_id
ihme_loc_ids <- locs |>
  select(location_id, ihme_loc_id)

df <- df |>
  left_join(ihme_loc_ids, by = "location_id")
stopifnot(sum(is.na(df$ihme_loc_id)) == 0)

# Build a pop data object
pop_data <- msca_mx |>
  select(age_start, age_end, sex_id, location_id, year_id) |>
  distinct()

# Merge population onto msca_death groups
pop_short_msca_mx <- pop_short_msca_mx |>
  left_join(age_map_extended, by = "age_group_id")

pop_data <- pop_data |>
  left_join(pop_short_msca_mx, by = c("age_start", "age_end", "sex_id", "location_id", "year_id"))

stopifnot(sum(is.na(pop_data$population)) == 0)

df_before <- nrow(df)
df <- semi_join(df, msca_mx, by = c("sex_id", "location_id", "year_id"))
df <- semi_join(df, pop_data, by = c("sex_id", "location_id", "year_id"))
df_after <- nrow(df)
stopifnot(df_before - df_after == 0)

# Adding in row number as id
df <- df |>
  mutate(id = row_number())

df <- df |>
  mutate(age_group_id = ifelse(has_999, 22, age_group_id))

# Check data, pattern, and pop for any duplicates
dup_vars <- c("location_id", "sex_id", "year_id", "age_start", "age_end", "nid", "source_type", "extraction_source")

pattern_cols <- c("location_id", "year_id", "age_start", "age_end")

pattern_cols_age_split <- c(pattern_cols, "sex_id")

# Validations: make sure all index/pattern columns are in dataset,
# and there are no duplicates
check_df <- df |>
  group_by(location_id, sex_id, year_id, age_start, age_end, nid, source_type, underlying_nid, extraction_source) |>
  filter(n() > 1) |>
  ungroup()

check_pattern <- msca_mx |>
  group_by(location_id, sex_id, year_id, age_start, age_end) |>
  filter(n() > 1) |>
  ungroup()

check_pop <- pop_data |>
  group_by(location_id, sex_id, year_id, age_start, age_end) |>
  filter(n() > 1) |>
  ungroup()

stopifnot(nrow(check_df) == 0)
stopifnot(nrow(check_pattern) == 0)
stopifnot(nrow(check_pop) == 0)

# Review of data to be split
stopifnot(all(df$sex_id %in% c(1, 2)))

# Split data

# 1. groupings that only have a 0-125 row ()
df_0_125 <- df |>
  group_by(location_id, year_id, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  filter(all(has_0_125)) |>
  mutate(count_has_0_125_true = sum(has_0_125 == TRUE, na.rm = TRUE)) |>
  filter(count_has_0_125_true == 1) |>
  select(-count_has_0_125_true) |>
  ungroup()

# 2. groupings that only have subgroups
df_subgroups <- df |>
  group_by(location_id, year_id, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  filter(all(subgroup))

# 3. groupings that have subgroups AND a 999 row (but no 0-125 row)
df_subgroups_999 <- df |>
  group_by(location_id, year_id, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  filter(any(subgroup)) |>
  filter(!any(has_0_125)) |>
  mutate(count_has_999_true = sum(has_999 == TRUE, na.rm = TRUE)) |>
  filter(count_has_999_true == 1) |>
  select(-count_has_999_true) |>
  ungroup()

check <- df |>
  group_by(location_id, year_id, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  filter(any(subgroup)) |>
  filter(!any(has_0_125)) |>
  mutate(count_has_999_true = sum(has_999 == TRUE, na.rm = TRUE)) |>
  ungroup()

table(check$count_has_999_true)

# 4. groupings that ONLY have a 999 row
df_999_only <- df |>
  group_by(location_id, year_id, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  filter(!any(subgroup)) |>
  filter(!any(has_0_125)) |>
  mutate(count_has_999_true = sum(has_999 == TRUE, na.rm = TRUE)) |>
  filter(count_has_999_true == 1) |>
  select(-count_has_999_true) |>
  ungroup()

readr::write_csv(
  df_999_only,
  fs::path("FILEPATH")
)

# Combine the three groupings, but keep the 999 rows in their own df
df_main <- rbind(df_0_125, df_subgroups, df_subgroups_999) |>
  filter(!has_999)

df_999 <- df_subgroups_999 |>
  filter(has_999) |>
  bind_rows(df_999_only)

stopifnot(nrow(df_main) + nrow(df_999) == nrow(df))

# In df_main only, remove any instances where the study age-range is smaller than any single pattern range
does_encompass <- function(age_start, age_end) {
  any(msca_age_groups$age_start >= age_start & msca_age_groups$age_end <= age_end)
}

msca_age_groups <- msca_mx |>
  select(age_group_id, age_start, age_end) |>
  distinct()

# Apply the function to each row in df and filter based on the result
df_main <- df_main |>
  rowwise() |>
  filter(does_encompass(age_start, age_end)) |>
  ungroup()

# Remove overlapping age groups in the df_main only
data_overlap <- df_main |>
  group_by(location_id, year_id, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  mutate(overlap_detected = if(n() > 1) {
    any({
      combn(seq_len(n()), 2, function(idx) {
        i = idx[1]
        j = idx[2]
        (age_start[i] < age_end[j] && age_end[i] > age_start[j]) ||
          (age_start[j] < age_end[i] && age_end[j] > age_start[i])
      }, simplify = TRUE)
    })
  } else {
    FALSE
  }) |>
  ungroup() |>
  filter(overlap_detected) |>
  select(id, location_id, sex_id, year_id, nid, age_start, age_end, underlying_nid, source_type, outlier, rank, extraction_source)

# Preserve the largest age group so we preserve the most data
data_overlap_keep <- data_overlap |>
  group_by(location_id, sex_id, year_id, nid, underlying_nid, source_type, extraction_source) |>
  mutate(age_range = age_end - age_start) |>
  slice(which.max(age_range)) |>
  ungroup()

remove <- setdiff(data_overlap$id, data_overlap_keep$id)

# Remove rows
dropped_overlap <- df_main |>
  filter(id %in% remove)

# Save for investigations as needed
readr::write_csv(
  dropped_overlap,
  fs::path("FILEPATH")
)

df_main <- df_main |>
  filter(!id %in% remove)

# Prep 999 data for its own splitting ------------------------------------------

# Save df_999 data before age-splitting
readr::write_csv(
  df_999,
  fs::path("FILEPATH")
)

df_999_pre_split_deaths <- sum(df_999$deaths)

# Set up the pyDisagg configs: -------------------------------------------------

data_config <- splitter$AgeDataConfig(
  index = c("sex_id", "location_id", "ihme_loc_id", "year_id", "nid", "underlying_nid", "source_type_id", "source_type", "id", "outlier", "rank", "extraction_source", "sex_id_4"),
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "mx",
  val_sd = "mx_se"
)

pattern_config <- splitter$AgePatternConfig(
  by = list("sex_id", "location_id", "year_id"),
  age_key = "age_group_id",
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "pred_rate",
  val_sd = "pred_rate_sd"
)

pop_config <- splitter$AgePopulationConfig(
  index = c("age_group_id", "location_id", "year_id", "sex_id"),
  val = "population"
)

age_splitter <- splitter$AgeSplitter(
  data = data_config,
  pattern = pattern_config,
  population = pop_config
)

result_999 <- age_splitter$split(
  data = df_999,
  pattern = msca_mx,
  population = pop_data,
  model = "rate",
  output_type = "rate",
  propagate_zeros = TRUE
)

# Check for duplicates in result_999
check_result_999 <- result_999 |>
  group_by(
    nid, year_id, sex_id, location_id, pat_age_start, pat_age_end, underlying_nid,
    source_type_id, source_type, extraction_source
  ) |>
  mutate(n = n()) |>
  filter(n > 1) |>
  ungroup()

stopifnot(nrow(check_result_999) == 0)

unique_unsplit_999 <- df_999 |>
  select(id, mx)

result_split_deaths_sum_999 <- result_999 |>
  mutate(deaths_split = age_split_result * pop_population) |>
  mutate(age_range = paste(pat_age_start, pat_age_end, sep = ":")) |>
  group_by(id) |>
  summarize(
    split_summed_pop = sum(pop_population),
    split_deaths = sum(deaths_split),
    age_ranges = paste(age_range, collapse = ", ")
  )

deaths_compare_999 <- unique_unsplit_999 |>
  left_join(result_split_deaths_sum_999, by = "id") |>
  mutate(pre_split_deaths = mx * split_summed_pop) |>
  mutate(diff_pct = abs(split_deaths - pre_split_deaths) / pre_split_deaths) |>
  filter(diff_pct > 0.03 & split_deaths > 100)

stopifnot(nrow(deaths_compare_999) == 0)

# Swap out population data with emp_pop_split_no_vr where applicable
emp_pop_split_no_vr_short <- emp_pop_split_no_vr |> select(age_group_id, location_id, year_id, sex_id, source_type, census_pop)

result_999 <- result_999 |>
  left_join(emp_pop_split_no_vr_short, by = c("age_group_id", "location_id", "year_id", "sex_id", "source_type"))

result_999 <- result_999 |>
  mutate(pop_population = ifelse(is.na(census_pop), pop_population, census_pop))

# Apply India SRS scalars to population
result_999 <- merge(
  result_999,
  srs_pop_scalars,
  by = c("ihme_loc_id", "sex_id"),
  all.x = TRUE
)

setDT(result_999)
result_999[ihme_loc_id %like% "IND" & year_id < 1993 & source_type == "SRS", ':=' (pop_population = pop_population / srs_scale)]
result_999[, srs_scale := NULL]

# df_main splitting ------------------------------------------------------------

# Save df_main data before age-splitting
readr::write_csv(
  df_main,
  fs::path("FILEPATH")
)

df_main_pre_split_deaths <- sum(df_main$deaths)

# Remove data that is already age specific
has_age_group <- df_main |>
  filter(!is.na(age_group_id))

df_main <- df_main |>
  filter(is.na(age_group_id))

rm(data_config, pattern_config, pop_config, age_splitter)

data_config <- splitter$AgeDataConfig(
  index = c("sex_id", "location_id", "ihme_loc_id", "year_id", "nid", "underlying_nid", "source_type_id", "source_type", "id", "outlier", "rank", "extraction_source", "sex_id_4"),
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "mx",
  val_sd = "mx_se"
)

pattern_config <- splitter$AgePatternConfig(
  by = list("sex_id", "location_id", "year_id"),
  age_key = "age_group_id",
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "pred_rate",
  val_sd = "pred_rate_sd"
)

pop_config <- splitter$AgePopulationConfig(
  index = c("age_group_id", "location_id", "year_id", "sex_id"),
  val = "population"
)

age_splitter <- splitter$AgeSplitter(
  data = data_config,
  pattern = pattern_config,
  population = pop_config
)

result_main <- age_splitter$split(
  data = df_main,
  pattern = msca_mx,
  population = pop_data,
  model = "rate",
  output_type = "rate",
  propagate_zeros = TRUE
)

result_main_backup <- copy(result_main)

# Add in data that was already age specific
has_age_group <- has_age_group |>
  select(
    source_type, source_type_id, sex_id, location_id, year_id, nid, underlying_nid,
    outlier, id, pat_age_start = age_start, pat_age_end = age_end,
    age_split_result = mx, age_split_result_se = mx_se, pop_population = population, rank, extraction_source, sex_id_4
  )

result_main <- bind_rows(result_main, has_age_group)

# Check for duplicates in result_main
check_result_main <- result_main |>
  group_by(
    nid, year_id, sex_id, location_id, pat_age_start, pat_age_end, underlying_nid,
    source_type_id, source_type, id, extraction_source
  ) |>
  mutate(n = n()) |>
  filter(n > 1) |>
  ungroup()

check_result_main_ids <- check_result_main$id

stopifnot(nrow(check_result_main) == 0)

unique_unsplit_main <- df_main |>
  select(id, mx)

result_split_deaths_sum_main <- result_main |>
  mutate(deaths_split = age_split_result * pop_population) |>
  mutate(age_range = paste(pat_age_start, pat_age_end, sep = ":")) |>
  group_by(id) |>
  summarize(
    split_summed_pop = sum(pop_population),
    split_deaths = sum(deaths_split),
    age_ranges = paste(age_range, collapse = ", ")
  )

deaths_compare_main <- unique_unsplit_main |>
  left_join(result_split_deaths_sum_main, by = "id") |>
  mutate(pre_split_deaths = mx * split_summed_pop) |>
  mutate(diff_pct = abs(split_deaths - pre_split_deaths) / pre_split_deaths) |>
  filter(diff_pct > 0.03 & split_deaths > 100)

stopifnot(nrow(deaths_compare_main) == 0)

# Swap out population data with emp_pop_split_no_vr where applicable
result_main <- result_main |>
  select(-age_group_id) |>
  left_join(age_map_gbd, by = c("pat_age_start" = "age_start", "pat_age_end" = "age_end"))

stopifnot(!any(is.na(result_main$age_group_id)))

result_main <- result_main |>
  left_join(emp_pop_split_no_vr_short, by = c("age_group_id", "location_id", "year_id", "sex_id", "source_type"))

result_main <- result_main |>
  mutate(pop_population = ifelse(is.na(census_pop), pop_population, census_pop))

# Apply SRS scalar to population
result_main <- merge(
  result_main,
  srs_pop_scalars,
  by = c("ihme_loc_id", "sex_id"),
  all.x = TRUE
)

setDT(result_main)
result_main[ihme_loc_id %like% "IND" & year_id < 1993 & source_type == "SRS", ':=' (pop_population = pop_population / srs_scale)]
result_main[, srs_scale := NULL]

# Save -------------------------------------------------------------------------

# Save result_999 data
readr::write_csv(
  result_999,
  fs::path("FILEPATH")
)

# Save result_main
readr::write_csv(
  result_main,
  fs::path("FILEPATH")
)
