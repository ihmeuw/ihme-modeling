## Meta ------------------------------------------------------------------------

# Compile split data post sex and age splitting - there is a single file output
# from the VR splitting pipeline.

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Compile split data
#   4. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(dplyr)

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

# Data for age-splitting
# result_main
result_main <- fread(fs::path("FILEPATH"))

# result_999
result_999 <- fread(fs::path("FILEPATH"))

# df_999_only
df_999_only <- fread(fs::path("FILEPATH"))

# sex_id_4_pre_splitting
sex_id_4_pre_splitting <- fread(fs::path("FILEPATH"))

# Standard objects
# age maps
age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

# population estimates
pop <- fread(fs::path("FILEPATH"))

pop_short_msca_mx <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# Compile split data -----------------------------------------------------------

# Need to "add" on the 999 results to their respective main results
result_999$source <- "result_999"
result_main$source <- "result_main"

result_999 <- result_999 |>
  select(
    source, sex_id, location_id, year_id, nid, underlying_nid, source_type_id,
    source_type, outlier, id, rank, extraction_source, age_start = pat_age_start,
    age_end = pat_age_end, mx = age_split_result, mx_se = age_split_result_se,
    population = pop_population, sex_id_4
  )

result_main <- result_main |>
  select(
    source, sex_id, location_id, year_id, nid, underlying_nid, source_type_id,
    source_type, outlier, id, rank, extraction_source, age_start = pat_age_start,
    age_end = pat_age_end, mx = age_split_result, mx_se = age_split_result_se,
    population = pop_population, sex_id_4
  )

stopifnot(sum(is.na(result_999$mx)) == 0)

result_999 <- result_999 |>
  mutate(population = ifelse(location_id %in% c(161, 165) & source_type == "SRS", population / 10000, population))

result_main <- result_main |>
  mutate(population = ifelse(location_id %in% c(161, 165) & source_type == "SRS", population / 10000, population))

# Sex id 4 processing

sex_id_4_pre_splitting <- fs::path("FILEPATH") |> fread()

# result_999 rows that have a match in the standard gbd data
result_999 <- result_999 |>
  mutate(row_id = row_number())

sex_id_4_999 <- result_999 |>
  filter(sex_id_4 == "split_sex_id_4" | sex_id_4 == "standard_sex_id_4") |>
  semi_join(
    sex_id_4_pre_splitting,
    by = c("nid", "underlying_nid", "source_type_id", "source_type", "extraction_source")
  )

# result_main rows that have a match in the standard gbd data
result_main <- result_main |>
  mutate(row_id = row_number())

sex_id_4_main <- result_main |>
  filter(sex_id_4 == "split_sex_id_4" | sex_id_4 == "standard_sex_id_4") |>
  semi_join(
    sex_id_4_pre_splitting,
    by = c("nid", "underlying_nid", "source_type_id", "source_type", "extraction_source")
  )

# Part of the sex_id 4 data comes from gbd standard data
all_sex_id_4 <- rbind(sex_id_4_999, sex_id_4_main)

sex_id_4_for_10a <- all_sex_id_4 |>
  filter(sex_id_4 == "standard_sex_id_4")

sex_id_4_for_split <- all_sex_id_4 |>
  filter(sex_id_4 == "split_sex_id_4")

stopifnot(nrow(all_sex_id_4) == nrow(sex_id_4_for_10a) + nrow(sex_id_4_for_split))

# calculate deaths
sex_id_4_for_10a <- sex_id_4_for_10a |>
  mutate(deaths = mx * population)

# Check if it is possible to have duplicate age_group data that originated from a 999 group and another group
check <- sex_id_4_for_10a |>
  group_by(sex_id, nid, location_id, year_id, underlying_nid, source_type_id, source_type,
           extraction_source, age_start, age_end) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

# First, calc deaths
sex_id_4_for_split <- sex_id_4_for_split |>
  mutate(deaths = mx * population)

sex_id_4_for_split_combined <- sex_id_4_for_split |>
  group_by(sex_id, nid, location_id, year_id, underlying_nid, source_type_id, source_type,
           extraction_source, age_start, age_end) |>
  filter(n() == 2) |>
  filter(any(source == "result_999")) |>
  filter(any(source == "result_main")) |>
  summarise(
    deaths = sum(deaths),
    mx = pmax(mx),
    mx_se = pmax(mx_se),
    population = population[1]) |>
  slice(1) |>
  ungroup()

sex_id_4_for_split_single <- sex_id_4_for_split |>
  group_by(sex_id, nid, location_id, year_id, underlying_nid, source_type_id, source_type,
           extraction_source, age_start, age_end) |>
  filter(n() == 1)

# Resave sex_id 4 for split data
final_sex_id_4_for_split <- bind_rows(sex_id_4_for_split_combined, sex_id_4_for_split_single)

# drop original sex_id 4 data from result_ dfs
result_999 <- result_999 |>
  filter(!(row_id %in% sex_id_4_999$row_id)) |>
  select(-sex_id_4, -row_id)

result_main <- result_main |>
  filter(!(row_id %in% sex_id_4_main$row_id)) |>
  select(-sex_id_4, -row_id)

result_999_only <- result_999 |>
  filter(id %in% df_999_only$id)

result_999 <- result_999 |>
  filter(!(id %in% df_999_only$id))

mx_999 <- result_999 |>
  select(
    sex_id, location_id, year_id, age_start, age_end, mx_999 = mx, population,
    nid, underlying_nid, source_type_id, source_type, outlier, rank, extraction_source
  )

stopifnot(sum(is.na(mx_999$mx_999)) == 0)

result_main <- result_main |>
  mutate(id = row_number())

mx_main <- result_main |>
  select(
    sex_id, location_id, year_id, age_start, age_end, mx_main = mx, mx_se, population,
    nid, underlying_nid, source_type_id, source_type, outlier, id, rank, extraction_source
  )

stopifnot(sum(is.na(mx_main$mx_main)) == 0)

add_rows <- inner_join(
  mx_main,
  mx_999,
  by = c(
    "sex_id", "location_id", "year_id", "age_start", "age_end", "population",
    "nid", "underlying_nid", "source_type_id", "source_type",
    "extraction_source"
  )
)

# Check NA values
check <- add_rows |>
  filter(is.na(mx_999) | is.na(mx_main))

stopifnot(sum(is.na(add_rows$mx_999)) == 0)
stopifnot(sum(is.na(add_rows$mx_main)) == 0)

# Remove the rows that were in df_main that have been added already (with 999) into the add_rows df
result_main <- result_main |>
  filter(!(id %in% add_rows$id)) |>
  select(-source, -id) |>
  distinct()

add_rows <- add_rows |>
  mutate(deaths_999 = mx_999 * population,
         deaths_main = mx_main * population) |>
  mutate(deaths_combined = deaths_999 + deaths_main) |>
  mutate(mx_combined = deaths_combined / population) |>
  select(
    sex_id, location_id, year_id, age_start, age_end, mx = mx_combined, mx_se,
    population, nid, source_type_id, source_type, underlying_nid, outlier = outlier.x, rank = rank.x,
    extraction_source
  )

# Save results together
result_999_only <- result_999_only |>
  select(-source, -id)

result <- rbind(result_main, add_rows, result_999_only)

check <- result |>
  group_by(nid, location_id, sex_id, year_id, underlying_nid, source_type_id, source_type,
           extraction_source, age_start, age_end) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

# add in sex_id_4 data deaths to result
final_sex_id_4_for_split <- final_sex_id_4_for_split |>
  rename(deaths_sex_id_4 = deaths) |>
  select(-mx, -mx_se, -population)

# Dupe check
check <- final_sex_id_4_for_split |>
  group_by(nid, location_id, sex_id, year_id, underlying_nid, source_type_id, source_type,
           extraction_source, age_start, age_end) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

result <- result |>
  left_join(final_sex_id_4_for_split,
            by = c("location_id", "sex_id", "year_id", "nid", "underlying_nid", "source_type_id", "source_type",
                   "extraction_source", "age_start", "age_end")) |>
  rename(outlier = outlier.x, rank = rank.x) |>
  select(-outlier.y, -rank.y)

# Create a deaths column
result <- result |>
  mutate(deaths = mx * population)

result <- result |>
  mutate(deaths = ifelse(is.na(deaths_sex_id_4), deaths, deaths + deaths_sex_id_4)) |>
  select(-deaths_sex_id_4)

check <- result |>
  group_by(
    nid, year_id, sex_id, location_id, age_start, age_end, underlying_nid,
    source_type_id, source_type, extraction_source
  ) |>
  mutate(n = n()) |>
  filter(n > 1) |>
  ungroup()

stopifnot(nrow(check) == 0)
stopifnot(sum(is.na(result$deaths)) == 0)
stopifnot(result$deaths > 0)

# Add in sex_id 4 data
result <- rbind(result, sex_id_4_for_10a)

# Add in age group ids
result <- result |>
  left_join(age_map_gbd, by = c("age_start", "age_end"))

stopifnot(sum(is.na(result$age_group_id)) == 0)

post_deaths <- result[, .(deaths = sum(deaths))]

# Assert no NAs in multiple columns
assertable::assert_values(
  result,
  colnames = c("mx", "deaths", "population", "sex_id", "location_id", "year_id", "nid"),
  test = "not_na"
)

check <- result |>
  group_by(nid, underlying_nid, location_id, year_id, sex_id, age_group_id, source_type_id, source_type, extraction_source) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

# Save -------------------------------------------------------------------------

# Save final data
readr::write_csv(
  result,
  fs::path("FILEPATH")
)
