
## Meta ------------------------------------------------------------------------

# Description: VR data preparation and creation of the 'pattern' data
# to be used in VR sex and age splitting. The pattern is essentially age-sex
# specific mx values that serve as the reference values for sex and age
# splitting with pyDisagg.

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Prep VR data to be split
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

# VR data to be split
df <- fread(fs::path("FILEPATH"))

# GBD mx values used in pattern selection
gbd_mx <- demInternal::get_dem_outputs(
  "with shock life table estimate",
  wslt_version,
  gbd_year = 2023,
  life_table_parameter_ids = 1)

# Raw pattern from MSCA
msca_mx <- arrow::read_parquet(mx_predictions_path)

# Age pattern handoff file
handoff_1 <- arrow::read_parquet(fs::path("FILEPATH"))

# Standard VR data
standard <- fread(fs::path("FILEPATH"))

# VR reliability
vr_reliability_adj_sheet <- fread(fs::path("FILEPATH"))

# Standard objects
# age maps
age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

# population estimates
pop <- fread(fs::path("FILEPATH"))

# locations
locs <- fread(fs::path("FILEPATH"))

# empirical pop
emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# Prep VR data to be split (df) ------------------------------------------------

# Filter data for splitting to only data that has more than 0 deaths
df <- df |>
  filter(deaths > 0)

# Capture number of pre deaths for use in checks later on
pre_deaths <- df[, .(deaths = sum(deaths))]

# Check to make sure we only have these sex_ids
stopifnot(all(df$sex_id %in% c(1, 2, 3, 4)))

# Identify sex_id 4 rows (unknown sex)
df_4_only <- df |>
  filter(sex_id == 4) |>
  mutate(row_id = row_number())

id_cols <- c("age_start", "age_end", "location", "date_start", "nid", "source_type", "extraction_source")

df_4_has_match_standard <- df_4_only |>
  semi_join(standard, by = id_cols)

df_4_only <- df_4_only |>
  mutate(sex_id_4 = ifelse(row_id %in% df_4_has_match_standard$row_id, "standard_sex_id_4", "split_sex_id_4")) |>
  select(-row_id)

df_4_only <- df_4_only |>
  left_join(age_map_gbd, by = c("age_start", "age_end"))

readr::write_csv(
  df_4_only,
  fs::path("FILEPATH")
)

# Drop off age_group_id column to be consistent with rest of data
df_4_only <- df_4_only |>
  select(-age_group_id)

# Add sex_id 4 data with new identification column back into df
df <- df |>
  filter(!(sex_id %in% c(4))) |>
  mutate(sex_id_4 = "not_sex_id_4") |>
  bind_rows(df_4_only)

warning("Keeping outliers")

# Save copy of df at this point for reference later
df_save <- copy(df)

# Row classifications
df <- df |>
  mutate(has_999 = age_start == 999 & age_end == 999) |>
  mutate(has_0_125 = age_start == 0 & age_end == 125) |>
  mutate(subgroup = !has_999 & !has_0_125)

# Handle location-sex-year-nid groupings that have subgroups AND a 0-125 row
# Keep only the 0-125 row in these cases
subgroup_and_0125_only <- df |>
  group_by(location, date_start, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  filter(all(subgroup | has_0_125)) |>
  filter(any(subgroup)) |>
  filter(any(has_0_125)) |>
  filter(subgroup)

df <- df |>
  anti_join(
    subgroup_and_0125_only,
    by = c(
      "location", "date_start", "sex_id", "age_start", "age_end", "nid",
      "underlying_nid", "source_type", "extraction_source"
    )
  )

# Handle location-sex-year-nid groupings that have subgroups AND a 0-125 row AND a 999 row
all_types <- df |>
  group_by(location, date_start, sex_id, nid, underlying_nid, source_type, extraction_source) |>
  filter(all(subgroup | has_0_125 | has_999)) |>
  filter(any(subgroup)) |>
  filter(any(has_0_125)) |>
  filter(any(has_999))

stopifnot(nrow(all_types) == 0)

# Rename location variable
df <- df |>
  rename(location_id = location)

# Change date_start & date_end to year_id
stopifnot(all(df$date_end - df$date_start) == 1)
df <- df |>
  mutate(year_id = date_start)

stopifnot(all(df$mx > 0))
stopifnot(all(df$mx_se > 0))

# Check for row dups - by location-sex-year-age_group
check <- df |>
  group_by(location_id, sex_id, year_id, age_start, age_end, nid, source_type, extraction_source) |>
  mutate(n = n()) |>
  filter(n() > 1) |>
  mutate(dup_id = cur_group_id()) |>
  ungroup() |>
  arrange(dup_id)

stopifnot(nrow(check) == 0)

# Check for instances of only have sex_id 3 and sex_id 4 for the same location-year
check <- df |>
  group_by(location_id, year_id, age_start, age_end, underlying_nid, source_type, extraction_source) |>
  summarize(only_3_and_4 = all(sex_id %in% c(3, 4)) && all(c(3, 4) %in% sex_id),
            .groups = "drop") |>
  filter(only_3_and_4)

stopifnot(nrow(check) == 0)

# First, do sex splitting for all undetermined sex age groups (sex_id 3 & 4)
needs_sex_split <- df |>
  filter(sex_id %in% c(3, 4))

# Filter down df to sex_id 1 and 2, but we will bring back sex-split versions of sex_id 3 & 4 later after sex splitting
df_1_2 <- df |>
  filter(sex_id %in% c(1, 2))

df_1_2_sum_deaths <- sum(df_1_2$deaths)

has_both_sexes <- df |>
  filter(sex_id %in% c(1, 2)) |>
  group_by(location_id, year_id, age_start, age_end, nid, underlying_nid, source_type, extraction_source) |>
  summarize(has_both = all(c(1, 2) %in% unique(sex_id)), .groups = "drop") |>
  filter(has_both)

# Filter out rows from needs_sex_split where there's a match in has_both_sexes
# for location_id, year_id, and sex_id is 3
needs_sex_split <- needs_sex_split |>
  filter(sex_id == 3) |>
  anti_join(
    has_both_sexes,
    by = c("location_id", "year_id", "age_start", "age_end", "nid", "underlying_nid", "source_type", "extraction_source")
  ) |>
  bind_rows(needs_sex_split |> filter(sex_id == 4))

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
kreg_df <- df |>
  filter(outlier == 0) |>
  left_join(vr_unoutliered, by = c("location_id", "sex_id")) |>
  filter(age_start.x <= age_end.y & age_end.x >= age_start.y) |>
  pull(location_id)

msca_mx <- msca_mx |>
  mutate(mx_column = ifelse(location_id %in% kreg_df, "kreg", "spxmod"))

# Manual overrides where we specifically want to use spxmod
use_spxmod <- c("KGZ", "MNG")
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

msca_mx <- msca_mx |>
  mutate(mx_column = ifelse(!is.na(gbd_mx), "gbd_mx", mx_column))

# Identify low_granularity data with populations over 2 million
pop_over_2mil <- pop |>
  filter(year_id == 2023) |>
  filter(age_group_id == 22) |>
  filter(population > 2000000) |>
  pull(location_id)

low_gran <- df |>
  filter(year_id == 2023) |>
  group_by(location_id, sex_id) |>
  filter(n() == 1) |>
  filter(age_start == 0,
         age_end == 125) |>
  filter(sex_id %in% c(1, 2, 3)) |>
  filter(location_id %in% pop_over_2mil)

# Drop sex_id 3 if we have sex_id 1 and 2
low_gran <- low_gran |>
  group_by(location_id) |>
  mutate(row_count = n()) |>
  ungroup() |>
  filter(!(row_count == 3 & !(sex_id %in% c(1, 2))) | row_count != 3) |>
  select(-row_count)

# If we have a sex_id 3 but not sex-specific rows for the same location, make two new
# low_gran rows, one for male and one for female
low_gran_only_sex_3 <- low_gran |>
  group_by(location_id) |>
  mutate(row_count = n()) |>
  ungroup() |>
  filter(row_count %in% c(1, 2) & sex_id == 3)

stopifnot(all(low_gran_only_sex_3$row_count == 1))

# Copy over the sex_id 3 data to ensure we have sex_id 1 and 2 data (the pattern only uses sex_id 1 and 2)
copy_sex_id_1 <- low_gran_only_sex_3 |>
  mutate(sex_id = 1)

copy_sex_id_2 <- low_gran_only_sex_3 |>
  mutate(sex_id = 2)

# Remove sex_id 3 from low_gran entirely now
low_gran <- low_gran |>
  filter(sex_id %in% c(1, 2))

low_gran <- bind_rows(low_gran, copy_sex_id_1, copy_sex_id_2)

# Remove duplicates
low_gran <- low_gran |>
  distinct(location_id, sex_id)

# Look for handoff 1 unoutliered vr data that has full or partial gbd standard age groups
# that corresponds with the low_granularity data
h1_check <- handoff_1 |>
  filter(outlier == 0) |>
  filter(source_type_name == "VR") |>
  filter(year_id == 2022) |>
  semi_join(low_gran, by = c("location_id", "sex_id")) |>
  inner_join(age_map_gbd, by = c("age_start", "age_end"), suffix = c("", "_rover")) |>
  filter(!(is.na(age_group_id_rover))) |>
  mutate(mx_22_available = TRUE)

# Use kreg to fill in gaps in the gbd standard data
kreg_h1 <- msca_mx |>
  select(location_id, sex_id, year_id, age_group_id, kreg) |>
  filter(year_id == 2023) |>
  semi_join(h1_check, by = c("location_id", "sex_id"))

h1_check <- kreg_h1 |>
  left_join(h1_check, by = c("location_id", "sex_id", "age_group_id")) |>
  mutate(mx_22_available = ifelse(is.na(mx_22_available), FALSE, mx_22_available)) |>
  mutate(mx_column = ifelse(!(mx_22_available), "kreg", "2022_mx")) |>
  mutate(mx = ifelse(is.na(mx), kreg, mx))

# Generate mx based on population estimates
h1 <- h1_check |>
  transmute(location_id, sex_id, age_start, age_end, age_group_id, mx, deaths, mx_column, year_id = 2023)

h1 <- h1 |>
  left_join(pop, by = c("location_id", "sex_id", "age_group_id", "year_id"))

stopifnot(is.na(h1$population) == 0)

# Calculate mx
h1 <- h1 |>
  mutate(mx = ifelse(mx_column == "kreg", mx, deaths / population))

# Create final object
final_low_gran_2022_mx <- h1

# Identify the 2023 low_gran data
low_gran_kreg <- low_gran |>
  anti_join(final_low_gran_2022_mx, by = c("location_id", "sex_id")) |>
  select(location_id, sex_id)

# Look at sex_id 3 rows specifically
sex_id_3_lgk <- low_gran_kreg |>
  group_by(location_id) |>
  filter(all(sex_id == 3))

final_sex_id_1_2 <- final_low_gran_2022_mx |>
  filter(sex_id %in% c(1,2)) |>
  select(location_id, sex_id) |>
  group_by(location_id) |>
  filter(any(sex_id == 1) & any(sex_id == 2)) |>
  ungroup() |>
  select(location_id) |>
  distinct()

sex_id_3_lgk_new <- sex_id_3_lgk |>
  anti_join(final_sex_id_1_2, by = "location_id")

stopifnot(nrow(sex_id_3_lgk) == nrow(sex_id_3_lgk_new))

# Low_granularity data that should use kreg (2023 data only)
low_gran_kreg$low_gran_kreg <- TRUE
low_gran_kreg$year_id <- 2023

msca_mx <- msca_mx |>
  left_join(low_gran_kreg, by = c("location_id", "year_id", "sex_id"))

msca_mx <- msca_mx |>
  mutate(low_gran_kreg = ifelse(is.na(low_gran_kreg), FALSE, as.logical(low_gran_kreg))) |>
  mutate(mx_column = ifelse(low_gran_kreg == TRUE, "kreg", mx_column))

# Low_granularity data that should use 2022 mx from handoff 1 vr
final_low_gran_2022_mx <- final_low_gran_2022_mx |>
  select(location_id, sex_id, year_id, low_gran_2022_mx = mx, age_group_id, mx_column_new = mx_column) |>
  mutate(year_id = 2023)

msca_mx <- msca_mx |>
  left_join(final_low_gran_2022_mx, by = c("location_id", "sex_id", "year_id", "age_group_id"))

msca_mx <- msca_mx |>
  mutate(mx_column = ifelse(is.na(mx_column_new), mx_column, mx_column_new))

check <- msca_mx |>
  group_by(location_id, sex_id, year_id, age_group_id) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

# A few adjustments to only use kreg where results create NaN when splitting
msca_mx <- msca_mx |>
  mutate(mx_column = ifelse(year_id == 2023 & location_id %in% c(55, 44906, 44959) & age_end <= 10, "kreg", mx_column)) |>
  mutate(mx_column = ifelse(year_id == 2023 & location_id %in% c(62), "kreg", mx_column))

# Use spxmod for vr_reliability_locs
vr_reliability_locs <- unique(vr_reliability_adj_sheet$ihme_loc_id)

vr_reliability_locs <- locs[ihme_loc_id %in% vr_reliability_locs, location_id]

mena_locs <- locs[super_region_name == "North Africa and Middle East", location_id]

RUS_locs <- locs |>
  filter(grepl("RUS", ihme_loc_id)) |>
  pull(location_id)

# Get post neonatal and above (1 month +) mx values from handoff 1 USA national
usa_fix <- handoff_1[location_id == 102 & year_id == 1959 & nid == 56939]

usa_fix <- usa_fix |>
  transmute(location_id, year_id, sex_id, age_group_id, mx) |>
  left_join(age_map_gbd, by = "age_group_id")

# Fill in Enn/Lnn with kreg
usa_fix_ennlnn <- msca_mx |>
  filter(
    location_id == 102, # USA national
    age_group_id %in% c(2, 3),
    year_id == 1959
  ) |>
  select(location_id, age_group_id, age_start, age_end, year_id, sex_id, mx = kreg)

usa_fix <- rbind(usa_fix, usa_fix_ennlnn)

stopifnot(nrow(usa_fix) == 50)

years_df <- data.frame(year_id = 1950:1958)

usa_fix <- usa_fix |> rename(original_year_id = year_id)

usa_fix <- usa_fix |>
  crossing(years_df) |>
  select(-original_year_id)

stopifnot(nrow(usa_fix) == 450)

usa_fix <- usa_fix |>
  rename(usa_fix_mx = mx) |>
  select(-age_start, -age_end)

msca_mx <- msca_mx |>
  left_join(usa_fix, by = c("location_id", "year_id", "sex_id", "age_group_id"))

zaf_locs <- locs |>
  filter(grepl("ZAF", ihme_loc_id))

zaf_kreg <- msca_mx |>
  filter(location_id %in% zaf_locs$location_id) |>
  filter(year_id >= 2019 & year_id <= 2023) |>
  select(kreg, location_id, year_id, sex_id, age_group_id)

zaf_kreg <- zaf_kreg |>
  left_join(pop, by = c("location_id", "sex_id", "year_id", "age_group_id")) |>
  mutate(kreg_deaths = kreg * population) |>
  group_by(location_id, year_id, age_group_id) |>
  mutate(sum_kreg_deaths = sum(kreg_deaths)) |>
  ungroup()

zaf_vr <- handoff_1 |>
  filter(grepl("ZAF", ihme_loc_id)) |>
  filter(year_id == 2018,
         nid == 490473)

zaf_vr_m <- zaf_vr |>
  filter(sex_id == 1) |>
  mutate(deaths_male = deaths) |>
  select(location_id, age_group_id, deaths_male)

zaf_vr_f <- zaf_vr |>
  filter(sex_id == 2) |>
  mutate(deaths_female = deaths) |>
  select(location_id, age_group_id, deaths_female)

zaf_vr <- zaf_vr_m |>
  left_join(zaf_vr_f, by = c("location_id", "age_group_id")) |>
  mutate(sum_deaths = deaths_male + deaths_female) |>
  mutate(male_prop = deaths_male / sum_deaths) |>
  mutate(female_prop = deaths_female / sum_deaths) |>
  select(location_id, age_group_id, male_prop, female_prop)

zaf_vr_1 <- zaf_vr |>
  transmute(location_id, age_group_id, sex_id = 1, proportion = male_prop)

zaf_vr_2 <- zaf_vr |>
  transmute(location_id, age_group_id, sex_id = 2, proportion = female_prop)

zaf_vr <- rbind(zaf_vr_1, zaf_vr_2)

zaf_kreg <- zaf_kreg |>
  left_join(zaf_vr, by = c("location_id", "age_group_id", "sex_id")) |>
  mutate(new_deaths = sum_kreg_deaths * proportion) |>
  mutate(zaf_fix_mx = new_deaths / population)

zaf_fix <- zaf_kreg |>
  transmute(location_id, year_id, sex_id, age_group_id, zaf_fix_mx)

msca_mx <- msca_mx |>
  left_join(zaf_fix, by = c("location_id", "year_id", "sex_id", "age_group_id"))

# Create the pred_rate and pred_rate_sd columns
msca_mx <- msca_mx |>
  mutate(pred_rate = case_when(
    !is.na(zaf_fix_mx) ~ zaf_fix_mx,
    !is.na(usa_fix_mx) ~ usa_fix_mx,
    location_id == 34 ~ spxmod, # Use spxmod for AZE
    location_id == 97 & year_id == 2022 ~ spxmod,   # Use spxmod for ARG 2022 data
    location_id == 161 ~ spxmod, # Use spxmod for BGD data
    location_id %in% vr_reliability_locs ~ spxmod,
    location_id %in% mena_locs ~ spxmod,
    location_id %in% RUS_locs & year_id == 2023 ~ spxmod,   # Use spxmod for RUS 2023 data
    mx_column == "kreg" ~ kreg,
    mx_column == "spxmod" ~ kreg, # defualt to kreg
    mx_column == "gbd_mx" ~ kreg, # default to kreg
    mx_column == "2022_mx" ~ low_gran_2022_mx
  )) |>
  mutate(pred_rate_sd = 0) # Always set sd to 0

stopifnot(!any(is.na(msca_mx$pred_rate)))

# Save the mx_column for plotting
save_msca_mx <- msca_mx |>
  select(kreg, spxmod, low_gran_2022_mx, usa_fix_mx, zaf_fix_mx, location_id, year_id, age_group_id, pred_rate, sex_id) |>
  pivot_longer(cols = -c(pred_rate, location_id, year_id, age_group_id, sex_id),
               names_to = "column",
               values_to = "value") |>
  filter(pred_rate == value) |>
  select(pred_rate, mx_column = column, location_id, year_id, age_group_id, sex_id)

# Trim msca_mx columns
setDT(msca_mx)
msca_mx <- msca_mx |>
  select(location_id, age_group_id, sex_id, year_id, pred_rate, pred_rate_sd)

# Add in age map
msca_mx <- msca_mx |>
  left_join(age_map_gbd, by = "age_group_id")

stopifnot(sum(is.na(msca_mx$age_start)) == 0)
stopifnot(sum(is.na(msca_mx$age_end)) == 0)

msca_age_groups <- msca_mx |>
  select(age_group_id, age_start, age_end) |>
  distinct()

needs_sex_split <- needs_sex_split |>
  left_join(msca_age_groups, by = c("age_start", "age_end"))

# Make a shorter pop object for join
pop_merge_pydisagg <- merge(pop, age_map_extended, by = "age_group_id")

pop_short <- pop_merge_pydisagg |>
  select(location_id, sex_id, year_id, age_group_id, population) |>
  filter(sex_id <= 2)

# Create a pop_short_msca_mx object
pop_short_msca_mx <- pop_short |>
  left_join(locs, by = c("location_id"))

# Add on deaths and population to msca data
msca_mx <- msca_mx |>
  left_join(pop_short_msca_mx, by = c("location_id", "sex_id", "year_id", "age_group_id"))

stopifnot(sum(is.na(msca_mx$population)) == 0)

msca_mx$deaths <- msca_mx$population * msca_mx$pred_rate

# Save -------------------------------------------------------------------------

# Investigate file of msca_mx (with pattern selection column)
readr::write_csv(
  save_msca_mx,
  fs::path("FILEPATH")
)

# msca_mx file for use in 08b and 08c
readr::write_csv(
  msca_mx,
  fs::path("FILEPATH")
)

# pop_short_msca_mx
readr::write_csv(
  pop_short_msca_mx[, !c("location_name_accent")],
  fs::path("FILEPATH")
)

# Data for sex splitting
readr::write_csv(
  needs_sex_split,
  fs::path("FILEPATH")
)

# Already sex-specific data
readr::write_csv(
  df_1_2,
  fs::path("FILEPATH")
)
