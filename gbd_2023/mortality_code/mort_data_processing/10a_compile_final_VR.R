## Meta ------------------------------------------------------------------------

# Description: Compile gbd ages data and split nonstandard ages data to produce final VR data

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in, format, and remove shocks from gbd ages VR data
#   4. Read in, format, and remove shocks from age split VR data
#   5. Combine VR datasets
#   6. Additional cleaning and outliering
#   7. Save

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

source(fs::path("FILEPATH"))
source(fs::path("FILEPATH"))
source(fs::path("FILEPATH"))

# Read in internal inputs ------------------------------------------------------

age_map_gbd <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# read in shocks
shocks <- fread(fs::path("FILEPATH"))

# read in completeness estimates
dt_comp_est <- fread(fs::path("FILEPATH"))

# read in ssa inclusion sheet
ssa_data_inclusion <- fread(fs::path("FILEPATH"))
ssa_data_inclusion <- ssa_data_inclusion[
  ,
  .(location_id, nid, underlying_nid, source_type = source_type_name, updated_outlier, updated_notes)
]

# Read in, format, and remove shocks from gbd ages VR data ---------------------

vr_gbd_ages <- fread(
  fs::path("FILEPATH")
)

setnames(
  vr_gbd_ages,
  c("location", "date_start", "country_ihme_loc_id"),
  c("location_id", "year_id", "ihme_loc_id")
)
drop_cols <- c(
  "date_end", "date_granularity", "date_format", "age_unit", "age_granularity",
  "location_granularity", "location_type", "dem_measure_id", "outlier_note"
)
vr_gbd_ages <- vr_gbd_ages[, -..drop_cols]
vr_gbd_ages[locs, ihme_loc_id := i.ihme_loc_id, on = "location_id"]

vr_gbd_ages[age_map_gbd, age_group_id := i.age_group_id, on = c("age_start", "age_end")]
vr_gbd_ages <- vr_shocks_subtraction(vr_gbd_ages, shocks)
vr_gbd_ages[, mx := deaths / sample_size]

vr_gbd_ages[, mx_denominator := sample_size]

vr_gbd_ages$onemod_process_type <- "standard gbd age group data"
vr_gbd_ages$sex_id_4 <- "not_sex_id_4--gbd_standard_data"

# Read in, format, and remove shocks from age split VR data --------------------

vr_age_split <- fread(fs::path("FILEPATH"))

# add official titles
vr_age_split <- demInternal::merge_ghdx_record_fields(vr_age_split, nid_col = "underlying_nid")
setnames(vr_age_split, "title", "underlying_title")
vr_age_split <- demInternal::merge_ghdx_record_fields(vr_age_split)

# other columns
vr_age_split[, series_name := NA]
vr_age_split[loc_map, ihme_loc_id := i.ihme_loc_id, on = "location_id"]

# age-sex split shocks removal
vr_age_split <- vr_shocks_subtraction(vr_age_split, shocks)
vr_age_split[, mx := deaths / population]
vr_age_split[, qx := demCore::mx_to_qx(mx, age_end - age_start)]

# add sample size
vr_age_split <- vr_age_split |>
  rename(mx_denominator = population) |> # save original population
  left_join(pop[, c("location_id", "sex_id", "year_id", "age_group_id", "population")], by = c("location_id", "sex_id", "year_id", "age_group_id")) |>
  mutate(sample_size = (ifelse(mx >= 0.99, 0.99, mx) * (1 - ifelse(mx >= 0.99, 0.99, mx))) / mx_se^2)
vr_age_split[mx == 0, ':=' (deaths = 0, sample_size = mx_denominator)]

# if sample size is greater than population then use the lower value for that row
vr_age_split <- vr_age_split |>
  mutate(sample_size = ifelse(sample_size > population, pmin(sample_size, mx_denominator), sample_size)) |>
  setDT()

assertable::assert_values(
  vr_age_split,
  colnames = c("sample_size", "population", "deaths", "mx", "mx_se"),
  test = "not_na"
)

readr::write_csv(
  vr_age_split,
  fs::path("FILEPATH")
)

# use gbd pop for VR in all cases
vr_age_split[source_type == "VR", mx_denominator := population]

vr_age_split$onemod_process_type <- "age-sex split data"

vr_age_split <- vr_age_split |>
  mutate(sex_id_4 = ifelse(is.na(sex_id_4), "not_sex_id_4--age-sex split_data", sex_id_4))

vr_age_split <- vr_age_split |> select(-source, -id, -row_id)

# Combine VR datasets ----------------------------------------------------------

vr_combine <- rbind(vr_gbd_ages, vr_age_split)

# Check that all mx_denominator values are not NA
stopifnot(all(!is.na(vr_combine$mx_denominator)))

any(is.na(vr_combine$deaths))
table(vr_combine$onemod_process_type)

# ensure we don't have more than 2 rows per grouping
check <- vr_combine |>
  group_by(nid, underlying_nid, sex_id, location_id, year_id, age_start, age_end, extraction_source) |>
  dplyr::mutate(n_rows = n()) |>
  dplyr::filter(n_rows > 2)

stopifnot(nrow(check) == 0)

# first, add on original sex_id 4 data from split data to standard data

# grab sex_id 4 that is to be added onto the standard data
sex_id_4_data <- vr_combine |>
  filter(sex_id_4 == "standard_sex_id_4")

stopifnot(all(sex_id_4_data$onemod_process_type == "age-sex split data"))

table(vr_combine$sex_id_4)

# drop sex_id 4 data from vr_combine so we don't duplicate later
vr_combine <- vr_combine |>
  filter(sex_id_4 != "standard_sex_id_4")

# add deaths
sex_id_4_data <- sex_id_4_data |>
  select(location_id, sex_id, nid, year_id, source_type_id, underlying_nid, age_start, age_end, deaths_sex_id_4 = deaths, extraction_source)

# get standard data that sex_id 4 data can be added to
standard_add_sex_id_4 <- vr_combine |>
  filter(onemod_process_type == "standard gbd age group data")

check <- standard_add_sex_id_4 |>
  group_by(location_id, sex_id, year_id, nid, age_start, age_end, source_type_id, underlying_nid, extraction_source) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

check <- sex_id_4_data |>
  group_by(location_id, sex_id, year_id, nid, age_start, age_end, source_type_id, underlying_nid, extraction_source) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

pre_mx <- standard_add_sex_id_4

pre_nrow <- nrow(standard_add_sex_id_4)

standard_add_sex_id_4 <- standard_add_sex_id_4 |>
  rename(deaths_original = deaths) |>
  left_join(
    sex_id_4_data,
    by = c(
      "location_id", "sex_id", "year_id", "source_type_id", "nid", "underlying_nid",
      "age_start", "age_end", "extraction_source"
    )
  ) |>
  mutate(deaths_sex_id_4 = ifelse(is.na(deaths_sex_id_4), 0, deaths_sex_id_4)) |>
  mutate(deaths = deaths_original + deaths_sex_id_4) |>
  mutate(mx = deaths / mx_denominator)

post_nrow <- nrow(standard_add_sex_id_4)

stopifnot(pre_nrow == post_nrow)

post_mx <- standard_add_sex_id_4

check <- standard_add_sex_id_4 |>
  group_by(location_id, sex_id, year_id, nid, age_start, age_end, source_type, underlying_nid, extraction_source) |>
  filter(n() > 1)

stopifnot(nrow(check) == 0)

# check changes
compare <- inner_join(pre_mx, post_mx, by = c("location_id", "sex_id", "year_id", "nid", "age_start", "age_end", "source_type_id", "underlying_nid", "extraction_source"))

# Check how often the mx differs by 30% or more
compare <- compare |>
  filter(mx.x / mx.y > 1.3 | mx.x / mx.y < .7)

# If this is happening more than 1% of the time, throw an error to investigate
stopifnot(nrow(compare) / nrow(standard_add_sex_id_4) < 0.01)

# Finish with check
standard_add_sex_id_4 <- standard_add_sex_id_4 |>
  mutate(onemod_process_type = ifelse(deaths_sex_id_4 == 0, "standard gbd age group data", "standard+split_data"))

table(standard_add_sex_id_4$onemod_process_type, useNA = "always")
any(is.na(standard_add_sex_id_4$deaths))

check <- standard_add_sex_id_4[deaths_sex_id_4 == 0]

standard_add_sex_id_4 <- standard_add_sex_id_4 |>
  select(-deaths_original, -deaths_sex_id_4)

table(vr_combine$onemod_process_type, useNA = "always")

# remake vr_combine
vr_combine <- vr_combine |>
  filter(onemod_process_type == "age-sex split data") |>
  bind_rows(standard_add_sex_id_4)

table(vr_combine$onemod_process_type)
any(is.na(vr_combine$deaths))

# finished with sex_id 4 adjustment
vr_combine_save <- copy(vr_combine)
table(vr_combine_save$onemod_process_type)

# Add 999 age-sex split data ---------------------------------------------------

only_999_groups <- fread(
  fs::path("FILEPATH")
)

vr_combine <- vr_combine |>
  mutate(temp_row_id = row_number())

# identify 999 rows
vr_combine_999_only <- vr_combine |>
  filter(onemod_process_type == "age-sex split data") |>
  semi_join(only_999_groups, by = c("location_id", "sex_id", "nid", "year_id", "source_type_id", "underlying_nid", "extraction_source"))

# identify split rows that are NOT 999 rows
vr_combine_split <- vr_combine |>
  filter(onemod_process_type == "age-sex split data") |>
  filter(!temp_row_id %in% vr_combine_999_only$temp_row_id) |>
  select(-temp_row_id)

# filter out rows with zero deaths
vr_combine_999_only <- vr_combine_999_only |>
  filter(deaths > 0) |>
  select(-temp_row_id)

# get standard data
vr_combine_standard <- vr_combine |>
  filter(onemod_process_type != "age-sex split data")

# identify standard rows that need 999 rows merged on
standard_add_999 <- vr_combine_standard |>
  semi_join(
    vr_combine_999_only,
    by = c(
      "location_id", "sex_id", "nid", "year_id", "source_type_id", "underlying_nid",
      "age_group_id", "extraction_source"
    )
  )

# get standard data that do not need 999 added in
vr_combine_standard <- vr_combine_standard |>
  anti_join(
    vr_combine_999_only,
    by = c(
      "location_id", "sex_id", "nid", "year_id", "source_type_id", "underlying_nid",
      "age_group_id", "extraction_source"
    )
  )

# add deaths to standard_add_999 from vr_combine_999_only
vr_combine_999_only <- vr_combine_999_only |>
  select(location_id, sex_id, nid, year_id, source_type_id, underlying_nid, age_start, age_end, deaths_999 = deaths, extraction_source)

pre_999 <- standard_add_999

standard_add_999 <- standard_add_999 |>
  rename(deaths_original = deaths) |>
  left_join(
    vr_combine_999_only,
    by = c(
      "location_id", "sex_id", "nid", "year_id", "source_type_id", "underlying_nid",
      "age_start", "age_end", "extraction_source"
    )
  ) |>
  mutate(deaths = deaths_original + deaths_999) |>
  mutate(mx = deaths / mx_denominator) |>
  mutate(onemod_process_type = "standard+split_data")

post_999 <- standard_add_999

# check changes
compare <- inner_join(
  pre_999,
  post_999,
  by = c(
    "location_id", "sex_id", "year_id", "nid", "age_start", "age_end", "source_type",
    "underlying_nid", "extraction_source"
  )
)

# Check how often the mx differs by 30% or more
compare <- compare |>
  filter(mx.x / mx.y > 1.3 | mx.x / mx.y < .7)

# If this is happening more than 1% of the time, throw an error to investigate
stopifnot(nrow(compare) / nrow(standard_add_999) < 0.01)

# remove deaths_999 and deaths_original that were kept for viewing
standard_add_999 <- standard_add_999 |>
  select(-deaths_999, - deaths_original)

# merge data back together
vr_combine <- rbind(vr_combine_standard[, -c("temp_row_id")], vr_combine_split, standard_add_999[, -c("temp_row_id")])

vr_combine <- vr_combine |>
  select(-mx_denominator)

assertable::assert_values(
  vr_combine,
  colnames = c("nid", "location_id", "year_id", "age_group_id", "sex_id", "sample_size", "deaths", "mx"),
  test = "not_na"
)

# Additional cleaning and outliering -------------------------------------------

# keep outliering from handoff 1 with note
vr_combine[outlier == 1, outlier_note := "Outliered in handoff 1; "]
vr_combine[is.na(outlier_note), outlier_note := ""]

age_split <- vr_combine |>
  filter(onemod_process_type == "age-sex split data") |>
  mutate(group = paste(nid, location_id, year_id, age_group_id, sex_id, sep = "_"))

gbd_standard <- vr_combine |>
  filter(onemod_process_type != "age-sex split data") |>
  mutate(group = paste(nid, location_id, year_id, age_group_id, sex_id, sep = "_"))

to_add <- age_split |>
  filter(group %in% gbd_standard$group)

# save
readr::write_csv(
  to_add[, -c("group")],
  fs::path("FILEPATH")
)

# re-add series_name for age sex split data
vr_combine[title %like% "WHO", series_name := "WHO"]
vr_combine[title %like% "United Nations Demographic Yearbook", series_name := "DYB"]
vr_combine[source_type == "DSP", series_name := "DSP"]
vr_combine[title %like% "Human Mortality Database", series_name := "HMD"]
vr_combine[source_type == "SRS", series_name := "SRS"]
vr_combine[source_type_id == 55, series_name := "MCCD"]
vr_combine[
  title %like% "Vital Statistics|Vital Registration|NVSS|Death Registry|Death Register|Death Registration|Civil Registration|Mortality Statistics|VR|Mortality Registration" &
    is.na(series_name),
  series_name := "CRVS"
]
vr_combine[is.na(series_name), series_name := "Custom"]
vr_combine[, series_name := factor(series_name, levels = c("MCCD", "low_gran", "DYB", "HMD", "Custom", "CRVS", "DSP", "SRS", "WHO"))]
# add version to WHO
vr_combine[series_name == "WHO", version_date := gsub("WHO Mortality Database Version ", "", title)]
vr_combine[!is.na(version_date), version_date := gsub(" ", " 1, ", version_date)]
vr_combine[, version_date := as.Date(version_date, format = '%B %d, %Y')]

# deduplicate with age split data de-prioritized
vr_ranked <- unique(vr_combine[, c("nid", "location_id", "year_id", "series_name", "version_date", "extraction_source")])
vr_ranked <- vr_ranked[order(location_id, year_id, -series_name, -version_date, extraction_source)]
vr_ranked[, rank := seq(.N), by = .(location_id, year_id)]
vr_combine <- merge(
  vr_combine[, -c("rank")],
  vr_ranked,
  by = c("nid", "location_id", "year_id", "series_name", "version_date", "extraction_source"),
  all.x = TRUE
)
vr_test <- unique(vr_combine[rank == 1, c("location_id", "year_id", "nid")])
vr_test[, dup_sources := .N, by = c("location_id", "year_id")]
assert_values(vr_test, "dup", test = "equal", test_val = 1)
vr_combine[rank == 1 & outlier == 1, ':=' (outlier = 0, outlier_note = paste0(outlier_note, "Prioritized source; "))]
vr_combine[!rank == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Duplicate non-prioritized source; "))]
vr_combine[, dup := .N, by = c("nid", "rank", "location_id", "year_id", "sex_id", "age_group_id", "source_type")]
vr_combine[
  rank == 1 & dup == 2 & onemod_process_type == "age-sex split data",
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Duplicate age/sex split source; "))
]
vr_combine[, dup := NULL]
vr_combine[, rank := seq(.N), by = .(location_id, year_id, age_group_id, sex_id)]

# check for duplicate location/year/age_group_id/sex_id in final unoutliered data
vr_raw_dupes <- vr_combine[outlier == 0]
vr_raw_dupes[, dup := .N, by = c("location_id", "year_id", "sex_id", "age_group_id", "source_type")]
assert_values(vr_raw_dupes, "dup", test = "equal", test_val = 1)

# outlier if >50% higher than previous year
vr_combine <- vr_combine[order(location_id, age_group_id, sex_id, year_id)]
vr_combine[
  outlier == 0, diff_prev := deaths - shift(deaths),
  by = c("location_id", "sex_id", "age_group_id")
]
vr_combine[
  outlier == 0, perc_diff_prev := ((deaths - shift(deaths)) / shift(deaths)) * 100,
  by = c("location_id", "sex_id", "age_group_id")
]
vr_combine[
  outlier == 0, diff_next := shift(deaths, type = "lead") - deaths,
  by = c("location_id", "sex_id", "age_group_id")
]
vr_combine[
  outlier == 0, perc_diff_next := ((shift(deaths, type = "lead") - deaths) / deaths) * 100,
  by = c("location_id", "sex_id", "age_group_id")
]
# only keep where comparison is one year previous
vr_combine[outlier == 0, ':=' (year_prev = shift(year_id), year_next = shift(year_id, type = "lead"))]
vr_combine[(year_prev != (year_id - 1) & outlier == 0), perc_diff_prev := NA]
vr_combine[perc_diff_prev == Inf, perc_diff_prev := NA]
vr_combine[(year_next != (year_id + 1) & outlier == 0), perc_diff_next := NA]
vr_combine[perc_diff_next == Inf, perc_diff_next := NA]
vr_combine[(abs(diff_prev) > 50) & (perc_diff_prev < -50) & (abs(diff_next) > 50) & (perc_diff_next > 50), diff_too_high := 1]
vr_combine[(abs(diff_prev) > 50) & (perc_diff_prev > 50) & (abs(diff_next) > 50) & (perc_diff_next < -50), diff_too_high := 1]
vr_combine[,
  ':=' (diff_prev = NULL, perc_diff_prev = NULL, year_prev = NULL,
        diff_next = NULL, perc_diff_next = NULL, year_next = NULL)
]

vr_combine[
  rank == 1 & diff_too_high == 1 & !year_id %in% c(2020:2021) &
    age_group_id %in% age_map_gbd[, age_group_id] & sex_id %in% 1:2 &
    !series_name %in% c("DSP", "SRS"),
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Far from neighbors; "))
]
vr_combine[, diff_too_high := NULL]

# add completeness adjustment
vr_combine <- merge(
  vr_combine,
  dt_comp_est,
  by = c("location_id", "year_id", "sex_id", "source_type_id", "source_type"),
  all.x = TRUE
)

vr_combine[
  is.na(comp_adult) & is.na(comp_10_14) & is.na(comp_5_9) & is.na(comp_u5),
  ':=' (comp_adult = 1, comp_10_14 = 1, comp_5_9 = 1, comp_u5 = 1, missing_completeness = 1)
]

# adjust LBN completeness
vr_combine[
  location_id == 146 & age_group_id %in% c(2:3, 388:389, 238, 34),
  ':=' (comp_adult = 1, comp_10_14 = 1, comp_5_9 = 1, comp_u5 = 1)
]

# adjust ZAF completeness
vr_combine[
  nid == 546150 & year_id >= 2019,
  ':=' (comp_adult = 1, comp_10_14 = 1, comp_5_9 = 1, comp_u5 = 1)
]

# adjust ALB completeness
vr_combine[
  location_id == 43 & year_id >= 2016,
  comp_u5 := 1
]

# calculate deaths_adj and mx_adj
vr_combine[age_start >= 0 & age_end <= 5, ':=' (deaths_adj = deaths / comp_u5, mx_adj = mx / comp_u5, completeness = comp_u5)]
vr_combine[age_start >= 5 & age_end <= 10, ':=' (deaths_adj = deaths / comp_5_9, mx_adj = mx / comp_5_9, completeness = comp_5_9)]
vr_combine[age_start >= 10 & age_end <= 15, ':=' (deaths_adj = deaths / comp_10_14, mx_adj = mx / comp_10_14, completeness = comp_10_14)]
vr_combine[age_start >= 15, ':=' (deaths_adj = deaths / comp_adult, mx_adj = mx / comp_adult, completeness = comp_adult)]

# flag complete VR
vr_combine[, complete_vr := ifelse(completeness == 1, 1, 0)]
vr_combine[missing_completeness == 1, ':=' (completeness = NA, complete_vr = NA)]
assertable::assert_values(
  vr_combine[is.na(missing_completeness)],
  colnames = c("completeness", "complete_vr"),
  test = "not_na"
)
vr_combine[, missing_completeness := NULL]

# calculate mx_se_adj
vr_combine[, mx_se_adj := sqrt((ifelse(mx_adj >= 0.99, 0.99, mx_adj) * (1 - ifelse(mx_adj >= 0.99, 0.99, mx_adj))) / sample_size)]
vr_combine[mx_adj == 0, mx_se_adj := 0]

vr_combine[, sample_size_orig := sample_size]

# calculate qx
vr_combine[, qx := demCore::mx_to_qx(mx, age_end - age_start)]
vr_combine[, qx_adj := demCore::mx_to_qx(mx_adj, age_end - age_start)]

# manual outliering
vr_combine <- manual_outliering_final_vr(vr_combine)

# unoutlier IND subnational VR w/ >90% completeness for 5+
vr_combine[
  series_name == "CRVS" & location_id %in% locs[ihme_loc_id %like% "IND_"]$location_id &
    comp_adult > 0.9 & age_group_id %in% c(6:20, 30:32, 235),
  `:=` (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered (IND CRVS >90% completeness); "))
]

# additional IND subnational VR outliering decisions
vr_combine[
  location_id == 43881 & series_name == "CRVS" & year_id %in% 2009:2015 & !is.na(completeness),
  `:=` (outlier = 0, outlier_note = paste0(outlier_note, "Manually un-outliered; "))
]
vr_combine[
  location_id %in% locs[ihme_loc_id %like% "IND_"]$location_id & series_name == "CRVS" & is.na(completeness),
  `:=` (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
]
vr_combine[
  location_id %in% locs[ihme_loc_id %like% "IND_"]$location_id & series_name == "CRVS" & age_group_id %in% c(2:3, 388:389, 238, 34),
  `:=` (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
]
vr_combine[
  location_id %in% c(
    4841, 4842, 4843, 4846, 4849, 4851, 4852, 4855, 4856, 4867, 4871, 4872, 4873,
    4874, 4875, 43872, 43873, 43874, 43877, 43880, 43881, 43882, 43883, 43884,
    43887, 43888, 43890, 43891, 43893, 43895, 43896, 43898, 43899, 43901, 43902,
    43903, 43904, 43905, 43906, 43908, 43909, 43910, 43913, 43917, 43919, 43920,
    43922, 43927, 43929, 43936, 43937, 43938, 43939, 43940, 43941, 43942, 44538
  ) & series_name == "CRVS",
  `:=` (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
]

vr_combine[
  location_id %in% c(4850, 43917) & series_name == "CRVS" & year_id == 2010,
  `:=` (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
]
vr_combine[
  location_id == 4857 & series_name == "CRVS" & age_group_id %in% c(30:32, 235),
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
]
vr_combine[
  location_id %in% c(4863, 43894, 43930) & series_name == "CRVS" & year_id == 2018,
  `:=` (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
]

# update outlier status based on SSA Data Inclusion Sheet
vr_combine <- merge(
  vr_combine,
  ssa_data_inclusion,
  by = c("location_id", "nid", "underlying_nid", "source_type"),
  all.x = TRUE
)

# outliering changes to prevent dupes
vr_combine[location_id == 196 & nid == 287600 & year_id %in% 1997:2015, updated_outlier := 1]
vr_combine[location_id == 196 & nid == 546150 & year_id %in% 2015:2018, updated_outlier := 1]
vr_combine[location_id == 196 & nid == 426273 & year_id %in% 2016, updated_outlier := 1]
vr_combine[location_id == 196 & nid == 140967 & year_id %in% 2017:2018, updated_outlier := 1]
vr_combine[location_id == 203 & nid == 287600 & year_id %in% 2011:2012, updated_outlier := 1]

vr_combine[
  location_id == 215 & year_id %in% c(1984, 1987),
  updated_outlier := NA
]
vr_combine[
  location_id == 215 & year_id == 1985 & onemod_process_type == "age-sex split data",
  updated_outlier := NA
]
vr_combine[
  location_id == 196 & year_id %in% 1993:1996 & nid == 287600,
  updated_outlier := NA
]
vr_combine[
  location_id %in% c(196, 482:490) & year_id %in% 1997:2005 & nid == 107077,
  updated_outlier := NA
]
vr_combine[
  location_id %in% c(196, 482:490) & year_id %in% 2006:2018 & title %like% "South Africa Vital Registration - Causes of Death",
  updated_outlier := NA
]
vr_combine[
  location_id == 198 & age_group_id %in% c(2:3, 388:389, 238, 34),
  updated_outlier := NA
]
vr_combine[
  location_id == 195 & age_group_id %in% c(31:32, 235) & year_id %in% c(2009:2011, 2014, 2016:2021),
  updated_outlier := NA
]
vr_combine[
  location_id %in% c(196, 482:490) & age_group_id %in% c(32, 235) & year_id %in% c(2019:2023),
  updated_outlier := NA
]

vr_combine[!is.na(updated_outlier), outlier := updated_outlier]
vr_combine[
  !is.na(updated_outlier),
  outlier_note := ifelse(!is.na(updated_notes), paste0("SSA data inclusion (", updated_notes, "); "), "SSA data inclusion; ")
]
vr_combine[, c("updated_outlier", "updated_notes") := NULL]

vr_combine[
  location_id == 181 & year_id %in% 1984:1995,
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
] # MDG
vr_combine[
  location_id == 195 & age_group_id %in% c(30:32, 235),
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
] # NAM
vr_combine[
  location_id %in% c(196, 482:490) & age_group_id %in% c(32, 235),
  `:=` (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
] # ZAF + subnats
vr_combine[
  location_id == 203 & year_id <= 1960 & age_group_id %in% c(2:3, 388:389),
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
] # CPV

vr_combine[outlier == 0, outlier_note := gsub("Manually outliered; Manually un-outliered; ", "Manually un-outliered; ", outlier_note)]
vr_combine[outlier == 0, outlier_note := gsub("^Manually un-outliered; ", "", outlier_note)]
vr_combine[outlier == 1, outlier_note := gsub("Manually un-outliered; Manually outliered; ", "Manually outliered; ", outlier_note)]
vr_combine[outlier == 1, outlier_note := gsub("Manually outliered; Manually outliered; ", "Manually outliered; ", outlier_note)]

# outlier based on drops in mx causing bad age distribution
country_pop <- unique(pop[age_group_id == 22 & sex_id == 3, c("location_id", "year_id", "population")])
setnames(country_pop, "population", "country_pop")
vr_combine <- merge(
  vr_combine,
  country_pop,
  by = c("location_id", "year_id"),
  all.x = TRUE
)
age_group_id_order <- c(2, 3, 388, 389, 238, 34, 6:20, 30:32, 235)
vr_combine[, age_group_id := factor(age_group_id, levels = age_group_id_order)]
vr_combine <- vr_combine[order(location_id, year_id, sex_id, age_group_id)]
vr_combine[outlier == 0, mx_adj_diff_prev := mx_adj - shift(mx_adj), by = c("location_id", "year_id", "sex_id")]
vr_combine[mx_adj_diff_prev < 0 & age_group_id %in% c(20, 30:32, 235), bad_age_dist := 1]
vr_combine <- vr_combine %>%
  group_by(location_id, year_id, sex_id) %>%
  tidyr::fill(bad_age_dist, .direction = "down") %>%
  dplyr::ungroup() %>% setDT
vr_combine[bad_age_dist == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Bad old age distribution; "))]
vr_combine[, ':=' (mx_adj_diff_prev = NULL, bad_age_dist = NULL, country_pop = NULL,
                   age_group_id = as.numeric(as.character(age_group_id)))]

# outlier mx >= 1
vr_combine[mx_adj >= 1 & !age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Mx >= 1; "))]

# outlier based on qx being too low in old ages (80-90)
vr_combine[deaths > 50 & age_group_id == 32 & sex_id == 1 & qx_adj < 0.25, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_combine[deaths > 50 & age_group_id == 32 & sex_id == 2 & qx_adj < 0.20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_combine[deaths > 50 & age_group_id == 31 & sex_id == 1 & qx_adj < 0.15, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_combine[deaths > 50 & age_group_id == 31 & sex_id == 2 & qx_adj < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_combine[deaths > 50 & age_group_id == 30 & sex_id == 1 & qx_adj < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_combine[deaths > 50 & age_group_id == 30 & sex_id == 2 & qx_adj < 0.05, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]

# outlier based on improper sex ratio in older age groups
demUtils::assert_is_unique_dt(
  vr_combine,
  id_cols = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "sex_id", "source_type_id", "rank")
)
vr_sex_ratios <- dcast(
  vr_combine,
  nid + underlying_nid + location_id + year_id + age_group_id + source_type_id + rank ~ sex_id,
  value.var = c("mx_adj", "deaths_adj")
)
vr_sex_ratios[, sex_ratio := mx_adj_2 / mx_adj_1]
vr_sex_ratios[deaths_adj_1 > 25 & deaths_adj_2 > 25 & sex_ratio > 1.25 & age_group_id %in% c(20, 30:32, 235), outlier_new := 1]
vr_combine <- merge(
  vr_combine,
  vr_sex_ratios[, -c("mx_adj_1", "mx_adj_2", "deaths_adj_1", "deaths_adj_2", "sex_ratio")],
  by = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "source_type_id", "rank"),
  all.x = TRUE
)
vr_combine[outlier_new == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Improper sex ratio in older ages; "))]
vr_combine[, outlier_new := NULL]

# outlier CHN DSP for 2009:2010 85+
vr_combine[
  location_id == 44533 & year_id %in% 2009:2010 & age_group_id %in% c(31:32, 235) & series_name == "DSP",
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "HOTFIX: see row 807; "))
]

# multiply BGD SRS sample sizes by 10
vr_combine[location_id == 161 & source_type == "SRS", sample_size := sample_size * 10]

# scale down PER's sample size by multiplying by completeness
vr_combine[location_id == 123 & age_start >= 0 & age_end <= 5, sample_size := sample_size * comp_u5]
vr_combine[location_id == 123 & age_start >= 5 & age_end <= 10, sample_size := sample_size * comp_5_9]
vr_combine[location_id == 123 & age_start >= 10 & age_end <= 15, sample_size := sample_size * comp_10_14]
vr_combine[location_id == 123 & age_start >= 15, sample_size := sample_size * comp_adult]

# add more formatting
vr_final <- vr_combine[,
  c("location_id", "year_id", "sex_id", "age_group_id", "age_start", "age_end",
    "nid", "source_type_id", "source_type", "underlying_nid", "title", "underlying_title",
    "series_name", "rank", "sample_size_orig", "sample_size", "population", "deaths",
    "deaths_adj", "mx", "mx_adj", "mx_se", "mx_se_adj", "qx", "qx_adj", "outlier",
    "outlier_note", "onemod_process_type", "extraction_source", "completeness", "complete_vr")
]

# fix outlier_note ending
vr_final[, outlier_note := gsub("; $", "", outlier_note)]
vr_final[outlier_note == "", outlier_note := NA]

# check for missingness
assertable::assert_values(
  vr_final,
  colnames = setdiff(
    colnames(vr_final),
    c("underlying_nid", "underlying_title", "completeness", "complete_vr", "outlier_note")
  ),
  test = "not_na"
)

assertable::assert_values(
  vr_final[!is.na(underlying_nid)],
  colnames = "underlying_title",
  test = "not_na"
)

assertable::assert_values(
  vr_final[outlier == 1],
  colnames = "outlier_note",
  test = "not_na"
)

assertable::assert_values(
  data = vr_final,
  colnames = "sample_size",
  test = "gt",
  test_val = 0,
  warn_only = TRUE
)

# check for duplicates
demUtils::assert_is_unique_dt(
  vr_final,
  id_cols = setdiff(names(vr_final), "source_type_id")
)

# check for duplicate location/year/source type in unoutliered data
demUtils::assert_is_unique_dt(
  vr_final[outlier == 0],
  id_cols = c("location_id", "year_id", "sex_id", "age_group_id", "source_type")
)

vr_final_dupes <- demUtils::identify_non_unique_dt(
  vr_final[outlier == 0],
  id_cols = c("location_id", "year_id", "sex_id", "age_group_id", "source_type")
)

# Save -------------------------------------------------------------------------

# compare mx to previous version
prev_version <- fread(
  fs::path_norm(fs::path("FILEPATH"))
)
mx_comparison <- quick_mx_compare(
  test_file = vr_final,
  compare_file = prev_version,
  id_cols = c(
    "location_id", "nid", "underlying_nid", "year_id", "sex_id", "age_start",
    "age_end", "source_type", "source_type_id", "outlier"
  ),
  warn_only = TRUE
)
mx_comparison[loc_map, ihme_loc_id := i.ihme_loc_id, on = "location_id"]
readr::write_csv(
  mx_comparison,
  fs::path("FILEPATH")
)

readr::write_csv(
  vr_final,
  fs::path("FILEPATH")
)
