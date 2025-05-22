
## Meta ------------------------------------------------------------------------

# Description: Create census & survey dataset with only GBD age groups

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and format census & survey data
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
source(fs::path("FILEPATH"))

# Read in internal inputs ------------------------------------------------------

age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# read in formatted empirical pop
emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# read in prepped DDM completeness estimates
dt_comp_est <- fread(fs::path("FILEPATH"))

# Read in and format census & survey data --------------------------------------

cod_noncod <- fread(fs::path("FILEPATH"))

census_survey <- cod_noncod[
  !source_type %in% c("VR", "SRS", "DSP", "Civil Registration") &
    age_group_id %in% age_map_gbd[, unique(age_group_id)] & sex_id %in% 1:2
]

# subset to GBD locations
census_survey <- census_survey[location_id %in% locs$location_id]

# fix underlying nids
census_survey[underlying_nid == 58750, underlying_nid := NA]
census_survey[nid == 6550 & underlying_nid == 6550, underlying_nid := NA]
census_survey[nid == 140967 & location_id == 205 & year_id == 2021, underlying_nid := 553463]
census_survey[nid == 140967 & location_id == 207 & year_id == 2010, underlying_nid := 218222]
census_survey[nid == 140967 & location_id == 208 & year_id == 2014, underlying_nid := 218331]
census_survey[nid == 140967 & location_id == 182 & year_id == 2018, underlying_nid := 325175]
census_survey[nid == 140967 & location_id == 164 & year_id == 2011, underlying_nid := 153156]
census_survey[nid == 140967 & location_id == 164 & year_id == 2021, underlying_nid := 535050]
census_survey[nid == 140967 & location_id == 152 & year_id == 2016, underlying_nid := 305971]
census_survey[nid == 140967 & location_id == 435 & year_id == 2008, underlying_nid := 24134]
census_survey[nid == 140967 & location_id == 27 & year_id == 2016 & source_type_id == 5, underlying_nid := 325189]
census_survey[nid == 140967 & location_id == 196 & year_id == 2011, underlying_nid := 12146]
census_survey[nid == 140967 & location_id == 198 & year_id == 2022, underlying_nid := 539878]
census_survey[nid == 426273 & location_id == 11 & year_id == 2010, underlying_nid := 91740]

# correct MOZ 2007 nid
census_survey[nid == 8890, nid := 8891]

# merge empirical pop
census_survey <- merge(
  census_survey,
  emp_pop_split_no_vr[, c("year_id", "location_id", "sex_id", "age_group_id", "source_type", "census_pop")],
  by = c("year_id", "location_id", "sex_id", "age_group_id", "source_type"),
  all.x = TRUE
)

# merge population estimates
census_survey <- merge(
  census_survey,
  pop[, c("year_id", "location_id", "sex_id", "age_group_id", "population")],
  by = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

# update source type
census_survey[
  nid %in% c(
    1413, 3120, 7427, 8119, 12683, 32357, 56476, 56554, 56794, 56810, 105633,
    106570, 126913, 134132, 193927
  ),
  ':=' (source_type_id = 5, source_type = "Census")
]
census_survey[
  underlying_nid %in% c(24134, 153156, 218222, 553463, 535050),
  ':=' (source_type_id = 5, source_type = "Census")
]
census_survey[nid %in% c(6550, 6904, 43747), ':=' (source_type_id = 40, source_type = "SUSENAS")]

# add official titles
census_survey <- demInternal::merge_ghdx_record_fields(census_survey, nid_col = "underlying_nid")
setnames(census_survey, "title", "underlying_title")
census_survey <- demInternal::merge_ghdx_record_fields(census_survey)

census_survey[!is.na(census_pop), has_census_pop := TRUE]
investigate_vr_surveys <- unique(census_survey[, c("nid", "underlying_nid", "title", "underlying_title", "source_type", "has_census_pop")])
investigate_vr_surveys <- investigate_vr_surveys[order(source_type, title, underlying_title)]
readr::write_csv(
  investigate_vr_surveys,
  fs::path("FILEPATH")
)
census_survey[,
  ':=' (title = NULL, underlying_title = NULL, status = NULL, has_census_pop = NULL)
]

census_survey[!is.na(census_pop), population := census_pop]
census_survey[, census_pop := NULL]

# calculate mx
census_survey[, mx := deaths / population]

# calculate mx_se
census_survey[, mx_se := sqrt((ifelse(mx >= 0.99, 0.99, mx) * (1 - ifelse(mx >= 0.99, 0.99, mx))) / population)]

# calculate qx
census_survey[, qx := demCore::mx_to_qx(mx, age_end - age_start)]

# add outliers
census_survey[, ':=' (outlier = 0, outlier_note = "")]
census_survey <- manual_outliering_vr_surveys(census_survey)

# add in Bihar ENHANCE
bihar_survey <- fread("FILEPATH")

bihar_survey[, age_type := "gbd_age_group"]
bihar_survey[, extraction_source := "noncod"]
bihar_survey[, source := "ENHANCE"]
bihar_survey[, source_type := "Survey"]
bihar_survey[, source_type_id := 58]
bihar_survey[, underlying_nid := NA]
bihar_survey[, year_id := 2020]

setnames(bihar_survey, "births", "population")

bihar_survey <- bihar_survey[, !c("date_start", "date_end", "title")]

bihar_survey <- merge(
  bihar_survey[age_group_id != 42],
  age_map_gbd,
  by = "age_group_id",
  all.x = TRUE
)

bihar_survey[, mx := demCore::qx_to_mx(qx, age_length = age_end - age_start)]
bihar_survey[, mx_se := sqrt((ifelse(mx >= 0.99, 0.99, mx) * (1 - ifelse(mx >= 0.99, 0.99, mx))) / population)]

bihar_survey[, `:=` (outlier = 1, outlier_note = "Manually outliered; ")]

census_survey <- rbind(census_survey, bihar_survey)

# drop WSM survey (duplicate)
census_survey <- census_survey[!(location_id == 27 & year_id == 2016 & source_type == "Survey")]


# outlier based on drops in mx causing bad age distribution
country_pop <- unique(pop[age_group_id == 22 & sex_id == 3, c("location_id", "year_id", "population")])
setnames(country_pop, "population", "country_pop")
census_survey <- merge(
  census_survey,
  country_pop,
  by = c("location_id", "year_id"),
  all.x = TRUE
)
age_group_id_order <- c(2, 3, 388, 389, 238, 34, 6:20, 30:32, 235)
census_survey[, age_group_id := factor(age_group_id, levels = age_group_id_order)]
census_survey <- census_survey[order(location_id, year_id, sex_id, age_group_id)]
census_survey[outlier == 0, mx_diff_prev := mx - shift(mx), by = c("location_id", "year_id", "sex_id")]
census_survey[mx_diff_prev < 0 & age_group_id %in% c(20, 30:32, 235), bad_age_dist := 1]
census_survey <- census_survey %>%
  group_by(location_id, year_id, sex_id) %>%
  tidyr::fill(bad_age_dist, .direction = "down") %>%
  dplyr::ungroup() %>% setDT
census_survey[bad_age_dist == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Bad old age distribution; "))]
census_survey[,
  ':=' (mx_diff_prev = NULL, bad_age_dist = NULL, country_pop = NULL, age_group_id = as.numeric(as.character(age_group_id)))
]

# outlier mx >= 1
census_survey[mx >= 1 & !age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Mx >= 1; "))]

# outlier based on qx being too low in old ages (80-90)
census_survey[deaths > 50 & age_group_id == 32 & sex_id == 1 & qx < 0.25, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
census_survey[deaths > 50 & age_group_id == 32 & sex_id == 2 & qx < 0.20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
census_survey[deaths > 50 & age_group_id == 31 & sex_id == 1 & qx < 0.15, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
census_survey[deaths > 50 & age_group_id == 31 & sex_id == 2 & qx < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
census_survey[deaths > 50 & age_group_id == 30 & sex_id == 1 & qx < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
census_survey[deaths > 50 & age_group_id == 30 & sex_id == 2 & qx < 0.05, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]

# outlier based on improper sex ratio in older age groups
demUtils::assert_is_unique_dt(
  census_survey,
  id_cols = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "sex_id", "source_type_id")
)
vr_survey_sex_ratios <- dcast(
  census_survey,
  nid + underlying_nid + location_id + year_id + age_group_id + source_type_id ~ sex_id,
  value.var = c("mx", "deaths")
)
vr_survey_sex_ratios[, sex_ratio := mx_2 / mx_1]
vr_survey_sex_ratios[deaths_1 > 25 & deaths_2 > 25 & sex_ratio > 1.25 & age_group_id %in% c(20, 30:32, 235), outlier_new := 1]
census_survey <- merge(
  census_survey,
  vr_survey_sex_ratios[, -c("mx_1", "mx_2", "deaths_1", "deaths_2", "sex_ratio")],
  by = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "source_type_id"),
  all.x = TRUE
)
census_survey[outlier_new == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Improper sex ratio in older ages; "))]
census_survey[, outlier_new := NULL]

# manual outliers for handoff 1
census_survey[
  location_id == 44533 & nid == 65578 & age_group_id == 6,
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
] # CHN 2003 SSPC 5-9
census_survey[
  location_id %in% locs[level >= 3 & region_name %like% "Sub-Saharan Africa"]$location_id,
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
] # All SSA

# format
setnames(
  census_survey,
  c("location_id", "year_id", "population"),
  c("location", "date_start", "sample_size")
)
census_survey[, date_end := date_start]

# fix outlier_note ending
census_survey[, outlier_note := gsub("; $", "", outlier_note)]
census_survey[outlier_note == "", outlier_note := NA]

assertable::assert_values(
  census_survey[, -c("underlying_nid", "outlier_note")],
  colnames = colnames(census_survey),
  test = "not_na"
)

# check for duplicates
demUtils::assert_is_unique_dt(census_survey, names(census_survey))

# Save -------------------------------------------------------------------------

# compare mx to previous version
prev_version <-  fread(
  fs::path_norm(
    fs::path(
      "FILEPATH"
    )
  )
)

mx_comparison <- quick_mx_compare(
  test_file = census_survey,
  compare_file = prev_version,
  id_cols = c(
    "location", "nid", "underlying_nid", "sex_id", "date_start", "age_start",
    "age_end", "source_type", "source_type_id", "outlier", "extraction_source"
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
  census_survey,
  fs::path("FILEPATH")
)
