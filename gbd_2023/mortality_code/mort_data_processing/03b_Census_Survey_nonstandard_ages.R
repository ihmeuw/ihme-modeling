
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
pop_sy <- fread(fs::path("FILEPATH"))

# read in formatted empirical pop
emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# read in prepped DDM completeness estimates
dt_comp_est <- fread(fs::path("FILEPATH"))

# Read in and format census & survey data --------------------------------------

cod_noncod <- fread(fs::path("FILEPATH"))

vr_survey_ns_ages <- cod_noncod[
  !source_type %in% c("VR", "SRS", "DSP", "Civil Registration") &
    !age_group_id %in% age_map_gbd[, unique(age_group_id)]
]

if (nrow(vr_survey_ns_ages[sex_id == 3]) > 0) stop("Check for sex id 3 in vr surveys")
if (nrow(vr_survey_ns_ages[age_group_id == 22]) > 0) stop("Check for all ages group in vr surveys")

# subset to GBD locations
vr_survey_ns_ages <- vr_survey_ns_ages[location_id %in% locs$location_id]

# fix underlying nids
vr_survey_ns_ages[underlying_nid == 58750, underlying_nid := NA]
vr_survey_ns_ages[nid == 6550 & underlying_nid == 6550, underlying_nid := NA]
vr_survey_ns_ages[nid == 140967 & location_id == 205 & year_id == 2021, underlying_nid := 553463]
vr_survey_ns_ages[nid == 140967 & location_id == 207 & year_id == 2010, underlying_nid := 218222]
vr_survey_ns_ages[nid == 140967 & location_id == 208 & year_id == 2014, underlying_nid := 218331]
vr_survey_ns_ages[nid == 140967 & location_id == 182 & year_id == 2018, underlying_nid := 325175]
vr_survey_ns_ages[nid == 140967 & location_id == 164 & year_id == 2011, underlying_nid := 153156]
vr_survey_ns_ages[nid == 140967 & location_id == 164 & year_id == 2021, underlying_nid := 535050]
vr_survey_ns_ages[nid == 140967 & location_id == 152 & year_id == 2016, underlying_nid := 305971]
vr_survey_ns_ages[nid == 140967 & location_id == 435 & year_id == 2008, underlying_nid := 24134]
vr_survey_ns_ages[nid == 140967 & location_id == 27 & year_id == 2016 & source_type_id == 5, underlying_nid := 325189]
vr_survey_ns_ages[nid == 140967 & location_id == 196 & year_id == 2011, underlying_nid := 12146]
vr_survey_ns_ages[nid == 140967 & location_id == 198 & year_id == 2022, underlying_nid := 539878]
vr_survey_ns_ages[nid == 426273 & location_id == 11 & year_id == 2010, underlying_nid := 91740]

# correct MOZ 2007 nid
vr_survey_ns_ages[nid == 8890, nid := 8891]

# remove overlapping age groups
vr_survey_ns_ages <- vr_survey_ns_ages[!(underlying_nid == 153004 & age_start == 75 & age_end == 125)]

# merge empirical pop
vr_survey_ns_ages <- merge(
  vr_survey_ns_ages,
  emp_pop_split_no_vr[, c("year_id", "location_id", "sex_id", "age_group_id", "source_type", "census_pop")],
  by = c("year_id", "location_id", "sex_id", "age_group_id", "source_type"),
  all.x = TRUE
)

# merge population estimates
vr_survey_ns_ages <- merge(
  vr_survey_ns_ages,
  pop[, c("year_id", "location_id", "sex_id", "age_group_id", "population")],
  by = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

# create missing population age groups
missing_age_groups <- vr_survey_ns_ages[is.na(population), unique(age_group_id)]
agg_population <- data.table()
for (age in missing_age_groups) {
  message(paste0("Creating population for: ", age))
  missing_age_start <- age_map_extended[age_group_id == age, age_start]
  missing_age_end <- age_map_extended[age_group_id == age, age_end]
  if (missing_age_end - missing_age_start < 1) stop("fix pop agg: cannot be aggregated by single year pop")
  if (missing_age_start > 95) stop("fix pop agg: terminal age group issue")
  if (missing_age_end != 125){
    pop_sy_sub <- pop_sy[
      age_group_years_start >= missing_age_start & age_group_years_end <= missing_age_end & age_length == 1
    ]
  } else {
    pop_sy_sub <- pop_sy[
      (age_group_years_start >= missing_age_start & age_group_years_end <= missing_age_end & age_length == 1) | age_group_id == 235
    ]
  }
  pop_sy_sub <- pop_sy_sub[, .(agg_population = sum(population)), by = c("year_id", "location_id", "sex_id")]
  pop_sy_sub[, age_group_id := age]
  agg_population <- rbind(agg_population, pop_sy_sub)
}

# use agg pop for missing population
vr_survey_ns_ages <- merge(
  vr_survey_ns_ages,
  agg_population[, c("year_id", "location_id", "sex_id", "age_group_id", "agg_population")],
  by = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)
vr_survey_ns_ages[is.na(population) & !is.na(agg_population), population := agg_population]
assertable::assert_values(vr_survey_ns_ages, "population", test = "not_na")
vr_survey_ns_ages[, agg_population := NULL]

# update source type
vr_survey_ns_ages[
  nid %in% c(1413, 3120, 7427, 8119, 12683, 32357, 56476, 56554, 56794, 56810,
             105633, 106570, 126913, 134132, 193927),
  ':=' (source_type_id = 5, source_type = "Census")
]
vr_survey_ns_ages[underlying_nid %in% c(24134, 153156, 218222, 553463, 535050), ':=' (source_type_id = 5, source_type = "Census")]
vr_survey_ns_ages[nid %in% c(6550, 6904, 43747), ':=' (source_type_id = 40, source_type = "SUSENAS")]

# add official titles
vr_survey_ns_ages <- demInternal::merge_ghdx_record_fields(vr_survey_ns_ages, nid_col = "underlying_nid")
setnames(vr_survey_ns_ages, "title", "underlying_title")
vr_survey_ns_ages <- demInternal::merge_ghdx_record_fields(vr_survey_ns_ages)

vr_survey_ns_ages[!is.na(census_pop), has_census_pop := TRUE]
missing_underlying_nid <- vr_survey_ns_ages[(nid == 140967 & is.na(underlying_nid)) | (nid == 426273 & is.na(underlying_nid))]
missing_underlying_nid <- unique(
  missing_underlying_nid[, c("nid", "underlying_nid", "title", "underlying_title", "location_id", "year_id", "source_type", "has_census_pop")]
)
missing_underlying_nid[loc_map, ihme_loc_id := i.ihme_loc_id, on = "location_id"]
missing_underlying_nid <- missing_underlying_nid[order(source_type, title, underlying_title)]
readr::write_csv(
  missing_underlying_nid,
  fs::path("FILEPATH")
)
investigate_vr_surveys <- unique(
  vr_survey_ns_ages[, c("nid", "underlying_nid", "title", "underlying_title", "source_type", "has_census_pop")]
)
investigate_vr_surveys <- investigate_vr_surveys[order(source_type, title, underlying_title)]
readr::write_csv(
  investigate_vr_surveys,
  fs::path("FILEPATH")
)
vr_survey_ns_ages[, ':=' (title = NULL, underlying_title = NULL, has_census_pop = NULL)]

vr_survey_ns_ages[!is.na(census_pop), population := census_pop]
vr_survey_ns_ages[, census_pop := NULL]

# calculate mx
vr_survey_ns_ages[, ':=' (mx = deaths / population)]

# calculate mx_se
vr_survey_ns_ages[, ':=' (mx_se = sqrt((ifelse(mx >= 0.99, 0.99, mx) * (1 - ifelse(mx >= 0.99, 0.99, mx))) / population))]

# calculate qx
vr_survey_ns_ages[, qx := demCore::mx_to_qx(mx, age_end - age_start)]

# add outliers
vr_survey_ns_ages[, ':=' (outlier = 0, outlier_note = "")]
vr_survey_ns_ages <- manual_outliering_vr_surveys(vr_survey_ns_ages)
vr_survey_ns_ages[deaths == 0 & population == 0, ':=' (mx = 0, mx_se = 0, qx = 0, outlier = 1, outlier_note = paste0(outlier_note, "No deaths or pop in group; "))]

# only outlier ZWE HHD for handoff 1
vr_survey_ns_ages[location_id == 198 & nid == 21163, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))]

# drop WSM survey (duplicate)
vr_survey_ns_ages <- vr_survey_ns_ages[!(location_id == 27 & year_id == 2016 & source_type == "Survey")]

# outlier mx >= 1
vr_survey_ns_ages[mx >= 1 & !age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Mx >= 1; "))]

# outlier based on qx being too low in old ages (80-90)
vr_survey_ns_ages[deaths > 50 & age_group_id == 32 & sex_id == 1 & qx < 0.25, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_survey_ns_ages[deaths > 50 & age_group_id == 32 & sex_id == 2 & qx < 0.20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_survey_ns_ages[deaths > 50 & age_group_id == 31 & sex_id == 1 & qx < 0.15, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_survey_ns_ages[deaths > 50 & age_group_id == 31 & sex_id == 2 & qx < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_survey_ns_ages[deaths > 50 & age_group_id == 30 & sex_id == 1 & qx < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_survey_ns_ages[deaths > 50 & age_group_id == 30 & sex_id == 2 & qx < 0.05, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]

# format
setnames(vr_survey_ns_ages, "population", "sample_size")
vr_survey_ns_ages[, source_type_name := ifelse(source_type == "Census", "Census", "Survey")]
vr_survey_ns_ages <- vr_survey_ns_ages[, !c("source", "age_start", "age_end", "age_type", "qx")]

# fix outlier_note ending
vr_survey_ns_ages[, outlier_note := gsub("; $", "", outlier_note)]
vr_survey_ns_ages[outlier_note == "", outlier_note := NA]

# add IND sci lit
ind_sci_lit <- fread("FILEPATH")

ind_sci_lit[, mx := mx / 1000]

ind_sci_lit[, extraction_source := "noncod"]
ind_sci_lit[, outlier := 0]
ind_sci_lit[, outlier_note := NA]
ind_sci_lit[, source_type := "Sci Lit"]
ind_sci_lit[, source_type_name := "Survey"]
ind_sci_lit[, source_type_id := 58]
ind_sci_lit[, underlying_nid := NA]
ind_sci_lit[, year_id := year_start]

ind_sci_lit <- merge(
  ind_sci_lit[, !c("location_name", "year_start", "year_end", "mx_lower", "mx_upper")],
  pop[, c("year_id", "location_id", "sex_id", "age_group_id", "population")],
  by = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)
ind_sci_lit[, sample_size := ifelse(is.na(sample_size), population * 0.079, sample_size)] # % of 0-4 coverage
ind_sci_lit[, population := NULL]

ind_sci_lit[is.na(deaths), deaths := sample_size * mx]
ind_sci_lit[, mx_se := sqrt((ifelse(mx >= 0.99, 0.99, mx) * (1 - ifelse(mx >= 0.99, 0.99, mx))) / sample_size)]

vr_survey_ns_ages <- rbind(vr_survey_ns_ages, ind_sci_lit)

# final checks
assertable::assert_values(
  vr_survey_ns_ages[, -c("underlying_nid", "outlier_note")],
  colnames = colnames(vr_survey_ns_ages),
  test = "not_na"
)

demUtils::assert_is_unique_dt(vr_survey_ns_ages, names(vr_survey_ns_ages))

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
  test_file = vr_survey_ns_ages,
  compare_file = prev_version,
  id_cols = c(
    "location_id", "nid", "underlying_nid", "sex_id", "age_group_id", "year_id",
    "source_type", "source_type_id", "outlier", "extraction_source"
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
  vr_survey_ns_ages,
  fs::path("FILEPATH")
)
