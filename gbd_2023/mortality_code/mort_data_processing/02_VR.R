
## Meta ------------------------------------------------------------------------

# Description: Create VR datasets for only GBD age groups and for non-standard age groups

# Steps:
#   1. Load libraries
#   2. Command line arguments
#   3. Setup
#   4. Read in internal inputs
#   5. Read in low granularity data
#   5. Read in VRP intermediates and format
#   8. Save

# Mortality Inputs:
#   * "FILEPATH"
#   * "FILEPATH"

# Outputs:
#   1. "FILEPATH"
#   2. "FILEPATH"

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(demCore)
library(demInternal)
library(haven)
library(hierarchyUtils)
library(mortdb)
library(reticulate)
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

# subnational nid map
location_agg_nid_map <- fread(
  fs::path("FILEPATH")
)[, -"V1"]

# read in prepped DDM completeness estimates
dt_comp_est <- fread(fs::path("FILEPATH"))

# read in prepped DDM corrrection factor
ddm_correction_factors <- fread(
  fs::path(
    "FILEPATH"
  )
)

# early srs correction
srs_pop_scalars <- ddm_correction_factors[source_type %in% c("SRS", "SRS_LIBRARY") & ihme_loc_id %like% "IND"]
srs_pop_scalars <- srs_pop_scalars[year >= 1993, .(srs_scale = mean(correction_factor)), by = c("ihme_loc_id", "sex")]
setnames(srs_pop_scalars, "sex", "sex_id")
srs_pop_scalars <- srs_pop_scalars %>%
  mutate(sex_id = case_when(
    sex_id == "male" ~ 1,
    sex_id == "female" ~ 2,
    sex_id == "both" ~ 3,
    TRUE ~ as.numeric(sex_id)
  )
)

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))
pop_sy <- fread(fs::path("FILEPATH"))

# read in formatted empirical pop
emp_pop_split_no_vr <- fread(fs::path("FILEPATH"))

# read in formatted srs pop
srs_pop <- fread(fs::path("FILEPATH"))

# read in scalars
scalars <- fread(fs::path("FILEPATH"))

# read in reference 5q0
reference_5q0 <- fread(fs::path("FILEPATH"))
reference_5q0 <- reference_5q0[source_name == "VR"]
reference_5q0[, loc_yr := paste(location_id, year_id, sep = "_")]

# read in ssa inclusion sheet
ssa_data_inclusion <- fread(fs::path("FILEPATH"))
ssa_data_inclusion <- ssa_data_inclusion[
  ,
  .(location_id, nid, underlying_nid, source_type = source_type_name, updated_outlier, updated_notes)
]

# Read in low granularity data -------------------------------------------------

# Create a data frame from the subfolders
subfolders <- list.dirs(path = low_gran_dir, full.names = FALSE, recursive = FALSE)
subfolders_df <- data.frame(folder = subfolders) |>
  # Filter out folders that don't start with "GBD" and whose year is <= gbd_year
  filter(grepl("^GBD[0-9]{4}$", folder)) |>
  mutate(year = as.numeric(substr(folder, start = 4, stop = 7))) |>
  filter(year <= gbd_year) |>
  mutate(full_path = paste0(low_gran_dir, "/", folder))

files_list <- subfolders_df$full_path |>
  map(~list.files(path = .x, full.names = TRUE)) |>
  unlist()

low_gran <- assertable::import_files(
  filenames = files_list
)

low_gran[, ':=' (age_start = round(age_start, 7), age_end = round(age_end, 7))]
low_gran <- merge(
  age_map_extended[!age_group_id == 27, c("age_start", "age_end", "age_group_id")],
  low_gran,
  by = c("age_start", "age_end"),
  all.y = TRUE
)
low_gran[,
  ':=' (source_type_id = 1, source_type = "VR", outlier = 0, underlying_nid = NA,
        underlying_title = NA, rank = 4, ihme_loc_id = NULL, series_name = "low_gran",
        source = NULL, extraction_source = "noncod", outlier_note = "")
]
low_gran[, age_type := ifelse(age_group_id %in% age_map_gbd$age_group_id, "gbd_age_group", "too_wide")]
low_gran <- demInternal::merge_ghdx_record_fields(low_gran)
low_gran[sex_id == 9, sex_id := 4]

# Read in VRP intermediates and format -----------------------------------------

cod_noncod <- fread(fs::path("FILEPATH"))

vr_raw <- cod_noncod[source_type %in% c("VR", "SRS", "DSP", "Civil Registration", "MCCD")]

# Data Adjustments

# Adjust CYP, MDA, & SRB for non-representativeness (from VRP)
scalars <- fread("FILEPATH")
scalars <- scalars[, .(scalar = mean(scalar)), by = "parent_ihme_loc_id"]
scalars <- merge(
  locs[, c("location_id", "ihme_loc_id")],
  scalars,
  by.x = "ihme_loc_id",
  by.y = "parent_ihme_loc_id",
  all.y = TRUE
)
scalars[, ihme_loc_id := NULL]

vr_raw <- merge(
  vr_raw,
  scalars,
  by = "location_id",
  all.x = TRUE
)
# all data for CYP, MDA, & SRB is source_type_id 1 in this dataset
vr_raw[(!is.na(scalar) & source_type_id == 1 & (!location_id == 61 | (location_id == 61 & year_id >= 1997))), deaths := deaths * scalar]
vr_raw[, scalar := NULL]

# Add implied 0s to VR in NVSS
vr_nvss <- vr_raw[source %like% "NVSS|US_NCHS" & outlier == 0]
vr_nvss[, nid_source := paste0(nid, "_", extraction_source)]
nvss_locs <- c(523:573)
vr_nvss_additions <- data.table()
for (yr in unique(vr_nvss$year_id)) {
  message(paste0("Working on: ", yr))
  # make a data table of all expected combinations of ids
  for (nn in unique(vr_nvss[year_id == yr & location_id %in% nvss_locs, nid_source])) {
    id_set <- CJ(
      location_id = nvss_locs,
      sex_id = 1:2,
      year_id = yr,
      age_group_id = unique(vr_nvss[nid_source == nn & year_id == yr & location_id %in% nvss_locs, age_group_id]),
      source_type_id = unique(vr_nvss[nid_source == nn & year_id == yr & location_id %in% nvss_locs, source_type_id]),
      source_type = unique(vr_nvss[nid_source == nn & year_id == yr & location_id %in% nvss_locs, source_type]),
      source = unique(vr_nvss[nid_source == nn & year_id == yr & location_id %in% nvss_locs, source]),
      extraction_source = unique(vr_nvss[nid_source == nn & year_id == yr & location_id %in% nvss_locs, extraction_source]),
      nid = unique(vr_nvss[nid_source == nn & year_id == yr & location_id %in% nvss_locs, nid]),
      outlier = 0,
      outlier_note = "",
      deaths = 0
    )
    id_set[age_map_extended, ':=' (age_start = i.age_start, age_end = i.age_end), on = "age_group_id"]
    id_set <- anti_join(id_set, vr_nvss, by = c("location_id", "year_id", "age_group_id", "sex_id", "outlier", "nid", "extraction_source"))
    id_set[, age_type := ifelse(age_group_id %in% age_map_gbd$age_group_id, "gbd_age_group", "too_wide")]

    vr_nvss_additions <- rbind(vr_nvss_additions, id_set, fill = TRUE)
  }
}
vr_raw <- rbind(vr_raw, vr_nvss_additions, fill = TRUE)

# Add implied 0s to VR in NZL/JPN subnats
vr_other_additions <- data.table()
for (iso3 in c("NZL", "JPN")) {
  message(paste0("Working on: ", iso3))
  loc_id <- loc_map[ihme_loc_id == iso3, location_id]
  subnats <- loc_map[parent_id == loc_id, location_id]

  vr_only <- vr_raw[location_id %in% subnats & source_type == "VR" & outlier == 0]
  for (yr in unique(vr_only$year_id)) {
    # make a data table of all expected combinations of ids
    for (nn in unique(vr_only[year_id == yr & location_id %in% subnats, nid])) {
      id_set <- CJ(
        location_id = subnats,
        sex_id = 1:2,
        year_id = yr,
        age_group_id = unique(vr_only[nid == nn & year_id == yr & location_id %in% subnats, age_group_id]),
        source_type_id = unique(vr_only[nid == nn & year_id == yr & location_id %in% subnats, source_type_id]),
        source_type = unique(vr_only[nid == nn & year_id == yr & location_id %in% subnats, source_type]),
        source = unique(vr_only[nid == nn & year_id == yr & location_id %in% subnats, source]),
        extraction_source = unique(vr_only[nid == nn & year_id == yr & location_id %in% subnats, extraction_source]),
        nid = nn,
        outlier = 0,
        outlier_note = "",
        deaths = 0
      )
      id_set[age_map_extended, ':=' (age_start = i.age_start, age_end = i.age_end), on = "age_group_id"]
      id_set <- anti_join(id_set, vr_only, by = c("location_id", "year_id", "age_group_id", "sex_id", "outlier"))
      id_set[, age_type := ifelse(age_group_id %in% age_map_gbd$age_group_id, "gbd_age_group", "too_wide")]

      vr_other_additions <- rbind(vr_other_additions, id_set, fill = TRUE)
    }
  }
}

vr_other_additions <- vr_other_additions[!source == "RUS_44951_VR_1939-2016"]
vr_raw <- rbind(vr_raw, vr_other_additions, fill = TRUE)

agg_subnationals <- function(dt, iso3) {

  message(paste0("Aggregating: ", iso3))

  loc_id <- locs[ihme_loc_id == iso3, location_id]
  subnats <- locs[parent_id == loc_id, location_id]
  subnat_map <- data.table(parent = loc_id, child = subnats)

  dt <- dt[location_id %in% subnats & outlier == 0]
  dt[, added := NULL]
  demUtils::assert_is_unique_dt(dt, setdiff(names(dt), "deaths"))

  # merge on nid map for nationals that combine multiple sources
  dt <- merge(
    dt,
    location_agg_nid_map[ihme_loc_id == iso3, -c("ihme_loc_id")],
    by.x = c("nid", "year_id"),
    by.y = c("subnational_nid", "year_id"),
    all.x = TRUE
  )

  if (iso3 == "GBR") {
    dt <- dt[!(nid %in% c(494069, 511431, 553081) & location_id == 4749)]
    dt[nid %in% c(488437, 504636) & extraction_source == "noncod", ':=' (country_nid = NA, country_source = NA)]
    dt[nid == 542541 & location_id == 4636 & extraction_source == "noncod", ':=' (country_nid = NA, country_source = NA)]
  }
  if (iso3 == "GBR") {
    dt[!is.na(country_nid) & year_id >= 2020, extraction_source := "cod"]
  }
  if (iso3 == "UKR") {
    dt[!is.na(country_nid) & year_id >= 2017, extraction_source := "cod"]
  }
  dt[!is.na(country_nid), ':=' (nid = country_nid, source = country_source, underlying_nid = NA)]
  dt[, ':=' (country_nid = NULL, country_source = NULL)]

  subnat_aggs <- hierarchyUtils::agg(
    dt = dt,
    id_cols = setdiff(names(dt), "deaths"),
    col_stem = "location_id",
    col_type = "categorical",
    value_cols = "deaths",
    mapping = subnat_map,
    missing_dt_severity = "warning"
  )

  return(subnat_aggs)

}

# aggregate level 5 GBR & IND (no KEN data to aggregate)
subnats_to_agg <- c("GBR_4749", locs[ihme_loc_id %like% "IND" & level == 4, ihme_loc_id])

new_subnats <- rbindlist(
  lapply(
    subnats_to_agg,
    agg_subnationals,
    dt = vr_raw
  )
)
new_subnats[, location_agg := TRUE]
vr_raw <- rbind(vr_raw, new_subnats, fill = TRUE)
# use aggregated values if there are duplicates
vr_raw[,
  dup := .N,
  by = setdiff(names(vr_raw), c("location_agg", "source", "underlying_nid", "deaths"))
]
vr_raw <- vr_raw[dup == 1 | (dup == 2 & !is.na(location_agg))]
vr_raw[, c("dup", "location_agg") := NULL]

# aggregate AZE 0-1 and 1-5 for cleaner splits
vr_aze_u5 <- vr_raw[location_id == 34 & age_group_id %in% c(5, 28)]
vr_aze_u5 <- vr_aze_u5[, .(deaths = sum(deaths)), by = setdiff(names(vr_aze_u5), c("deaths", "age_group_id", "age_start", "age_end"))]
vr_aze_u5[, ':=' (age_group_id = 1, age_start = 0, age_end = 5)]

vr_raw <- rbind(vr_raw[!(location_id == 34 & age_group_id %in% c(5, 28))], vr_aze_u5)

# aggregate crimea and sevastopol so that we get UKR 2021
vr_ukr_u5 <- vr_raw[location_id %in% c(44934, 44939) & year_id == 2021 & age_group_id %in% c(5, 28)]
vr_ukr_u5 <- vr_ukr_u5[, .(deaths = sum(deaths)), by = setdiff(names(vr_ukr_u5), c("deaths", "age_group_id", "age_start", "age_end"))]
vr_ukr_u5[, ':=' (age_group_id = 1, age_start = 0, age_end = 5)]

vr_ukr_old <- vr_raw[location_id %in% c(44934, 44939) & year_id == 2021 & age_group_id %in% c(30, 160)]
vr_ukr_old <- vr_ukr_old[, .(deaths = sum(deaths)), by = setdiff(names(vr_ukr_old), c("deaths", "age_group_id", "age_start", "age_end", "age_type"))]
vr_ukr_old[, ':=' (age_group_id = 21, age_start = 80, age_end = 125, age_type = "too_wide")]

vr_raw <- rbind(vr_raw, vr_ukr_u5, vr_ukr_old)

# aggregate GBR countries so that we get U5 and 90+ for all available years
vr_gbr_u1 <- vr_raw[
  location_id %in% c(433, 434, 4636, 4749) & age_group_id %in% c(2, 3, 4) &
    !(location_id == 433 & year_id %in% 1968:1977 & nid == 18447)
]
vr_gbr_u1 <- vr_gbr_u1[, .(deaths = sum(deaths)), by = setdiff(names(vr_gbr_u1), c("deaths", "age_group_id", "age_start", "age_end", "age_type"))]
vr_gbr_u1[, ':=' (age_group_id = 28, age_start = 0, age_end = 1, age_type = "too_wide")]
vr_gbr_u1[, added := TRUE]

vr_gbr_1to4 <- vr_raw[
  location_id %in% c(433, 434) & age_group_id %in% c(34, 238) &
    !(location_id == 433 & year_id %in% c(1950:1978, 1980:2017) & extraction_source == "noncod") &
    !(location_id == 434 & year_id %in% c(1950:1978, 1980:2018) & extraction_source == "noncod")
]
vr_gbr_1to4 <- vr_gbr_1to4[, .(deaths = sum(deaths)), by = setdiff(names(vr_gbr_1to4), c("deaths", "age_group_id", "age_start", "age_end", "age_type"))]
vr_gbr_1to4[, ':=' (age_group_id = 5, age_start = 1, age_end = 5, age_type = "too_wide")]
vr_gbr_1to4[, added := TRUE]

vr_gbr_old <- vr_raw[location_id %in% c(433, 434, 4636, 4749) & age_group_id %in% c(32, 235)]
vr_gbr_old <- vr_gbr_old[, .(deaths = sum(deaths)), by = setdiff(names(vr_gbr_old), c("deaths", "age_group_id", "age_start", "age_end", "age_type"))]
vr_gbr_old[, ':=' (age_group_id = 237, age_start = 90, age_end = 125, age_type = "too_wide")]
vr_gbr_old[, added := TRUE]

vr_raw <- rbind(vr_raw, vr_gbr_u1, vr_gbr_1to4, vr_gbr_old, fill = TRUE)

nats_to_agg <- unique(substr(locs[ihme_loc_id %like% "_" & parent_id %in% unique(vr_raw$location_id), unique(ihme_loc_id)], 1, 3))
nats_to_agg <- c(nats_to_agg[nats_to_agg != "CHN"], "CHN_44533")

new_nats <- rbindlist(
  lapply(
    nats_to_agg,
    agg_subnationals,
    dt = vr_raw
  ),
  use.names = TRUE
)
new_nats[, location_agg := TRUE]
# Remove DSP aggregation
new_nats <- new_nats[!(source_type == "DSP")]
new_nats <- new_nats[!(location_id == 95 & age_group_id == 28 & year_id %in% c(2014, 2017:2019))]
vr_raw <- rbind(vr_raw, new_nats, fill = TRUE)
# Use aggregated value if a source already has national value
vr_raw[,
  dup := .N,
  by = setdiff(names(vr_raw), c("location_agg", "deaths"))
]
vr_raw <- vr_raw[dup == 1 | (dup == 2 & !is.na(location_agg))]
vr_raw <- vr_raw[is.na(added)]
vr_raw[, c("dup", "location_agg", "source", "added") := NULL]

# remove age groups added above
vr_raw <- vr_raw[!(location_id %in% c(44934, 44939) & year_id == 2021 & age_group_id %in% c(1, 21))]

# replace bad underlying_nid
vr_raw[underlying_nid == 346543, underlying_nid := 236744] # DOM 2002 VR from WHO

# add official titles
vr_raw <- demInternal::merge_ghdx_record_fields(vr_raw, nid_col = "underlying_nid")
setnames(vr_raw, "title", "underlying_title")
vr_raw <- demInternal::merge_ghdx_record_fields(vr_raw)

# add series name for de-duplication and plotting - separates VR into a few more sources than source_type
vr_raw[title %like% "WHO", series_name := "WHO"]
vr_raw[title %like% "United Nations Demographic Yearbook", series_name := "DYB"]
vr_raw[source_type == "DSP", series_name := "DSP"]
vr_raw[title %like% "Human Mortality Database", series_name := "HMD"]
vr_raw[source_type == "SRS", series_name := "SRS"]
vr_raw[source_type_id == 55, series_name := "MCCD"]
vr_raw[
  title %like% "Vital Statistics|Vital Registration|NVSS|Death Registry|Death Register|Death Registration|Civil Registration|Mortality Statistics|VR|Mortality Registration" &
    is.na(series_name),
  series_name := "CRVS"
]
vr_raw[is.na(series_name), series_name := "Custom"]
vr_raw[, series_name := factor(series_name, levels = c("MCCD", "low_gran", "DYB", "HMD", "Custom", "CRVS", "DSP", "SRS", "WHO"))]
# add version to WHO
vr_raw[series_name == "WHO", version_date := gsub("WHO Mortality Database Version ", "", title)]
vr_raw[!is.na(version_date), version_date := gsub(" ", " 1, ", version_date)]
vr_raw[, version_date := as.Date(version_date, format = '%B %d, %Y')]
# rank and outlier
vr_raw <- vr_raw[order(location_id, year_id, age_group_id, sex_id, -series_name, -version_date, extraction_source, -age_type)]
vr_raw[, rank := seq(.N), by = .(location_id, year_id, age_group_id, sex_id)]
vr_raw[outlier == 0 & rank != 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Duplicate; "))]
vr_raw[, version_date := NULL]

# Outlier if >50% higher than previous year
vr_raw <- vr_raw[order(location_id, age_group_id, sex_id, year_id)]
vr_raw[
  outlier == 0,
  diff_prev := deaths - shift(deaths),
  by = c("location_id", "sex_id", "age_group_id")
]
vr_raw[
  outlier == 0,
  perc_diff_prev := ((deaths - shift(deaths)) / shift(deaths)) * 100,
  by = c("location_id", "sex_id", "age_group_id")
]
vr_raw[
  outlier == 0,
  diff_next := shift(deaths, type = "lead") - deaths,
  by = c("location_id", "sex_id", "age_group_id")
]
vr_raw[
  outlier == 0,
  perc_diff_next := ((shift(deaths, type = "lead") - deaths) / deaths) * 100,
  by = c("location_id", "sex_id", "age_group_id")
]
# only keep where comparison is one year previous
vr_raw[outlier == 0, ':=' (year_prev = shift(year_id), year_next = shift(year_id, type = "lead"))]
vr_raw[(year_prev != (year_id - 1) & outlier == 0), perc_diff_prev := NA]
vr_raw[perc_diff_prev == Inf, perc_diff_prev := NA]
vr_raw[(year_next != (year_id + 1) & outlier == 0), perc_diff_next := NA]
vr_raw[perc_diff_next == Inf, perc_diff_next := NA]
vr_raw[(abs(diff_prev) > 50) & (perc_diff_prev < -50) & (abs(diff_next) > 50) & (perc_diff_next > 50), diff_too_high := 1]
vr_raw[(abs(diff_prev) > 50) & (perc_diff_prev > 50) & (abs(diff_next) > 50) & (perc_diff_next < -50), diff_too_high := 1]
vr_raw[,
  ':=' (diff_prev = NULL, perc_diff_prev = NULL, year_prev = NULL,
       diff_next = NULL, perc_diff_next = NULL, year_next = NULL)
]

vr_raw[
  rank == 1 & diff_too_high == 1 & !year_id %in% c(2020:2021) &
    age_group_id %in% age_map_gbd[, age_group_id] & sex_id %in% 1:2 &
    !series_name %in% c("DSP", "SRS", "MCCD"),
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Far from neighbors; "))
]
vr_raw[, diff_too_high := NULL]

# Manually change new outliering
vr_raw <- manual_outliering_raw_vr(vr_raw)

vr_raw[outlier == 0, outlier_note := gsub("Manually outliered; Manually un-outliered; ", "Manually un-outliered; ", outlier_note)]
vr_raw[outlier == 0, outlier_note := gsub("^Manually un-outliered; ", "", outlier_note)]
vr_raw[outlier == 1, outlier_note := gsub("Manually un-outliered; Manually outliered; ", "Manually outliered; ", outlier_note)]
vr_raw[outlier == 1, outlier_note := gsub("Manually outliered; Manually outliered; ", "Manually outliered; ", outlier_note)]

# update outlier status based on SSA Data Inclusion Sheet
vr_raw <- merge(
  vr_raw,
  ssa_data_inclusion,
  by = c("location_id", "nid", "underlying_nid", "source_type"),
  all.x = TRUE
)

# outliering to prevent dupes and overwriting
vr_raw[location_id == 181, updated_outlier := 1] # MDG

vr_raw[location_id == 196 & nid == 287600 & year_id %in% 1997:2015, updated_outlier := 1] # ZAF
vr_raw[location_id == 196 & nid == 140967 & year_id %in% 2017:2018, updated_outlier := 1] # ZAF
vr_raw[location_id == 203 & nid == 287600 & year_id %in% 2011:2012, updated_outlier := 1] # CPV

vr_raw[!is.na(updated_outlier), outlier := updated_outlier]
vr_raw[
  !is.na(updated_outlier),
  outlier_note := ifelse(!is.na(updated_notes), paste0("SSA data inclusion (", updated_notes, "); "), "SSA data inclusion; ")
]
vr_raw[, c("updated_outlier", "updated_notes") := NULL]

vr_raw[location_id == 203 & nid == 18447, `:=` (outlier = 1, outlier_note = "SSA data inclusion; ")]

# outlier data not used in reference 5q0
vr_raw[, loc_yr := paste(location_id, year_id, sep = "_")]
vr_raw[
  !loc_yr %in% unique(reference_5q0$loc_yr) & age_end <= 5,
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered - not in reference 5q0; "))
]
vr_raw[, loc_yr := NULL]

# check for duplicate location/year/age_group_id/sex_id in final unoutliered data
demUtils::assert_is_unique_dt(
  vr_raw[outlier == 0],
  id_cols = c("location_id", "year_id", "sex_id", "age_group_id")
)

# save
readr::write_csv(
  vr_raw,
  fs::path("FILEPATH")
)

# append low granularity data
vr_data <- rbind(vr_raw, low_gran)

# subset out all sexes where sex specific is present
vr_data_sex_specific <- vr_data[!sex_id == 3]
vr_data_all_sexes <- vr_data[sex_id == 3]
vr_data_all_sexes <- anti_join(
  vr_data_all_sexes,
  vr_data_sex_specific,
  by = setdiff(names(vr_data_all_sexes), c("deaths", "outlier", "sex_id"))
) %>%
  setDT()
vr_data <- rbind(vr_data_sex_specific, vr_data_all_sexes)

# subset out all ages where age specific is present
vr_data_age_specific <- vr_data[!age_group_id == 22]
vr_data_all_ages <- vr_data[age_group_id == 22]
vr_data_all_ages <- anti_join(
  vr_data_all_ages,
  vr_data_age_specific,
  by = setdiff(names(vr_data_all_ages), c("deaths", "outlier", "age_group_id", "age_start", "age_end", "age_type"))
) %>%
  setDT()
vr_data <- rbind(vr_data_age_specific, vr_data_all_ages)

# outlier low gran if we have a different source
vr_data <- vr_data[order(location_id, year_id, age_group_id, sex_id, rank)]
vr_data[, rank := seq(.N), by = .(location_id, year_id, age_group_id, sex_id)]
vr_data[series_name == "low_gran" & rank != 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low granularity duplicate; "))]

# subset to GBD locations
vr_data <- vr_data[location_id %in% locs$location_id]

# check for duplicate location/year/source type in unoutliered data
demUtils::assert_is_unique_dt(
  vr_data[outlier == 0],
  id_cols = c("location_id", "year_id", "sex_id", "age_group_id")
)

# format to align with other hand-offs
vr_data <- merge(
  locs[, c("location_id", "ihme_loc_id")],
  vr_data,
  by = "location_id",
  all.y = TRUE
)

# merge empirical pop
vr_data_adj <- merge(
  vr_data,
  emp_pop_split_no_vr[, c("year_id", "location_id", "sex_id", "age_group_id", "source_type", "census_pop")],
  by = c("year_id", "location_id", "sex_id", "age_group_id", "source_type"),
  all.x = TRUE
)

# merge population estimates
vr_data_adj <- merge(
  vr_data_adj,
  pop[, c("year_id", "location_id", "sex_id", "age_group_id", "population")],
  by = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

# check for problematic missing census population estimate age groups
emp_pop_split_no_vr[, group :=  paste(location_id, year_id, source_type, sep = "_") ]
vr_data_adj[, group :=  paste(location_id, year_id, source_type, sep = "_") ]
needed_ages <- vr_data_adj[is.na(census_pop) & group %in% emp_pop_split_no_vr[, unique(group)]]
if (nrow(needed_ages) > 0) {
  message("Review empirical pop split to add more age groups")
  print(unique(needed_ages[, c("age_start", "age_end")]))
}
emp_pop_split_no_vr[, group := NULL]
vr_data_adj[, group := NULL]

vr_data_adj[is.na(census_pop), census_pop := population]
setnames(vr_data_adj, "census_pop", "sample_size")

# create missing population age groups
missing_age_groups <- vr_data_adj[is.na(population), unique(age_group_id)]
agg_population <- data.table()
for (age in missing_age_groups) {
  message(paste0("Creating population for: ", age))
  missing_age_start <- age_map_extended[age_group_id == age, age_start]
  missing_age_end <- age_map_extended[age_group_id == age, age_end]
  if (missing_age_end - missing_age_start < 1) stop("fix pop agg")
  if (missing_age_start > 95) stop("fix pop agg")
  if (missing_age_end != 125) {
    pop_sy_sub <- pop_sy[age_group_years_start >= missing_age_start & age_group_years_end <= missing_age_end & age_length == 1]
  } else {
    pop_sy_sub <- pop_sy[(age_group_years_start >= missing_age_start & age_group_years_end <= missing_age_end & age_length == 1) | age_group_id == 235]
  }
  pop_sy_sub <- pop_sy_sub[, .(agg_population = sum(population)), by = c("year_id", "location_id", "sex_id")]
  pop_sy_sub[, age_group_id := age]
  agg_population <- rbind(agg_population, pop_sy_sub)
}

# use agg pop for missing population
vr_data_adj <- merge(
  vr_data_adj,
  agg_population[, c("year_id", "location_id", "sex_id", "age_group_id", "agg_population")],
  by = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)
vr_data_adj[is.na(population) & !is.na(agg_population), population := agg_population]
vr_data_adj[is.na(sample_size) & !is.na(agg_population), sample_size := agg_population]
assertable::assert_values(vr_data_adj, "population", test = "not_na")
vr_data_adj[, agg_population := NULL]

# calculate mx
vr_data_adj[, mx := deaths / sample_size]

# apply srs scalar before calculating standard error
vr_data_adj <- merge(
  vr_data_adj,
  srs_pop_scalars,
  by = c("ihme_loc_id", "sex_id"),
  all.x = TRUE
)
vr_data_adj[ihme_loc_id %like% "IND" & year_id < 1993 & source_type == "SRS", ':=' (deaths = deaths / srs_scale, sample_size = sample_size / srs_scale)]
vr_data_adj[, srs_scale := NULL]

# calculate mx_se
vr_data_adj[, mx_se := sqrt((ifelse(mx >= 0.99, 0.99, mx) * (1 - ifelse(mx >= 0.99, 0.99, mx))) / sample_size)]

vr_data_adj[(ihme_loc_id == "BGD" | ihme_loc_id %like% "PAK") & source_type == "SRS", mx_se := mx_se * 100]
vr_data_adj[(ihme_loc_id == "BGD" | ihme_loc_id %like% "PAK") & source_type == "SRS", sample_size := (mx * (1 - mx)) / (mx_se^2)]
vr_data_adj[(ihme_loc_id == "BGD" | ihme_loc_id %like% "PAK") & source_type == "SRS", deaths := (mx * sample_size)]
vr_data_adj[
  (ihme_loc_id == "BGD" | ihme_loc_id %like% "PAK") & source_type == "SRS" & is.na(deaths),
  ':=' (deaths = 0, sample_size = 0, population = 0, mx = 0, mx_se = 0, outlier = 1, outlier_note = "Unknown age group with 0 deaths; ")
]

# calculate qx
vr_data_adj[, qx := demCore::mx_to_qx(mx, age_end - age_start)]

# Outlier sources with bad age distribution
age_group_id_order <- c(2, 3, 388, 389, 238, 34, 6:20, 30:32, 235)

vr_data_apply_outliers <- vr_data_adj[(age_group_id %in% age_group_id_order & sex_id %in% 1:2)]

vr_data_apply_outliers[, age_group_id := factor(age_group_id, levels = age_group_id_order)]

vr_data_apply_outliers <- vr_data_apply_outliers[order(location_id, year_id, sex_id, age_group_id)]
vr_data_apply_outliers[outlier == 0, mx_diff_prev := mx - shift(mx), by = c("location_id", "year_id", "sex_id")]
vr_data_apply_outliers[mx_diff_prev < 0 & age_group_id %in% c(20, 30:32, 235), bad_age_dist := 1]
vr_data_apply_outliers <- vr_data_apply_outliers %>%
  group_by(location_id, year_id, sex_id) %>%
  tidyr::fill(bad_age_dist, .direction = "down") %>%
  dplyr::ungroup() %>% setDT
vr_data_apply_outliers[bad_age_dist == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Bad old age distribution; "))]

bad_ages <- unique(
  vr_data_apply_outliers[bad_age_dist == 1, c("location_id", "ihme_loc_id", "year_id", "sex_id", "age_group_id", "age_start", "age_end")]
)
readr::write_csv(
  bad_ages,
  fs::path("FILEPATH")
)

vr_data_apply_outliers[, ':=' (mx_diff_prev = NULL, bad_age_dist = NULL,
                              age_group_id = as.numeric(as.character(age_group_id)))]

vr_data_adj <- rbind(
  vr_data_apply_outliers,
  vr_data_adj[!(age_group_id %in% age_group_id_order & sex_id %in% 1:2)]
)

# outlier mx >= 1
vr_data_adj[mx >= 1 & !age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Mx >= 1; "))]

# outlier based on qx being too low in old ages (80-90)
vr_data_adj[deaths > 50 & age_group_id == 32 & sex_id == 1 & qx < 0.25, ':=' (outlier = 1, low_qx = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_data_adj[deaths > 50 & age_group_id == 32 & sex_id == 2 & qx < 0.20, ':=' (outlier = 1, low_qx = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_data_adj[deaths > 50 & age_group_id == 31 & sex_id == 1 & qx < 0.15, ':=' (outlier = 1, low_qx = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_data_adj[deaths > 50 & age_group_id == 31 & sex_id == 2 & qx < 0.10, ':=' (outlier = 1, low_qx = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_data_adj[deaths > 50 & age_group_id == 30 & sex_id == 1 & qx < 0.10, ':=' (outlier = 1, low_qx = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
vr_data_adj[deaths > 50 & age_group_id == 30 & sex_id == 2 & qx < 0.05, ':=' (outlier = 1, low_qx = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
low_qx <- unique(vr_data_adj[low_qx == 1, c("location_id", "ihme_loc_id", "year_id", "sex_id", "age_group_id", "age_start", "age_end", "deaths", "mx", "qx")])
readr::write_csv(
  low_qx,
  fs::path("FILEPATH")
)
vr_data_adj[, low_qx := NULL]

# outlier based on improper sex ratio in older age groups
demUtils::assert_is_unique_dt(
  vr_data_adj,
  id_cols = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "sex_id", "source_type_id", "rank")
)
vr_sex_ratios <- dcast(
  vr_data_adj,
  nid + underlying_nid + location_id + year_id + age_group_id + source_type_id + rank ~ sex_id,
  value.var = c("mx", "deaths")
)
vr_sex_ratios[, sex_ratio := mx_2 / mx_1]
vr_sex_ratios[deaths_1 > 25 & deaths_2 > 25 & sex_ratio > 1.25 & age_group_id %in% c(20, 30:32, 235), outlier_new := 1]
vr_data_adj <- merge(
  vr_data_adj,
  vr_sex_ratios[, -c("mx_1", "mx_2", "deaths_1", "deaths_2", "sex_ratio")],
  by = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "source_type_id", "rank"),
  all.x = TRUE
)
vr_data_adj[outlier_new == 1 & !sex_id == 4, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Improper sex ratio in older ages; "))]
vr_data_adj[, outlier_new := NULL]

# final formatting
vr_data_formatted <- copy(vr_data_adj)
setnames(
  vr_data_formatted,
  c("location_id", "year_id", "ihme_loc_id"),
  c("location", "date_start", "country_ihme_loc_id")
)

vr_data_formatted[, ':=' (
  date_end = date_start + 1, date_granularity = "detailed", date_format = "YYYY",
  age_unit = "years", age_granularity = "detailed", location_granularity = "detailed",
  location_type = "location_id", dem_measure_id = 10)
]

# re-format
key_cols <- c(
  "location", "nid", "sex_id", "date_start", "date_end", "age_start", "age_end", "age_group_id",
  "date_granularity", "date_format", "age_unit", "age_granularity", "source_type_id",
  "source_type", "location_granularity", "underlying_nid", "title", "underlying_title",
  "country_ihme_loc_id", "location_type", "dem_measure_id", "sample_size", "population",
  "deaths", "mx", "mx_se", "qx", "outlier", "outlier_note", "series_name",
  "extraction_source", "rank"
)
vr_data_formatted <- vr_data_formatted[, ..key_cols]

# fix outlier_note ending
vr_data_formatted[, outlier_note := gsub("; $", "", outlier_note)]
vr_data_formatted[outlier_note == "", outlier_note := NA]

# there are some duplicate outliers so make dataset unique
vr_data_formatted <- unique(vr_data_formatted)

# check for duplicates
demUtils::assert_is_unique_dt(
  vr_data_formatted,
  id_cols = setdiff(names(vr_data_formatted), "source_type_id")
)

# check for duplicate location/year/source type in unoutliered data
demUtils::assert_is_unique_dt(
  vr_data_formatted[outlier == 0],
  id_cols = c("location", "date_start", "sex_id", "age_start", "age_end")
)

# check for missingness
assertable::assert_values(
  vr_data_formatted,
  colnames = setdiff(colnames(vr_data_formatted), c("title", "rank", "underlying_nid", "underlying_title", "outlier_note")),
  test = "not_na"
)
assertable::assert_values(
  vr_data_formatted[!is.na(underlying_nid)],
  colnames = "underlying_title",
  test = "not_na"
)
assertable::assert_values(
  vr_data_formatted[outlier == 1],
  colnames = "outlier_note",
  test = "not_na"
)

# Save -------------------------------------------------------------------------

# GBD AGE GROUPS #

vr_data_formatted_gbd_ages <- vr_data_formatted[age_group_id %in% age_map_gbd$age_group_id & sex_id %in% c(1, 2)]
vr_data_formatted_gbd_ages[, age_group_id := NULL]

# compare mx to previous version
prev_version <-  fread(
  fs::path_norm(
    fs::path(
      "FILEPATH"
    )
  )
)

mx_comparison <- quick_mx_compare(
  test_file = vr_data_formatted_gbd_ages,
  compare_file = prev_version,
  id_cols = c(
    "location", "country_ihme_loc_id", "nid", "underlying_nid", "sex_id",
    "date_start", "age_start", "age_end", "source_type", "source_type_id",
    "outlier", "extraction_source"
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
  vr_data_formatted_gbd_ages,
  fs::path(
    "FILEPATH"
  )
)
message("Saved gbd ages.")

# NON-STANDARD AGES #

vr_data_formatted_non_standard_ages <- vr_data_formatted[!(age_group_id %in% age_map_gbd$age_group_id & sex_id %in% c(1, 2))]
vr_data_formatted_non_standard_ages[, age_group_id := NULL]

# compare mx to previous version
prev_version <-  fread(
  fs::path_norm(
    fs::path(
      "FILEPATH"
    )
  )
)

mx_comparison <- quick_mx_compare(
  test_file = vr_data_formatted_non_standard_ages,
  compare_file = prev_version,
  id_cols = c(
    "location", "country_ihme_loc_id", "nid", "underlying_nid", "sex_id",
    "date_start", "age_start", "age_end", "source_type", "source_type_id",
    "outlier", "extraction_source"
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
  vr_data_formatted_non_standard_ages,
  fs::path(
    "FILEPATH"
  )
)
message("Saved non-standard ages.")

