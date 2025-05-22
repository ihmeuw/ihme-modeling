
## Meta ------------------------------------------------------------------------

# Description: Download and process population data

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and clean data in VRP intermediates
#   4. Save

# Inputs:
#   *    "FILEPATH": detailed configuration file.
#   *    Empirical population
#   *    "FILEPATH": prepped IND SRS populations from setup
# Outputs:
#   *    "FILEPATH"

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(demInternal)
library(dplyr)
library(mortdb)

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

age_map_gbd <- fread(fs::path("FILEPATH"))
age_map_extended <- fread(fs::path("FILEPATH"))

all_ages <- mortdb::get_age_map(type = "all", gbd_year = gbd_year)
all_ages <- all_ages[,
  .(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)
]

source_map <- mortdb::get_mort_ids("source_type")

locs <- fread(fs::path("FILEPATH"))

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# pull GBD 2021 with shock death numbers
wsdn <- mortdb::get_mort_outputs(
  model_name = "with shock death number",
  model_type = "estimate",
  run_id = wsdn_version,
  age_group_id = 22
)

# Read in and clean data in VRP intermediates ----------------------------------

noncod_raw <- fread(fs::path("FILEPATH"))

noncod_raw <- noncod_raw[!nid == 536737]

# add CHN_SSPC
chn_sspc <- fread("FILEPATH")

chn_sspc <- merge(
  chn_sspc,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)

chn_sspc[, source := "SSPC"]
chn_sspc[, source_type_id := 36]
chn_sspc[, underlying_nid := NA]

# add CHN DSP 2016-17
chn_dsp <- fread("FILEPATH")

chn_dsp[, ':=' (age_start = round(age_start, 7), age_end = round(age_end, 7))]
chn_dsp <- merge(
  chn_dsp,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)

chn_dsp[, source := "DSP"]
chn_dsp[, source_type_id := 3]
chn_dsp[, underlying_nid := NA]

# add AZE 2023
noncod_raw <- noncod_raw[!(location_id == 34 & year_id == 2023)]
aze_2023 <- fread("FILEPATH")
aze_2023 <- merge(
  aze_2023,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)

aze_2023[, source := "VR"]
aze_2023[, source_type_id := 1]
aze_2023[, underlying_nid := NA]

# add MEX 2023
mex_2023 <- fread("FILEPATH")
mex_2023 <- merge(
  mex_2023,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)
mex_2023[, source := "VR"]
mex_2023[, source_type_id := 1]
mex_2023[, underlying_nid := NA]

noncod_raw <- rbind(
  noncod_raw,
  chn_sspc[age_group_id != 22, c("sex_id", "source", "source_type_id", "nid", "underlying_nid", "deaths", "age_group_id", "location_id", "year_id")],
  chn_dsp[, c("sex_id", "source", "source_type_id", "nid", "underlying_nid", "deaths", "age_group_id", "location_id", "year_id")],
  aze_2023[, c("sex_id", "source", "source_type_id", "nid", "underlying_nid", "deaths", "age_group_id", "location_id", "year_id")],
  mex_2023[, c("sex_id", "source", "source_type_id", "nid", "underlying_nid", "deaths", "age_group_id", "location_id", "year_id")]
)

noncod_raw[, group_id := paste(sex_id, source_type_id, nid, underlying_nid, age_group_id, location_id, year_id, sep = "_")]
noncod_raw[nid %in% c(469163, 469165, 528380, 528381), source_type_id := 3]
noncod_raw[, extraction_source := "noncod"]

cod_raw <- fread(fs::path("FILEPATH"))
cod_raw[
  nid %in% c(
    127808, 127809, 127810, 127811, 270005, 270007, 270008, 270009, 270010, 270011,
    270012, 270013
  ),
  ':=' (source_type_id = 3, outlier = 1, outlier_note = "Prioritize NonCod DSP; ")
]
cod_raw[nid %in% c(338606, 409164), source_type_id := 3]

cod_raw[, group_id := paste(sex_id, source_type_id, nid, underlying_nid, age_group_id, location_id, year_id, sep = "_")]
cod_raw[, extraction_source := "cod"]

# add comsa SLE here to maintain 'unknown' overlapping age granularity
comsa_sle <- fread("FILEPATH")
comsa_sle[, ':=' (age_start = round(age_start, 7), age_end = round(age_end, 7))]
comsa_sle <- merge(
  comsa_sle,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)
comsa_sle <- comsa_sle[, -c("age_start", "age_end")]
comsa_sle[, ':=' (underlying_nid = NA, source_type_id = 2, extraction_source = "noncod", ihme_loc_id = NULL)]
comsa_sle <- comsa_sle[year_id %in% 2017:2019] # only include complete years

# add comsa MOZ here to maintain 'unknown' overlapping age granularity
comsa_moz <- fread("FILEPATH")
comsa_moz[, ':=' (age_start = round(age_start, 7), age_end = round(age_end, 7))]
comsa_moz <- merge(
  comsa_moz,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)
comsa_moz <- comsa_moz[, -c("age_start", "age_end")]
comsa_moz[, ':=' (underlying_nid = NA, source_type_id = 2, extraction_source = "noncod", ihme_loc_id = NULL)]

# add BRA MOH under-5 mortality rates
bra_vr <- fread("FILEPATH")
bra_vr <- bra_vr[!is.na(q5), !c("source_date", "in_direct", "data_age")]

setnames(bra_vr, c("iso3", "t", "q5"), c("ihme_loc_id", "year_id", "qx"))

bra_vr <- merge(
  bra_vr,
  locs[, c("location_id", "ihme_loc_id")],
  by = "ihme_loc_id",
  all.x = TRUE
)

bra_vr[, age_group_id := 1]
bra_vr[, extraction_source := "noncod"]
bra_vr[, sex_id := 3]
bra_vr[, source_type_id := 1]
bra_vr[, underlying_nid := NA]

bra_vr <- merge(
  bra_vr,
  pop[, c("year_id", "location_id", "sex_id", "age_group_id", "population")],
  by = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

bra_vr[, mx := demCore::qx_to_mx(qx / 1000, 5)]
bra_vr[, deaths := population * mx]

bra_vr <- bra_vr[, !c("ihme_loc_id", "mx", "qx", "population")]

# combine cod and noncod
cod_noncod <- rbind(cod_raw, noncod_raw, comsa_sle, comsa_moz, bra_vr, fill = TRUE)
cod_noncod[is.na(outlier), ':=' (outlier = 0, outlier_note = "")]
cod_noncod[, group_id := NULL]

# subset by source type
cod_noncod <- source_type_id_correction(cod_noncod)

# pull MCCD out of VR
cod_noncod[source %like% "MCCD", `:=` (source_type = "MCCD", source_type_id = 55)]

# find age groups that are too granular
gbd_age_group_ids <- age_map_gbd$age_group_id
cod_noncod[age_group_id == 294, age_group_id := 237]
cod_noncod[age_map_extended, ':=' (age_start = i.age_start, age_end = i.age_end), on = "age_group_id"]
cod_noncod[, age_length := age_end - age_start]
cod_noncod[age_start >= 0 & age_end <= 0.0191781 & age_length < 0.0191781 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[age_start >= 0.0191781 & age_end <= 0.0767123 & age_length < 0.0575342 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[age_start >= 0.0767123 & age_end <= 0.5000000 & age_length < 0.4232877 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[age_start >= 0.5000000 & age_end <= 1 & age_length < 0.5000000 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[age_start >= 1 & age_end <= 2 & age_length < 1 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[age_start >= 2 & age_end <= 5 & age_length < 3 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[age_start >= 5 & age_end <= 95 & age_length < 5 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[age_start >= 95 & age_end <= 125 & age_length < 30 & !age_group_id %in% gbd_age_group_ids, age_type := "too_granular"]
cod_noncod[, age_length := NULL]

cod_noncod[is.na(age_type) & age_group_id %in% age_map_gbd$age_group_id, age_type := "gbd_age_group"]
cod_noncod[is.na(age_type) & !age_group_id %in% age_map_gbd$age_group_id, age_type := "too_wide"]

# EGY aggregate ages 10+ into 10 year age groups, and collapse terminal to 70+
data_egy <- cod_noncod[location_id == 141 & year_id %in% 1950:1992 & age_start >= 10 & !age_group_id == 283]
data_egy[, age_type := NULL]
data_egy_aggs <- hierarchyUtils::agg(
  dt = data_egy[, -c("age_group_id")],
  id_cols = setdiff(names(data_egy), c("deaths", "age_group_id")),
  col_stem = "age",
  col_type = "interval",
  value_cols = "deaths",
  mapping = data.table(age_start = seq(10, 70, by = 10), age_end = c(seq(20, 70, by = 10), 125)),
  overlapping_dt_severity = "warning",
  missing_dt_severity = "warning"
)
data_egy_aggs[, age_type := "aggregated"]
data_egy_aggs[age_map_extended, age_group_id := i.age_group_id, on = c("age_start", "age_end")]
cod_noncod <- cod_noncod[!(location_id == 141 & year_id %in% 1950:1992 & age_start >= 10 & !age_group_id == 283)]
cod_noncod <- rbind(cod_noncod, data_egy_aggs)

# if a source only has the unknown age group then assume it is all ages
all_unknown <- cod_noncod %>%
  group_by(nid, location_id, year_id) %>%
  filter(all(age_group_id == 283)) %>%
  mutate(age_start = 0) %>%
  mutate(age_end = 125) %>%
  mutate(age_group_id = 22) %>%
  setDT

not_all_unknown <- cod_noncod %>%
  group_by(nid, location_id, year_id) %>%
  filter(!all(age_group_id == 283)) %>%
  setDT

cod_noncod <- rbind(all_unknown, not_all_unknown)

all_unknown <- merge(
  all_unknown,
  wsdn[, c("location_id", "year_id", "sex_id", "age_group_id", "mean")],
  by = c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)
setnames(all_unknown, "mean", "gbd2021_mean")
readr::write_csv(
  all_unknown,
  fs::path("FILEPATH")
)

# aggregate to gbd age groups if possible
cod_noncod_gbd_aggs <- hierarchyUtils::agg(
  dt = cod_noncod[age_type == "too_granular", -c("age_group_id")],
  id_cols = setdiff(names(cod_noncod), c("deaths", "age_group_id")),
  col_stem = "age",
  col_type = "interval",
  value_cols = "deaths",
  mapping = age_map_gbd[, -c("age_group_id")],
  overlapping_dt_severity = "warning",
  missing_dt_severity = "warning"
)
cod_noncod_gbd_aggs[, age_type := "aggregated"]
cod_noncod_gbd_aggs[age_map_gbd, age_group_id := i.age_group_id, on = c("age_start", "age_end")]

# combine
cod_noncod <- rbind(cod_noncod, cod_noncod_gbd_aggs)
cod_noncod[,
  dup := .N,
  by = setdiff(names(cod_noncod), "age_type")
]
cod_noncod <- cod_noncod[dup == 1 | (dup == 2 & age_type == "gbd_age_group")]
cod_noncod[, dup := NULL]
cod_noncod <- cod_noncod[!age_type == "too_granular"]

# Data Adjustments

# replace outdated sex_id 9 with sex_id 4
cod_noncod[sex_id == 9, sex_id := 4]

# USA subnats 1972: 50% sample and is weighted by a factor of 2 (from GHDx)
cod_noncod[nid == 157276 & year_id == 1972, deaths := deaths * 2]

# standardize CHN census year
cod_noncod[nid == 130052, year_id := 1982]

# DYB nid change
cod_noncod[nid == 140966, nid := 140967]

# Save -------------------------------------------------------------------------

readr::write_csv(
  cod_noncod,
  fs::path("FILEPATH")
)
