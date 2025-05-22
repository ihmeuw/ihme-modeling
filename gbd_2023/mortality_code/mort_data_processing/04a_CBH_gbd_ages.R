
## Meta ------------------------------------------------------------------------

# Description: Create CBH dataset with only GBD age groups

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and format CBH data from prep
#   4. Read in and format ENN/LNN data from age-sex
#   5. Produce final CBH output
#   6. Save

# Load libraries ---------------------------------------------------------------

library(assertable)
library(data.table)
library(demInternal)
library(dplyr)
library(mortdb)
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

# Read in internal inputs ------------------------------------------------------

age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# read in reference 5q0
reference_5q0 <- fread(fs::path("FILEPATH"))

# Read in and format CBH data from prep ----------------------------------------

prep_ds_onemod <- arrow::open_dataset(
  sources = fs::path("FILEPATH")
)
prep_ds_onemod_enn_lnn <- arrow::open_dataset(
  sources = fs::path("FILEPATH")
)

# prep cbh data
cbh_data <- prep_ds_onemod |>
  dplyr::filter(dem_measure_id %in% c(19, 20, 13)) |> # nmx, nqx, p-ys
  dplyr::filter(collection_method_id == 1) |> # cbh
  dplyr::filter(date_format != "years_preceding_collection") |>
  dplyr::filter(location_type == "location_id") |>
  dplyr::collect() |>
  data.table()

# pull enn and lnn data from prep
cbh_data_enn_lnn <- prep_ds_onemod_enn_lnn |>
  dplyr::filter(dem_measure_id %in% c(19, 20, 13)) |> # nmx, nqx, p-ys
  dplyr::filter(collection_method_id == 1) |> # cbh
  dplyr::filter(date_format != "years_preceding_collection") |>
  dplyr::filter(location_type == "location_id") |>
  dplyr::collect() |>
  data.table()

cbh_data_enn_lnn <- cbh_data_enn_lnn[
  (age_start == 0 & age_end == 0.0191781) | (age_start == 0.0191781 & age_end == 0.0767123)
]

cbh_data_enn_lnn[, enn_lnn_source := "prep"]

readr::write_csv(
  cbh_data_enn_lnn,
  fs::path(main_dir, "outputs", "intermediates", "enn_lnn_CBH.csv")
)

cbh_data <- rbind(
  cbh_data,
  cbh_data_enn_lnn,
  fill = TRUE
)

# output list of dhs nids missing from prep if file doesn't already exist
if (!(fs::file_exists(fs::path("FILEPATH")))) {

  prep_source_list <- fread(
    fs::path("FILEPATH")
  )

  prep_source_list <- demInternal::merge_ghdx_record_fields(prep_source_list, fields = "series_nid")

  prepped_dhs <- prep_source_list[series_nid %like% 517 | series_nid %like% 422386]

  missing_dhs <- prepped_dhs[!(nid %in% unique(cbh_data$nid)), c("ihme_loc_id", "nid", "survey_name", "year_start", "year_end")]
  missing_dhs[, reason := ""]

  readr::write_csv(
    missing_dhs,
    fs::path("FILEPATH")
  )

}

cbh_data <- cbh_data[, -c("other", "other_type", "other_granularity", "reg_day_missing", "event")]

cbh_data <- merge(
  cbh_data,
  age_map_extended,
  by = c("age_start", "age_end"),
  all.x = TRUE
)

enn_lnn_prep_tracker <- fread(fs::path("FILEPATH"))
enn_lnn_prep_tracker <- enn_lnn_prep_tracker[, c("nid", "prepped", "notes")]
enn_lnn_prep_tracker[prepped == "-", fixable := "no"]
enn_lnn_prep_tracker[prepped %like% "79", fixable := "fixed"]
enn_lnn_prep_tracker[notes %in% c(
  "skip for now, national not estimated",
  "skip for now, subnational not estimated"),
  fixable := "skipped"
]
enn_lnn_prep_tracker[is.na(fixable), fixable := "yes"]

# check for missing data
enn_lnn_missing <- cbh_data[age_group_id %in% c(388:389, 238, 34, 6:7) & !(nid %in% cbh_data[age_group_id %in% 2:3]$nid)]
if (nrow(enn_lnn_missing) > 0) {
  enn_lnn_missing_nids <- as.data.table(sort(unique(enn_lnn_missing$nid)))
  setnames(enn_lnn_missing_nids, "V1", "nid")
  enn_lnn_missing_nids <- merge(
    enn_lnn_missing_nids,
    enn_lnn_prep_tracker[, c("nid", "fixable", "notes")],
    by = "nid",
    all.x = TRUE
  )
  enn_lnn_missing_nids[is.na(fixable), fixable := "yes"]

  readr::write_csv(
    enn_lnn_missing_nids,
    paste0("FILEPATH"),
    na = ""
  )

  warning(paste0("These nids have older ages but not enn/lnn: ", list(sort(unique(enn_lnn_missing_nids[fixable == "yes"]$nid)))))
  warning(paste0("These nids were marked fixed for enn/lnn, but are still missing: ", list(sort(unique(enn_lnn_missing_nids[fixable == "fixed"]$nid)))))
}

older_ages_missing <- cbh_data[age_group_id %in% 2:3 & !(nid %in% cbh_data[age_group_id %in% c(388:389, 238, 34, 6:7)]$nid)]
if (nrow(older_ages_missing) > 0) {
  older_ages_missing_nids <- as.data.table(sort(unique(older_ages_missing$nid)))
  setnames(older_ages_missing_nids, "V1", "nid")
  older_ages_missing_nids <- merge(
    older_ages_missing_nids,
    enn_lnn_prep_tracker[, c("nid", "fixable", "notes")],
    by = "nid",
    all.x = TRUE
  )
  older_ages_missing_nids[is.na(fixable), fixable := "yes"]

  readr::write_csv(
    older_ages_missing_nids,
    paste0("FILEPATH"),
    na = ""
  )

  warning(paste0("These nids have enn/lnn but not older ages: ", list(sort(unique(older_ages_missing_nids[fixable == "yes"]$nid)))))
  warning(paste0("These nids were marked fixed for older ages, but are still missing: ", list(sort(unique(older_ages_missing_nids[fixable == "fixed"]$nid)))))
}

# drop MEX ENADID
cbh_data <- cbh_data[!(nid %in% c(8480, 8517, 24006, 240604, 404407))]

# separate out neonatal age group (sent to age-sex splitting)
cbh_data_nn <- cbh_data[age_group_id == 42]

readr::write_csv(
  cbh_data_nn,
  fs::path("FILEPATH")
)

cbh_data <- cbh_data[age_group_id %in% age_map_gbd$age_group_id] # subset to most detailed age groups to prevent overlap

cbh_data_mx <- cbh_data[
  dem_measure_id == 19,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value", "uncertainty_value", "enn_lnn_source")
]
cbh_data_qx <- cbh_data[
  dem_measure_id == 20,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value", "uncertainty_value", "enn_lnn_source")
]
cbh_data_pys <- cbh_data[
  dem_measure_id == 13,
  c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value", "enn_lnn_source")
]

setnames(cbh_data_mx, "value", "mx")
setnames(cbh_data_qx, "value", "qx")
setnames(cbh_data_pys, "value", "sample_size")

setnames(cbh_data_mx, "uncertainty_value", "mx_se")
setnames(cbh_data_qx, "uncertainty_value", "qx_se")

cbh_data_wide <- merge(
  cbh_data_mx,
  cbh_data_qx,
  by = c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "enn_lnn_source"),
  all = TRUE
)

cbh_data_wide <- merge(
  cbh_data_wide,
  cbh_data_pys,
  by = c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "enn_lnn_source"),
  all = TRUE
)

# drop SDN duplicate source
cbh_data_wide <- cbh_data_wide[nid != 153563]

# drop rows where sample size is 0
cbh_data_wide <- cbh_data_wide[!(is.na(mx) & is.na(qx) & sample_size == 0)]

# drop rows where mx is missing
cbh_data_wide_missing_mx <- cbh_data_wide[age_group_id %in% 2:3 & is.na(mx)]
readr::write_csv(
  cbh_data_wide_missing_mx,
  fs::path("FILEPATH"),
  na = ""
)
cbh_data_wide <- cbh_data_wide[!(age_group_id %in% 2:3 & is.na(mx))]

# drop 15-19 and 20-24
cbh_data_wide <- cbh_data_wide[!(age_group_id %in% 8:9)]

# check for duplicates
demUtils::assert_is_unique_dt(
  cbh_data_wide,
  c("nid", "location", "sex_id", "age_group_id", "date_start", "date_end")
)

# Read in and format ENN/LNN data from age-sex ---------------------------------

# pull enn and lnn data from age-sex
agesex_data <- fread(
  paste0(
    "FILEPATH"
  )
)

agesex_data <- agesex_data[sex %in% c("female", "male")]
agesex_data <- agesex_data[!(broadsource %like% "VR|SRS|DSP")]

# fix metadata to add nids
agesex_data[, source_new := sub(".*___", "", source)]
agesex_data[source_new %like% "AIS", survey_type := "AIS"]
agesex_data[source_new %like% "DHS SP", survey_type := "DHS SP"]
agesex_data[is.na(survey_type) & source_new %like% "DHS", survey_type := "DHS"]
agesex_data[source_new %like% "ENADID", survey_type := "ENADID"]
agesex_data[source_new %like% "HDS", survey_type := "HDS"]
agesex_data[source_new %like% "LSMS", survey_type := "LSMS"]
agesex_data[source_new %like% "MICS", survey_type := "MICS"]
agesex_data[source_new %like% "MIS", survey_type := "MIS"]
agesex_data[source_new %like% "PAPCHILD", survey_type := "PAPCHILD"]
agesex_data[source_new %like% "PAPFAM", survey_type := "PAPFAM"]
agesex_data[source_new %like% "RHS", survey_type := "RHS"]
agesex_data[source_new %like% "WFS", survey_type := "WFS"]
agesex_data[source_new %like% "ZAF_OCT_HH", survey_type := "OCT HH"]

agesex_data[, survey_year := as.numeric(substr(stringr::str_extract(source_new, "[[:digit:]]+"), 1, 4))]

agesex_data[ihme_loc_id == "CIV" & survey_type == "AIS" & is.na(survey_year), survey_year := 2005]
agesex_data[ihme_loc_id == "DOM" & source %like% "ENHOGAR" & is.na(survey_year), survey_year := 2006]
agesex_data[ihme_loc_id == "EGY" & survey_type == "MICS" & is.na(survey_year), survey_year := 2013]
agesex_data[ihme_loc_id == "GUY" & survey_type == "AIS" & is.na(survey_year), survey_year := 2005]
agesex_data[ihme_loc_id == "IDN" & source %like% "IFLS" & is.na(survey_year), survey_year := 2007]
agesex_data[ihme_loc_id == "IRQ" & source %like% "IFHS" & is.na(survey_year), survey_year := 2006]
agesex_data[ihme_loc_id == "IRQ" & source %like% "IMIRA" & is.na(survey_year), survey_year := 2004]
agesex_data[ihme_loc_id == "TZA" & survey_type == "AIS" & is.na(survey_year), survey_year := 2007]
agesex_data[ihme_loc_id == "TZA" & survey_type == "AIS" & is.na(survey_year), survey_year := 2007]

agesex_data[ihme_loc_id == "BOL" & source %like% "EDSA" & survey_year == 2016, survey_type := "DHS"]
agesex_data[ihme_loc_id == "GHA" & survey_year == 2017, survey_type := "DHS SP"]
agesex_data[ihme_loc_id == "IRN" & survey_year == 2000, survey_type := "DHS"]
agesex_data[ihme_loc_id == "NIC" & source %like% "ENDESA 2011-2012" & survey_year == 2011, survey_type := "DHS"]
agesex_data[ihme_loc_id == "PSE" & source %like% "DHS" & survey_year == 2004, survey_type := "DHS"]
agesex_data[ihme_loc_id == "PSE" & source %like% "PSE_HEALTH_SURVEY_2000" & survey_year == 2000, survey_type := "MICS"]

ghdx_surveys <- demInternal::get_cooper_project(project_nid = 490969)
ghdx_surveys <- demInternal::merge_ghdx_record_fields(ghdx_surveys, fields = "geography")

ghdx_surveys[geography == "Bolivia", geography := "Bolivia (Plurinational State of)"]
ghdx_surveys[geography == "Côte d'Ivoire", geography := "Cote d'Ivoire"]
ghdx_surveys[geography == "Egypt, Al Gharbīyah, Al Minyā, Al Qalyūbīyah, Asyūt, Qinā, Sūhāj", geography := "Egypt"]
ghdx_surveys[geography == "Mali, Bamako, Kayes, Koulikoro, Mopti, Sikasso, Ségou", geography := "Mali"]
ghdx_surveys[geography == "Mozambique, Cabo Delgado, Gaza, Inhambane, Manica, Maputo, Maputo [City], Nampula, Niassa, Sofala, Tete, Zambézia", geography := "Mozambique"]
ghdx_surveys[geography == "Pakistan, Balochistan, Gilgit-Baltistan, Islamabad, Khyber Pakhtunkhwa, Punjab, Sindh", geography := "Pakistan"]
ghdx_surveys[geography == "Palestine, Gaza Strip, West Bank", geography := "Palestine"]
ghdx_surveys[geography == "South Africa, Eastern Cape, Free State, Gauteng, KwaZulu-Natal, Limpopo, Mpumalanga, North West, Northern Cape, Western Cape", geography := "South Africa"]
ghdx_surveys[geography == "Türkiye", geography := "Turkiye"]
ghdx_surveys[geography == "United Republic of Tanzania, Zanzibar Central/South, Zanzibar North, Zanzibar Urban/West", geography := "United Republic of Tanzania"]
ghdx_surveys[geography == "United States of America, Puerto Rico", geography := "Puerto Rico"]

ghdx_surveys <- merge(
  ghdx_surveys,
  locs[!(ihme_loc_id %in% c("MEX_4657", "NGA_25344", "USA_533")), c("location_name", "ihme_loc_id")],
  by.x = "geography",
  by.y = "location_name",
  all.x = TRUE
)

ghdx_surveys[nid == 7401, ihme_loc_id := "KEN_44795"]
ghdx_surveys[nid == 56420, ihme_loc_id := "KEN_44794"]
ghdx_surveys[nid == 135416, ihme_loc_id := "KEN_44798"]
ghdx_surveys[nid == 155335, ihme_loc_id := "KEN_44797"]
ghdx_surveys[nid == 203654, ihme_loc_id := "KEN_35619"]
ghdx_surveys[nid == 203663, ihme_loc_id := "KEN_35627"]
ghdx_surveys[nid == 203664, ihme_loc_id := "KEN_35659"]

# drop unused surveys causing duplicates
ghdx_surveys <- ghdx_surveys[
  !(nid %in% c(20664, 20700, 21412, 141913, 146860, 209930, 233903, 238388, 256233, 437724))
]

ghdx_surveys[title %like% "AIDS Indicator", survey_type := "AIS"]
ghdx_surveys[title %like% "Demographic and Health", survey_type := "DHS"]
ghdx_surveys[title %like% "Demographic Dynamics", survey_type := "ENADID"]
ghdx_surveys[title %like% "India Human Development Survey", survey_type := "HDS"]
ghdx_surveys[title %like% "Living Standards Measurement", survey_type := "LSMS"]
ghdx_surveys[title %like% "Malaria Indicator", survey_type := "MIS"]
ghdx_surveys[title %like% "Multiple Indicator Cluster", survey_type := "MICS"]
ghdx_surveys[title %like% "Reproductive Health Survey", survey_type := "RHS"]
ghdx_surveys[title %like% "South Africa October Household Survey", survey_type := "OCT HH"]
ghdx_surveys[title %like% "Special Demographic and Health", survey_type := "DHS SP"]
ghdx_surveys[title %like% "World Fertility Survey", survey_type := "WFS"]

ghdx_surveys[
  nid %in% c(634, 3583, 7676, 7761, 8101, 8829, 12259, 12388, 12970),
  survey_type := "PAPCHILD"
]
ghdx_surveys[
  nid %in% c(627, 3392, 9999, 12379, 12978, 13795, 24143, 44861, 107340, 126909, 126911, 218035, 464295),
  survey_type := "PAPFAM"
]

ghdx_surveys[ihme_loc_id == "BLZ" & nid %in% c(27646, 30261), survey_type := "RHS"]
ghdx_surveys[ihme_loc_id == "CRI" & nid == 27301, survey_type := "RHS"]
ghdx_surveys[ihme_loc_id == "ECU" & nid == 14243, survey_type := "RHS"]
ghdx_surveys[ihme_loc_id == "HND" & nid == 5025, survey_type := "RHS"]
ghdx_surveys[ihme_loc_id == "PRY" & nid == 10326, survey_type := "RHS"]
ghdx_surveys[ihme_loc_id == "SLV" & nid == 27582, survey_type := "RHS"]
ghdx_surveys[ihme_loc_id == "TZA" & nid == 12644, survey_type := "AIS"]
ghdx_surveys[ihme_loc_id == "ZWE" & nid == 35493, survey_type := "MICS"]

ghdx_surveys[, survey_year := as.numeric(substr(stringr::str_extract(title, "[[:digit:]]+"), 1, 4))]

ghdx_surveys <- ghdx_surveys[survey_year %in% 1950:2024]

ghdx_surveys[nid == 8829, survey_year := 1997] # MAR PAPCHILD
ghdx_surveys[nid == 12970, survey_year := 1995] # TUN PAPCHILD
ghdx_surveys[nid == 26919, survey_year := 2005] # IND HDS

# append on nids for subnational locations
for (nat in c("BRA", "ETH", "IDN", "IND", "KEN", "MEX", "NGA", "PAK", "PHL", "ZAF")) {

  ghdx_surveys_nat <- ghdx_surveys[ihme_loc_id == nat & survey_type == "DHS"]

  for (sub in locs[ihme_loc_id %like% nat & level > 3]$ihme_loc_id) {

    ghdx_surveys_sub <- copy(ghdx_surveys_nat)
    ghdx_surveys_sub[, ihme_loc_id := sub]

    ghdx_surveys <- rbind(ghdx_surveys, ghdx_surveys_sub)

  }

}

for (prov in c("KEN_44794", "KEN_44795", "KEN_44797", "KEN_44798")) {

  ghdx_surveys_prov <- ghdx_surveys[ihme_loc_id == prov & survey_type == "MICS"]

  for (county in locs[parent_id == as.numeric(substr(prov, 5, 9))]$ihme_loc_id) {

    ghdx_surveys_county <- copy(ghdx_surveys_prov)
    ghdx_surveys_county[, ihme_loc_id := county]

    ghdx_surveys <- rbind(ghdx_surveys, ghdx_surveys_county)

  }

}

for (sub in locs[ihme_loc_id %like% "IND" & level > 3]$ihme_loc_id) {

  ghdx_surveys_sub <- copy(ghdx_surveys[ihme_loc_id == "IND" & survey_type %like% "HDS"])
  ghdx_surveys_sub[, ihme_loc_id := sub]

  ghdx_surveys <- rbind(ghdx_surveys, ghdx_surveys_sub)

}

for (sub in locs[ihme_loc_id %like% "MEX" & level > 3]$ihme_loc_id) {

  ghdx_surveys_sub <- copy(ghdx_surveys[ihme_loc_id == "MEX" & survey_type %like% "ENADID"])
  ghdx_surveys_sub[, ihme_loc_id := sub]

  ghdx_surveys <- rbind(ghdx_surveys, ghdx_surveys_sub)

}

for (sub in locs[ihme_loc_id %like% "NGA" & level > 3]$ihme_loc_id) {

  ghdx_surveys_sub <- copy(ghdx_surveys[ihme_loc_id == "NGA" & survey_type %like% "MICS" & nid == 218613])
  ghdx_surveys_sub[, ihme_loc_id := sub]

  ghdx_surveys <- rbind(ghdx_surveys, ghdx_surveys_sub)

}

for (sub in locs[ihme_loc_id %like% "ZAF" & level > 3]$ihme_loc_id) {

  ghdx_surveys_sub <- copy(ghdx_surveys[ihme_loc_id == "ZAF" & survey_type %like% "OCT HH"])
  ghdx_surveys_sub[, ihme_loc_id := sub]

  ghdx_surveys <- rbind(ghdx_surveys, ghdx_surveys_sub)

}

agesex_data_nid <- merge(
  agesex_data,
  ghdx_surveys[!is.na(survey_type), !c("acceptance_status", "title")],
  by = c("ihme_loc_id", "survey_type", "survey_year"),
  all.x = TRUE
)

agesex_data_nid <- agesex_data_nid[!(source_new == "SEN_MICS_2015_2016" & is.na(nid))] # only one subnational

# add missing nids manually (not a standard survey_type)
agesex_data_nid[ihme_loc_id %like% "BWA" & source_new == "FHS 2007" & is.na(nid), nid := 22125]
agesex_data_nid[ihme_loc_id %like% "DOM" & source_new == "ENHOGAR" & is.na(nid), nid := 3455]
agesex_data_nid[ihme_loc_id %like% "ECU" & source_new == "ECU_ENSANUT_2012" & is.na(nid), nid := 153674]
agesex_data_nid[ihme_loc_id %like% "IDN" & source_new == "IFLS" & is.na(nid), nid := 6464]
agesex_data_nid[ihme_loc_id %like% "IDN" & source_new == "Population Census 2000" & is.na(nid), nid := 22674]
agesex_data_nid[ihme_loc_id %like% "IDN" & source_new == "SUPAS 2005" & is.na(nid), nid := 6547]
agesex_data_nid[ihme_loc_id %like% "IRQ" & source_new == "IRQ IMIRA" & is.na(nid), nid := 23565]
agesex_data_nid[ihme_loc_id %like% "IRQ" & source_new == "IRQ IFHS" & is.na(nid), nid := 23429]
agesex_data_nid[ihme_loc_id %like% "KGZ" & source_new == "LSMS 1998" & is.na(nid), nid := 45989]
agesex_data_nid[ihme_loc_id %like% "PAK" & source_new == "IHS 2001-2002" & is.na(nid), nid := 9720]
agesex_data_nid[ihme_loc_id %like% "PAK" & source_new == "PAK_IHS_1998-1999" & is.na(nid), nid := 9658]
agesex_data_nid[ihme_loc_id %like% "PER" & source_new == "PER_DEMOGRAPHIC_AND_FAMI_2015_2015" & is.na(nid), nid := 303663]
agesex_data_nid[ihme_loc_id %like% "PER" & source_new == "PER_DEMOGRAPHIC_AND_FAMI_2016_2016" & is.na(nid), nid := 303664]
agesex_data_nid[ihme_loc_id %like% "PHL" & source_new == "PHL_DHS_2011_2011" & is.na(nid), nid := 135803]
agesex_data_nid[ihme_loc_id %like% "SDN" & source_new == "DHS 1989-1990" & is.na(nid), nid := 20813]
agesex_data_nid[ihme_loc_id %like% "SDN" & source_new == "MICS 2010" & is.na(nid), nid := 153643]
agesex_data_nid[ihme_loc_id %like% "SDN" & source_new %in% c("SDN_ARAB_LEAGUE_PAPCHILD_1992_1993", "PAPCHILD 1993") & is.na(nid), nid := 12259]
agesex_data_nid[ihme_loc_id %like% "SDN" & source_new == "WFS 1978-1979" & is.na(nid), nid := 12256]
agesex_data_nid[ihme_loc_id %like% "SSD" & source_new == "MICS 2010" & is.na(nid), nid := 32189]
agesex_data_nid[ihme_loc_id %like% "TLS" & source_new == "DHS 1997" & is.na(nid), nid := 19999]
agesex_data_nid[ihme_loc_id %like% "TLS" & source_new == "East Timor 2003" & is.na(nid), nid := 20888]
agesex_data_nid[ihme_loc_id %like% "YEM" & source_new == "PAPCHILD 1991" & is.na(nid), nid := 21068]

# drop 3 rows where source may be wrong because no nid found
agesex_data_nid <- agesex_data_nid[!(ihme_loc_id == "MAR" & source_new == "PAPFAM 2004")]
agesex_data_nid <- agesex_data_nid[!(ihme_loc_id == "MOZ" & source_new == "CDC RHS 2001")]
agesex_data_nid <- agesex_data_nid[!(ihme_loc_id == "PER" & source_new == "DHS 2004-2008")]

# run missingness checks
agesex_data_nid_test <- unique(agesex_data_nid[, c("ihme_loc_id", "source", "source_new", "survey_type", "survey_year", "nid")])
agesex_data_nid_test <- demInternal::merge_ghdx_record_fields(agesex_data_nid_test)

assertable::assert_values(
  agesex_data_nid_test,
  colnames = "nid",
  test = "not_na"
)

# save file to review new nids
readr::write_csv(
  agesex_data_nid_test,
  fs::path("FILEPATH")
)

# deduplicate
agesex_nids_w_dups <- c(
  7028, 19464, 19571, 20567, 20584, 20595, 20674, 20699, 21301, 21421, 21433,
  30991, 63993, 76707, 77390, 77516, 157024, 174049, 218568
)
agesex_data_nid <- agesex_data_nid[
  !(nid %in% agesex_nids_w_dups & is.na(q_pna))
]
agesex_data_nid <- agesex_data_nid[
  !(nid == 19950 & ihme_loc_id %in% locs[level %in% 3:4]$ihme_loc_id & is.na(q_pna))
]
agesex_data_nid <- agesex_data_nid[ # values for 1997.667 make more sense than 1997.542
  !(nid == 157063 & year %like% 1997.5)
]

# split out enn and lnn
agesex_data_enn <- agesex_data_nid[, c("ihme_loc_id", "sex", "year", "nid", "q_enn", "exclude", "exclude_enn")]
agesex_data_enn <- agesex_data_enn[, `:=` (age_start = 0, age_end = 0.0191781, age_group_id = 2)]
setnames(agesex_data_enn, c("q_enn", "exclude", "exclude_enn"), c("qx", "outlier_id", "outlier_id_age_specific"))

agesex_data_lnn <- agesex_data_nid[, c("ihme_loc_id", "sex", "year", "nid", "q_lnn", "exclude", "exclude_lnn")]
agesex_data_lnn <- agesex_data_lnn[, `:=` (age_start = 0.0191781, age_end = 0.0767123, age_group_id = 3)]
setnames(agesex_data_lnn, c("q_lnn", "exclude", "exclude_lnn"), c("qx", "outlier_id", "outlier_id_age_specific"))

# combine enn and lnn
agesex_data <- rbind(agesex_data_enn, agesex_data_lnn)
agesex_data <- agesex_data[!is.na(qx)]

agesex_data[, year := floor(year)]

agesex_data[, sex := dplyr::case_when(
  sex == "male" ~ 1,
  sex == "female" ~ 2,
  sex == "both" ~ 3
)]

agesex_data[outlier_id != 0, outlier_id_age_specific := outlier_id]
agesex_data[, outlier_id := NULL]

setnames(agesex_data, "outlier_id_age_specific", "outlier")

# redo outliers from scratch
agesex_data[, outlier := 0]

agesex_data[, survey_year := max(year) + 2, by = c("ihme_loc_id", "nid")]
agesex_data[survey_year - year > 15, `:=` (outlier = 2, outlier_reason = "CBH before range")]
agesex_data[, survey_year := NULL]

agesex_data[nid == 8480, `:=` (outlier = 1, outlier_reason = "manual")] # MEX ENADID 1992
agesex_data[nid == 9658,`:=` (outlier = 1, outlier_reason = "manual")] # PAK PIHS 1998-99
agesex_data[nid == 9720, `:=` (outlier = 1, outlier_reason = "manual")] # PAK PIHS 2001-02
agesex_data[nid == 10326, `:=` (outlier = 1, outlier_reason = "manual")] # PRY CPS 1998
agesex_data[nid == 12259, `:=` (outlier = 1, outlier_reason = "manual")] # SDN MCHS 1992-93
agesex_data[nid == 12584, `:=` (outlier = 1, outlier_reason = "manual")] # TJK LSMS 2007
agesex_data[nid == 19001, `:=` (outlier = 1, outlier_reason = "manual")] # BOL DHS 2003-04
agesex_data[nid == 19999 & ihme_loc_id == "TLS", `:=` (outlier = 1, outlier_reason = "manual")] # IDN DHS 1997 (TLS only)
agesex_data[nid == 20092, `:=` (outlier = 1, outlier_reason = "manual")] # KAZ DHS 1995
agesex_data[nid == 20103, `:=` (outlier = 1, outlier_reason = "manual")] # KAZ DHS 1999
agesex_data[nid == 20596, `:=` (outlier = 1, outlier_reason = "manual")] # PSE DHS 2004
agesex_data[nid == 20813, `:=` (outlier = 1, outlier_reason = "manual")] # SDN DHS 1989-90
agesex_data[nid == 22125, `:=` (outlier = 1, outlier_reason = "manual")] # BWA FHS 2007-08
agesex_data[nid == 23565, `:=` (outlier = 1, outlier_reason = "manual")] # IRQ IMIRA 2004
agesex_data[nid == 44126, `:=` (outlier = 1, outlier_reason = "manual")] # ALB LSMS 2002
agesex_data[nid == 79839, `:=` (outlier = 1, outlier_reason = "manual")] # BEN DHS 2011-12
agesex_data[nid == 126952, `:=` (outlier = 1, outlier_reason = "manual")] # NIC ENDESA 2011-12
agesex_data[nid == 165908, `:=` (outlier = 1, outlier_reason = "manual")] # PAK LSMS 2008-09
agesex_data[nid == 188785, `:=` (outlier = 1, outlier_reason = "manual")] # BFA MIS 2014
agesex_data[nid == 218619, `:=` (outlier = 1, outlier_reason = "manual")] # SLE MICS 2017
agesex_data[nid == 218572, `:=` (outlier = 1, outlier_reason = "manual")] # GHA DHS SP 2017
agesex_data[nid == 218579, `:=` (outlier = 1, outlier_reason = "manual")] # KEN MIS 2015
agesex_data[nid == 218587, `:=` (outlier = 1, outlier_reason = "manual")] # MLI MIS 2015
agesex_data[nid == 218590, `:=` (outlier = 1, outlier_reason = "manual")] # NGA MIS 2015
agesex_data[nid == 234733, `:=` (outlier = 1, outlier_reason = "manual")] # COG MICS 2014-15
agesex_data[nid == 240604, `:=` (outlier = 1, outlier_reason = "manual")] # MEX ENADID 2014
agesex_data[nid == 303458, `:=` (outlier = 1, outlier_reason = "manual")] # GIN MICS 2016
agesex_data[nid == 555883, `:=` (outlier = 1, outlier_reason = "manual")] # MEX ENADID 2023

agesex_data_outlier_reasons <- as.data.table(table(agesex_data$outlier_reason))
setnames(agesex_data_outlier_reasons, "V1", "outlier_reason")
print(agesex_data_outlier_reasons)

# checks
assertable::assert_values(
  agesex_data,
  colnames = "sex",
  test = "in",
  test_val = 1:2
)

agesex_data <- merge(
  agesex_data,
  locs[, c("location_id", "ihme_loc_id")],
  by = "ihme_loc_id",
  all.x = TRUE
)
assertable::assert_values(
  agesex_data,
  colnames = "location_id",
  test = "not_na"
)

setnames(agesex_data, c("sex", "location_id"), c("sex_id", "location"))

demUtils::assert_is_unique_dt(
  agesex_data,
  c("nid", "location", "sex_id", "age_group_id", "year")
)

# calculate mx using ax values from GBD 2021
lt_ax <- demInternal::get_dem_outputs(
  "with shock life table estimate",
  wslt_version,
  gbd_year = 2021,
  age_group_ids = 2:3,
  life_table_parameter_ids = 2
)
agesex_data <- merge(
  agesex_data,
  lt_ax[, c("year_id", "location_id", "sex_id", "age_group_id", "mean")],
  by.x = c("year", "location", "sex_id", "age_group_id"),
  by.y = c("year_id", "location_id", "sex_id", "age_group_id"),
  all.x = TRUE
)
agesex_data[, mx := demCore::qx_ax_to_mx(qx, mean, age_end - age_start)]

# align dates
agesex_data[, date_start := paste0(as.character(year), "-01-01")]
agesex_data[, date_end := paste0(as.character(year + 1), "-01-01")]

# match up columns with CBH data
agesex_data[, `:=` (ihme_loc_id = NULL, year = NULL, mean = NULL)]

# calculate sample size by proportionately splitting the sample sizes from CBH's nn based on population
pop_enn <- pop[age_group_id == 2, c("location_id", "year_id", "sex_id", "population")]
pop_enn[, date_start := paste0(year_id, "-01-01")]
setnames(pop_enn, "population", "pop_enn")

pop_lnn <- pop[age_group_id == 3, c("location_id", "year_id", "sex_id", "population")]
pop_lnn[, date_start := paste0(year_id, "-01-01")]
setnames(pop_lnn, "population", "pop_lnn")

cbh_data_nn_ss <- unique(cbh_data_nn[
  dem_measure_id == 13 & location != 44858,
  c("location", "date_start", "sex_id", "nid", "value")
])
cbh_data_nn_ss[, location := as.numeric(location)]

cbh_data_nn_ss <- merge(
  cbh_data_nn_ss,
  pop_enn[, !c("year_id")],
  by.x = c("location", "date_start", "sex_id"),
  by.y = c("location_id", "date_start", "sex_id"),
  all.x = TRUE
)

cbh_data_nn_ss <- merge(
  cbh_data_nn_ss,
  pop_lnn[, !c("year_id")],
  by.x = c("location", "date_start", "sex_id"),
  by.y = c("location_id", "date_start", "sex_id"),
  all.x = TRUE
)

cbh_data_nn_ss[, sample_size_enn := value * (pop_enn / (pop_enn + pop_lnn))]
cbh_data_nn_ss[, sample_size_lnn := value * (pop_lnn / (pop_enn + pop_lnn))]

# average across years since year range doesn't always align for an nid between gbd and prep
cbh_data_nn_ss[, sample_size_enn_avg := mean(sample_size_enn), by = c("location", "sex_id", "nid")]
cbh_data_nn_ss[, sample_size_lnn_avg := mean(sample_size_lnn), by = c("location", "sex_id", "nid")]

# merge on sample size by nid
cbh_data_nn_ss_enn <- unique(
  cbh_data_nn_ss[, c("location", "sex_id", "nid", "sample_size_enn_avg")]
)
setnames(cbh_data_nn_ss_enn, "sample_size_enn_avg", "sample_size")
cbh_data_nn_ss_enn[, age_group_id := 2]

cbh_data_nn_ss_lnn <- unique(
  cbh_data_nn_ss[, c("location", "sex_id", "nid", "sample_size_lnn_avg")]
)
setnames(cbh_data_nn_ss_lnn, "sample_size_lnn_avg", "sample_size")
cbh_data_nn_ss_lnn[, age_group_id := 3]

cbh_data_enn_lnn_ss <- rbind(
  cbh_data_nn_ss_enn,
  cbh_data_nn_ss_lnn
)

agesex_data <- merge(
  agesex_data,
  cbh_data_enn_lnn_ss,
  by = c("location", "sex_id", "age_group_id", "nid"),
  all.x = TRUE
)

# merge by location if nid missing
agesex_data_ss <- agesex_data[!is.na(sample_size)]
agesex_data_no_ss <- agesex_data[is.na(sample_size), !c("sample_size")]

cbh_data_nn_ss[, sample_size_enn_avg_loc := mean(sample_size_enn), by = c("location", "sex_id")]
cbh_data_nn_ss[, sample_size_lnn_avg_loc := mean(sample_size_lnn), by = c("location", "sex_id")]

cbh_data_nn_ss_enn_loc <- unique(
  cbh_data_nn_ss[, c("location", "sex_id", "sample_size_enn_avg_loc")]
)
setnames(cbh_data_nn_ss_enn_loc, "sample_size_enn_avg_loc", "sample_size")
cbh_data_nn_ss_enn_loc[, age_group_id := 2]

cbh_data_nn_ss_lnn_loc <- unique(
  cbh_data_nn_ss[, c("location", "sex_id", "sample_size_lnn_avg_loc")]
)
setnames(cbh_data_nn_ss_lnn_loc, "sample_size_lnn_avg_loc", "sample_size")
cbh_data_nn_ss_lnn_loc[, age_group_id := 3]

cbh_data_enn_lnn_ss_loc <- rbind(
  cbh_data_nn_ss_enn_loc,
  cbh_data_nn_ss_lnn_loc
)

agesex_data_no_ss <- merge(
  agesex_data_no_ss,
  cbh_data_enn_lnn_ss_loc,
  by = c("location", "sex_id", "age_group_id"),
  all.x = TRUE
)

agesex_data <- rbind(
  agesex_data_ss,
  agesex_data_no_ss
)

# merge by global
agesex_data_ss <- agesex_data[!is.na(sample_size)]
agesex_data_no_ss <- agesex_data[is.na(sample_size), !c("sample_size")]

cbh_data_nn_ss[, sample_size_enn_avg_glob := mean(sample_size_enn), by = "sex_id"]
cbh_data_nn_ss[, sample_size_lnn_avg_glob := mean(sample_size_lnn), by = "sex_id"]

cbh_data_nn_ss_enn_glob <- unique(
  cbh_data_nn_ss[, c("sex_id", "sample_size_enn_avg_glob")]
)
setnames(cbh_data_nn_ss_enn_glob, "sample_size_enn_avg_glob", "sample_size")
cbh_data_nn_ss_enn_glob[, age_group_id := 2]

cbh_data_nn_ss_lnn_glob <- unique(
  cbh_data_nn_ss[, c("sex_id", "sample_size_lnn_avg_glob")]
)
setnames(cbh_data_nn_ss_lnn_glob, "sample_size_lnn_avg_glob", "sample_size")
cbh_data_nn_ss_lnn_glob[, age_group_id := 3]

cbh_data_enn_lnn_ss_glob <- rbind(
  cbh_data_nn_ss_enn_glob,
  cbh_data_nn_ss_lnn_glob
)

agesex_data_no_ss <- merge(
  agesex_data_no_ss,
  cbh_data_enn_lnn_ss_glob,
  by = c("sex_id", "age_group_id"),
  all.x = TRUE
)

agesex_data <- rbind(
  agesex_data_ss,
  agesex_data_no_ss
)

# add standard error
agesex_data[, mx_se := sqrt((ifelse(mx < 1, mx, 0.99) * (1 - ifelse(mx < 1, mx, 0.99))) / sample_size)]

# save intermediate file with outlier reasons
agesex_data <- agesex_data[order(nid, location, age_group_id, sex_id)]
readr::write_csv(
  agesex_data,
  fs::path("FILEPATH")
)

agesex_data[, outlier_reason := NULL]
agesex_data[, enn_lnn_source := "age-sex"]

# identify nids in enn/lnn that aren't in prep
missing_nids_from_prep <- unique(
  agesex_data[!(nid %in% cbh_data_wide$nid), "nid"]
)

missing_nids_from_prep <- demInternal::merge_ghdx_record_fields(
  missing_nids_from_prep,
  fields = "microdata_status"
)

readr::write_csv(
  missing_nids_from_prep,
  fs::path("FILEPATH")
)

# Produce final CBH output -----------------------------------------------------

cbh_data_wide <- rbind(
  cbh_data_wide,
  agesex_data,
  fill = TRUE
)

cbh_data_wide <- cbh_data_wide[sex_id %in% 1:2]

# add outliers
cbh_data_wide[is.na(outlier), outlier := 0]
cbh_data_wide[mx >= 1 & !(age_group_id %in% 2:3), outlier := 1]

cbh_data_wide[location == 49, outlier := 1] # MKD
cbh_data_wide[location == 53, outlier := 1] # SRB
cbh_data_wide[location == 63 & !(nid %in% reference_5q0[location_id == 63]$nid), outlier := 1] # UKR
cbh_data_wide[location %in% c(44934, 44939, 50559), outlier := 1] # UKR subs

cbh_data_wide[nid == 19533, outlier := 1] # SLV DHS 1985
cbh_data_wide[nid == 19999 & location == 19, outlier := 1] # IDN DHS 1997
cbh_data_wide[nid == 20326 & age_group_id %in% 2:3, outlier := 1] # MEX DHS 1987
cbh_data_wide[nid == 20552, outlier := 1] # NGA DHS 1990
cbh_data_wide[nid == 20813, outlier := 1] # SDN DHS 1989-90
cbh_data_wide[nid == 21442, outlier := 1] # CPV DHS 2005
cbh_data_wide[nid == 22125, outlier := 1] # BWA FHS 2007-08
cbh_data_wide[nid == 76850, outlier := 1] # COM DHS 2012-13
cbh_data_wide[nid == 264956, outlier := 1] # IDN IFLS5 2014-15
cbh_data_wide[nid == 400526, outlier := 1] # PAK (Punjab) MICS 2017-18
cbh_data_wide[nid == 479655, outlier := 1] # PAK (Sindh) MICS 2018-19
cbh_data_wide[nid == 494157, outlier := 1] # PAK (Balochistan) MICS 2019-20
cbh_data_wide[nid == 515231, outlier := 1] # PAK (Khyber Pakhtunkhwa) MICS 2019
cbh_data_wide[nid == 541281, outlier := 1] # COM MICS 2022

# outlier oldest age groups when not calculated
for (nid_temp in sort(unique(cbh_data_wide[age_group_id == 7]$nid))) {

  if (nrow(cbh_data_wide[nid == nid_temp & age_group_id == 7]) == nrow(cbh_data_wide[nid == nid_temp & age_group_id == 7 & mx == 0])) {

    print(paste0("Outliering 5-9 and 10-14 for nid: ", as.character(nid_temp)))
    cbh_data_wide[nid == nid_temp & age_group_id %in% 6:7, outlier := 3]

  }

}

# prioritize prep over age-sex
prep_loc_nids <- unique(cbh_data_wide[enn_lnn_source == "prep", c("location", "nid")])
agesex_loc_nids <- unique(cbh_data_wide[enn_lnn_source == "age-sex", c("location", "nid")])
matching_loc_nids <- agesex_loc_nids[prep_loc_nids, on = .(location, nid), nomatch = 0]
cbh_data_wide_agesex <- cbh_data_wide[enn_lnn_source == "age-sex"]
cbh_data_wide_agesex[matching_loc_nids, on = .(location, nid), outlier := 4]
cbh_data_wide[enn_lnn_source == "age-sex", outlier := cbh_data_wide_agesex$outlier]

# use enn/lnn from age-sex instead of prep for PHL subnats
cbh_data_wide[enn_lnn_source == "age-sex" & location %in% 53533:53614 & age_group_id %in% 2:3 & outlier == 4, outlier := 0]
cbh_data_wide[enn_lnn_source == "prep" & location %in% 53533:53614 & age_group_id %in% 2:3 & outlier == 0, outlier := 1]

# recalculate qx when qx is NA
cbh_data_wide[age_group_id %in% 2:3 & is.na(qx), qx := demCore::mx_to_qx(mx, age_length = age_end - age_start)]

# check for duplicates
assertable::assert_values(
  cbh_data_wide,
  colnames = colnames(cbh_data_wide[, !c("qx_se", "enn_lnn_source")]),
  test = "not_na"
)

# Save -------------------------------------------------------------------------

# compare mx to previous version
prev_version <-  fread(
  fs::path_norm(
    fs::path(
      "FILEPATH"
    )
  )
)
# adjust data types after reading in with data.table
prev_version[, ':=' (location = as.character(location), date_start = as.character(date_start))]

mx_comparison <- quick_mx_compare(
  test_file = cbh_data_wide,
  compare_file = prev_version,
  id_cols = c(
    "location", "nid", "sex_id", "age_group_id", "date_start", "enn_lnn_source",
    "outlier"
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
  cbh_data_wide,
  fs::path("FILEPATH")
)
