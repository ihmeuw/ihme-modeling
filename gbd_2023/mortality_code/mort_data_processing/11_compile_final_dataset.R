## Meta ------------------------------------------------------------------------

# Description: Combine final VR and final survey data

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and clean HHD data
#   5. Read in and combine final VR and survey data
#   6. Perform additional outliering
#   7. Run checks for missing data
#   8. Final checks and appending on of HDSS and covariate data
#   9. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)

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

age_map <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))
age_map_extended <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))
locs_cp <- fread(fs::path("FILEPATH"))

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# read in formatted hdss data
hdss_data <- fread(fs::path("FILEPATH"))

# read in ssa data inclusion sheet
ssa_data_inclusion <- fread(fs::path("FILEPATH"))
ssa_data_inclusion <- ssa_data_inclusion[
  ,
  .(location_id, nid, underlying_nid, source_type_name, updated_outlier, updated_notes)
]

# read in vr reliability sheet
vr_reliability <- fread(fs::path("FILEPATH"))

# Read in and clean HHD data ---------------------------------------------------

if (file.exists(fs::path("FILEPATH"))) {

  hhd_data_msca_wide <- fread(
    fs::path("FILEPATH")
  )

} else {

  # prep hhd data
  prep_data_onemod <- arrow::open_dataset(
    sources = fs::path("FILEPATH")
  )

  hhd_data_msca <- prep_data_onemod |>
    dplyr::filter(dem_measure_id %in% c(19, 20, 13)) |> # nmx, nqx, p-ys
    dplyr::filter(collection_method_id == 3) |> # hhd
    dplyr::filter(date_format != "years_preceding_collection") |>
    dplyr::filter(location_type == "location_id") |>
    dplyr::filter(sex_id %in% 1:2) |>
    dplyr::collect() |>
    data.table()

  hhd_data_msca <- hhd_data_msca[, -c("other", "other_type", "other_granularity", "reg_day_missing", "event")]

  hhd_data_msca <- merge(
    hhd_data_msca,
    age_map_extended,
    by = c("age_start", "age_end"),
    all.x = TRUE
  )

  hhd_data_msca <- hhd_data_msca[age_group_id %in% age_map_gbd$age_group_id] # subset to most detailed age groups to prevent overlap

  hhd_data_msca_mx <- hhd_data_msca[
    dem_measure_id == 19,
    c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value", "uncertainty_value")
  ]
  hhd_data_msca_qx <- hhd_data_msca[
    dem_measure_id == 20,
    c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value", "uncertainty_value")
  ]
  hhd_data_msca_pys <- hhd_data_msca[
    dem_measure_id == 13,
    c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end", "value")
  ]

  setnames(hhd_data_msca_mx, "value", "mx")
  setnames(hhd_data_msca_qx, "value", "qx")
  setnames(hhd_data_msca_pys, "value", "sample_size")

  setnames(hhd_data_msca_mx, "uncertainty_value", "mx_se")
  setnames(hhd_data_msca_qx, "uncertainty_value", "qx_se")

  hhd_data_msca_wide <- merge(
    hhd_data_msca_mx,
    hhd_data_msca_qx,
    by = c("age_group_id", "location", "nid", "sex_id", "age_start", "age_end", "date_start", "date_end"),
    all = TRUE
  )

  hhd_data_msca_wide <- merge(
    hhd_data_msca_wide,
    hhd_data_msca_pys,
    by = c("age_start", "age_end", "age_group_id", "location", "nid", "sex_id", "date_start", "date_end"),
    all = TRUE
  )

  hhd_data_msca_wide_dropped <- unique(
    hhd_data_msca_wide[is.na(mx) | is.na(qx) | is.na(sample_size)]
  )
  hhd_data_msca_wide_dropped <- hhd_data_msca_wide_dropped[order(nid, location, date_start, sex_id, age_start)]

  readr::write_csv(
    hhd_data_msca_wide_dropped,
    fs::path("FILEPATH")
  )

  hhd_data_msca_wide <- unique(
    hhd_data_msca_wide[!(is.na(mx) | is.na(qx) | is.na(sample_size))]
  )

  hhd_data_msca_wide[, location_id := as.numeric(location)]
  hhd_data_msca_wide <- hhd_data_msca_wide[location_id %in% locs$location_id]

  hhd_data_msca_wide[, source_type_name := "HHD"]

  hhd_data_msca_wide[, `:=` (date_start2 = as.numeric(substr(date_start, 1, 4)), date_end2 = as.numeric(substr(date_end, 1, 4)))]
  hhd_data_msca_wide[, year_id := floor((date_start2 + date_end2) / 2)]

  hhd_data_msca_wide[, outlier := ifelse(mx >= 1, 1, 0)]

  hhd_data_msca_wide <- merge(
    hhd_data_msca_wide,
    pop[, c("location_id", "year_id", "sex_id", "age_group_id", "population")],
    by = c("location_id", "year_id", "sex_id", "age_group_id"),
    all.x = TRUE
  )

  hhd_data_msca_wide[, onemod_process_type := "standard gbd age group data"]

  hhd_data_msca_wide <- hhd_data_msca_wide[,
    c("location_id", "year_id", "sex_id", "age_group_id", "nid", "source_type_name",
      "mx", "mx_se", "qx", "sample_size", "outlier", "population", "onemod_process_type")
  ]
  hhd_data_msca_wide[, sample_size_orig := sample_size]

  assertable::assert_values(
    hhd_data_msca_wide,
    colnames = colnames(hhd_data_msca_wide),
    test = "not_na"
  )

  readr::write_csv(
    hhd_data_msca_wide,
    fs::path("FILEPATH")
  )

}

# Read in and combine final VR, final survey, and HHD data ---------------------

# read in final VR file
vr_final <- fread(fs::path("FILEPATH"))

# HOTFIX: sort out if this should be carried through later
vr_final[, source_type_name := ifelse(source_type == "Civil Registration", "VR", source_type)]

vr_final <- vr_final[,
  !c("age_start", "age_end", "qx", "qx_adj", "series_name", "source_type_id",
     "source_type", "title", "underlying_title")
]

# reclassify WSM 2016 VR as a census
wsm_census <- vr_final[location_id == 27 & year_id == 2016]

wsm_census[, enn_lnn_source := NA]
wsm_census[, extraction_source := "noncod"]
wsm_census[, outlier := 1]
wsm_census[, outlier_note := "Manually outliered"]
wsm_census[, source_type := "Census"]
wsm_census[, source_type_id := 5]
wsm_census[, source_type_name := "Census"]

vr_final <- vr_final[!(location_id == 27 & year_id == 2016)]

# read in final survey file
survey_final <- fread(fs::path("FILEPATH"))
survey_final[, `:=` (age_start_orig = NULL, age_end_orig = NULL)]

# identify any hhd data already in handoff
hhd_data_msca_wide_meta <- unique(hhd_data_msca_wide[, c("location_id", "year_id", "nid")])

hhd_data_msca_wide_meta <- merge(
  hhd_data_msca_wide_meta,
  locs[, c("ihme_loc_id", "location_id")],
  by = "location_id",
  all.x = TRUE
)

hhd_data_msca_wide_meta <- demInternal::merge_ghdx_record_fields(
  hhd_data_msca_wide_meta,
  stop_if_invalid = FALSE
)

hhd_data_msca_wide_meta[nid %in% survey_final[source_type_name == "CBH" & onemod_process_type == "standard gbd age group data"]$nid, cbh := "x"]
hhd_data_msca_wide_meta[nid %in% survey_final[source_type_name == "CBH" & onemod_process_type == "age-sex split data"]$nid, cbh_split := "x"]
hhd_data_msca_wide_meta[nid %in% survey_final[source_type_name == "SBH" & onemod_process_type == "age-sex split data"]$nid, sbh_split := "x"]
hhd_data_msca_wide_meta[nid %in% survey_final[source_type_name == "SIBS" & onemod_process_type == "standard gbd age group data"]$nid, sibs := "x"]
hhd_data_msca_wide_meta[nid %in% survey_final[source_type_name == "SIBS" & onemod_process_type == "age-sex split data"]$nid, sibs_split := "x"]
hhd_data_msca_wide_meta[nid %in% survey_final[source_type_name %in% c("Census", "Survey") & onemod_process_type == "standard gbd age group data"]$nid, census_survey := "x"]
hhd_data_msca_wide_meta[nid %in% survey_final[source_type_name %in% c("Census", "Survey") & onemod_process_type == "age-sex split data"]$nid, census_survey_split := "x"]

hhd_data_msca_wide_meta[
  is.na(cbh) & is.na(cbh_split) & is.na(sbh_split) & is.na(sibs) & is.na(sibs_split) &
    is.na(census_survey) & is.na(census_survey_split),
  COMPLETELY_MISSING := "x"
]

readr::write_csv(
  hhd_data_msca_wide_meta,
  fs::path("FILEPATH")
)

# combine datasets
handoff_2 <- rbind(
  vr_final,
  survey_final,
  wsm_census,
  hhd_data_msca_wide[, !c("qx")],
  fill = TRUE
)

# ensure survey sample_size is >0
handoff_2[source_type_name %in% c("CBH", "SBH", "SIBS", "HHD") & sample_size <= 0, sample_size := 0.00001]

# outlier all HHD
handoff_2[source_type_name == "HHD", outlier := 1]

# Perform additional outliering ------------------------------------------------

# update outlier status based on SSA Data Inclusion Sheet
ssa_data_inclusion <- ssa_data_inclusion[source_type_name %in% c("Census", "Survey", "HHD")]

handoff_2 <- merge(
  handoff_2,
  ssa_data_inclusion,
  by = c("location_id", "nid", "underlying_nid", "source_type_name"),
  all.x = TRUE
)

handoff_2[!(is.na(updated_outlier)), outlier := updated_outlier]
handoff_2[!(is.na(updated_outlier)), outlier_note := updated_notes]
handoff_2[, c("updated_outlier", "updated_notes") := NULL]

# manual outliering for Census/Survey/HHD/VR
handoff_2[
  source_type_name == "Census" & location_id != 44533 & nid == 130052,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # CHN 1982 Census
handoff_2[
  source_type_name == "Survey" & location_id == 191 & year_id %in% 2006:2007,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # ZMB
handoff_2[
  source_type_name == "HHD" & location_id == 194 & year_id == 1995 & (sex_id == 2 | (sex_id == 1 & age_group_id == 235)),
  `:=` (outlier = 1, outlier_note = NA)
] # LSO 1996 Census (females only)
handoff_2[
  source_type_name == "VR" & location_id == 147 & year_id %in% 1972:1996,
  `:=` (outlier = 1, outlier_note = NA)
] # LBY
handoff_2[
  source_type_name == "VR" & nid == 550893 & age_group_id %in% c(2:3, 388:389, 238, 34),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # NMB
handoff_2[
  source_type_name == "VR" & nid == 546150 & age_group_id %in% c(2:3, 388:389, 238, 34),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # ZAF

# manual un-outliering for Census/Survey/HHD
handoff_2[source_type_name == "Census" & location_id == 20 & nid == 43571, `:=` (outlier = 0, outlier_note = NA)] # VNM 1989 Census
handoff_2[source_type_name == "Census" & location_id == 20 & nid == 43718, `:=` (outlier = 0, outlier_note = NA)] # VNM 1999 Census
handoff_2[source_type_name == "Census" & location_id == 27 & nid == 140967 & underlying_nid == 325189, `:=` (outlier = 0, outlier_note = NA)] # WSM 2016 Census
handoff_2[source_type_name == "Census" & location_id == 121 & nid == 53452 & underlying_nid == 312181, `:=` (outlier = 0, outlier_note = NA)] # BOL 1991 Census
handoff_2[source_type_name == "Census" & location_id == 121 & nid == 237659 & underlying_nid == 152786 & sex_id == 1, `:=` (outlier = 0, outlier_note = NA)] # BOL 2012 Census
handoff_2[source_type_name == "Census" & location_id == 162 & nid == 237659, `:=` (outlier = 0, outlier_note = NA)] # BTN 2005 Census
handoff_2[source_type_name == "Census" & location_id == 164 & nid == 41044, `:=` (outlier = 0, outlier_note = NA)] # NPL 2001 Census
handoff_2[source_type_name == "Census" & location_id == 164 & nid == 140967 & underlying_nid == 153156, `:=` (outlier = 0, outlier_note = NA)] # NPL 2011 Census
handoff_2[source_type_name == "Census" & location_id == 164 & nid == 140967 & underlying_nid == 535050, `:=` (outlier = 0, outlier_note = NA)] # NPL 2021 Census
handoff_2[source_type_name == "Census" & location_id == 44533 & nid == 130052, `:=` (outlier = 0, outlier_note = NA)] # CHN 1982 Census

handoff_2[source_type_name == "Survey" & nid == 2918, `:=` (outlier = 0, outlier_note = NA)] # CHN 2015 1% Survey
handoff_2[source_type_name == "Survey" & location_id == 11 & nid == 6811, `:=` (outlier = 0, outlier_note = NA)] # IDN 2000 SUSENAS
handoff_2[source_type_name == "Survey" & location_id == 11 & nid == 6904, `:=` (outlier = 0, outlier_note = NA)] # IDN 2004 SUSENAS
handoff_2[source_type_name == "Survey" & location_id == 11 & nid == 6970, `:=` (outlier = 0, outlier_note = NA)] # IDN 2007 SUSENAS
handoff_2[
  source_type_name == "Survey" & location_id == 175 & nid == 1966 & sex_id == 2 & age_group_id != 8,
  `:=` (outlier = 0, outlier_note = NA)
] # BDI 1965 Demographic Survey (females only, but not 15-19)

handoff_2[source_type_name == "HHD" & location_id == 10 & nid == 417500, `:=` (outlier = 0, outlier_note = NA)] # KHM 2004 Pop Survey
handoff_2[source_type_name == "HHD" & location_id == 10 & nid == 417506, `:=` (outlier = 0, outlier_note = NA)] # KHM 2013 Pop Survey
handoff_2[source_type_name == "HHD" & location_id == 160 & nid == 56099 & age_group_id %in% c(6:20, 30), `:=` (outlier = 0, outlier_note = NA)] # AFG 2010 SDHS
handoff_2[source_type_name == "HHD" & location_id == 161 & nid == 18920, `:=` (outlier = 0, outlier_note = NA)] # BGD 2001 SDHS
handoff_2[source_type_name == "HHD" & location_id == 182 & nid == 455390, `:=` (outlier = 0, outlier_note = NA)] # MWI 2010-2019 IHPS

# add manual age-specific outliers for Census/Survey/HHD/VR
handoff_2[
  source_type_name %in% c("Census", "Survey", "HHD") & !(location_id == 44533 & year_id > 1990) & age_group_id %in% c(2:3, 388:389, 238, 34),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # Everywhere <5

handoff_2[
  source_type_name == "Census" & location_id == 27 & nid == 140967 & underlying_nid == 325189 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # WSM 2016 Census 80+
handoff_2[
  source_type_name == "Census" & location_id == 121 & nid == 53452 & underlying_nid == 312181 & age_group_id == 235,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BOL 1991 Census
handoff_2[
  source_type_name == "Census" & location_id == 121 & nid == 237659 & underlying_nid == 152786 & age_group_id == 235,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BOL 2012 Census
handoff_2[
  source_type_name == "Census" & location_id == 161 & nid == 56810 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BGD 2011 Census 80+
handoff_2[
  source_type_name == "Census" & location_id == 169 & nid == 140201 & age_group_id %in% c(2:3, 388:389, 238, 34, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # CAF 1988 Census <5 & 95+
handoff_2[
  source_type_name == "Census" & location_id == 176 & nid == 3115 & age_group_id %in% c(17:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # COM 1958 Census 60+
handoff_2[
  source_type_name == "Census" & location_id == 182 & nid == 40155 & age_group_id %in% c(2:3, 388:389, 238, 34, 6:8),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # MWI 1987 Census <20
handoff_2[
  source_type_name == "Census" & location_id == 184 & nid == 8891 & age_group_id %in% c(2:3, 388:389, 238, 34, 6:7, 16:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # MOZ 2007 Census <15 & 55+
handoff_2[
  source_type_name == "Census" & location_id == 185 & nid == 42432 & age_group_id %in% c(2:3, 388:389, 238, 34),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # RWA 2002 Census <5
handoff_2[
  source_type_name == "Census" & location_id == 189 & nid ==  43207 & age_group_id %in% c(2:3, 388:389, 238, 34, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # TZA 1988 Census <5 & 95+
handoff_2[
  source_type_name == "Census" & location_id == 193 & nid == 314123 & age_group_id %in% c(19:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BWA 2001 Census 70+
handoff_2[
  source_type_name == "Census" & location_id == 195 & nid == 53065 & age_group_id %in% c(16:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # NAM 2001 Census 55+
handoff_2[
  source_type_name == "Census" & location_id == 195 & nid == 134132 & age_group_id %in% c(18:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # NAM 2011 Census 65+
handoff_2[
  source_type_name == "Census" & location_id == 197 & nid == 52741 & underlying_nid == 12326 & age_group_id %in% c(20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # SWZ 1997 Census 75+
handoff_2[
  source_type_name == "Census" & location_id == 197 & nid == 237659 & underlying_nid == 12298 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # SWZ 2007 Census 80+
handoff_2[
  source_type_name == "Census" & location_id == 198 & nid == 53065 & age_group_id %in% c(2:3, 388:389, 238, 34, 17:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # ZWE 2002 Census <5 & 60+
handoff_2[
  source_type_name == "Census" & location_id == 198 & nid == 140967 & underlying_nid == 539878 & age_group_id %in% c(2:3, 388:389, 238, 34, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # ZWE 2022 Census <5 & 80+
handoff_2[
  source_type_name == "Census" & location_id == 201 & nid %in% c(105387, 1933, 1955) & age_group_id %in% c(2:3, 388:389, 238, 34),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BFA 1985, 1995, 2005 Censuses <5
handoff_2[
  source_type_name == "Census" & location_id == 201 & nid == 1955 & age_group_id %in% c(18:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BFA 2005 Census 65+
handoff_2[
  source_type_name == "Census" & location_id == 205 & nid == 57471 & age_group_id %in% c(2:3, 388:389, 238, 34),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # CIV 1998 Census <5
handoff_2[
  source_type_name == "Census" & location_id == 205 & nid == 140967 & age_group_id %in% c(2:3, 388:389, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # CIV 2021 Census <1 & 80+
handoff_2[
  source_type_name == "Census" & location_id == 207 & nid == 140967 & underlying_nid == 218222 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # GHA 2010 Census 80+
handoff_2[
  source_type_name == "Census" & location_id == 208 & nid == 140967 & age_group_id %in% c(31:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # GIN 2014 Census 85+
handoff_2[
  source_type_name == "Census" & location_id == 211 & nid == 140201 & underlying_nid == 24018 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # MLI 1976 Census 80+
handoff_2[
  source_type_name == "Census" & location_id == 211 & nid %in% c(40192, 40235) & age_group_id == 235,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # MLI 1987 & 1998 Censuses 95+
handoff_2[
  source_type_name == "Census" & location_id == 211 & nid == 237659 & underlying_nid == 34589 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # MLI 2009 Census 80+

handoff_2[
  source_type_name == "Survey" & location_id == 205 & nid == 126910 & age_group_id %in% c(31:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # CIV 1979 Demographic Survey 85+
handoff_2[
  source_type_name == "Survey" & location_id == 210 & nid == 53473 & age_group_id %in% c(16:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # LBR 1970-71 Pop Growth Survey 55+
handoff_2[
  source_type_name == "Survey" & location_id == 44533 & nid == 65578 & age_group_id %in% c(2:3, 288:389, 6),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # CHN 2003 SSPC <1 & 5-9

handoff_2[
  source_type_name == "HHD" & location_id == 10 & nid == 417500 & age_group_id %in% c(31:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # KHM 2004 Pop Survey
handoff_2[
  source_type_name == "HHD" & location_id == 10 & nid == 417506 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # KHM 2013 Pop Survey
handoff_2[
  source_type_name == "HHD" & location_id == 198 & nid == 487152 & age_group_id %in% c(6, 30),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # ZWE 2020 ZIMPHIA 5-9 & 80-84
handoff_2[
  source_type_name == "HHD" & location_id == 200 & nid == 367344 & age_group_id %in% c(15:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BEN 1992 Census 50+
handoff_2[
  source_type_name == "HHD" & location_id == 200 & nid == 367347 & age_group_id %in% c(15:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BEN 2002 Census 50+
handoff_2[
  source_type_name == "HHD" & location_id == 200 & nid == 367419 & age_group_id %in% c(15:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # BEN 2013 Census 50+
handoff_2[
  source_type_name == "HHD" & location_id == 202 & nid == 358376 & age_group_id %in% c(6, 17:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Age-specific outlier", paste0(outlier_note, "; Age-specific outlier")))
] # CMR 2017-18 CAMPHIA 5-9 & 60+

handoff_2[
  source_type_name == "VR" & location_id == 181,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # MDG VR
handoff_2[
  source_type_name == "VR" & location_id == 193 & age_group_id %in% c(2:3, 388:389, 238, 34, 31:32, 20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # BWA VR <5 & 75+
handoff_2[
  location_id == 203 & year_id %in% c(1955:1960, 1969:1975) & age_group_id %in% c(19:20, 30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # CPV VR 70+ (certain years)
handoff_2[
  location_id == 203 & year_id %in% 1966:1967,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # CPV VR all ages (certain years)
handoff_2[
  location_id == 203 & year_id == 1968 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # CPV VR 80+ (certain years)
handoff_2[
  location_id == 203 & year_id %in% 1983:1985 & age_group_id %in% c(31:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # CPV VR 85+ (certain years)

handoff_2[
  location_id == 215 & year_id %in% 1955:1957 & age_group_id %in% c(30:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # STP VR 80+ (certain years)
handoff_2[
  location_id == 215 & year_id == 1958,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # STP VR all ages (certain years)
handoff_2[
  location_id == 215 & year_id %in% c(1962:1971, 1979, 1984:1987) & age_group_id %in% c(31:32, 235),
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # STP VR 85+ (certain years)

handoff_2[
  location_id %in% c(196, 482:490) & age_group_id == 235,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # ZAF + subs 95+ (all data types)

handoff_2[
  location_id %in% c(44533, 491:521) & age_group_id == 235,
  `:=` (outlier = 1, outlier_note = ifelse(is.na(outlier_note), "Manual outlier", paste0(outlier_note, "; Manual outlier")))
] # CHN + subs 95+ (all data types)

# add official titles
handoff_2 <- demInternal::merge_ghdx_record_fields(
  handoff_2,
  nid_col = "underlying_nid",
  stop_if_invalid = FALSE
)
setnames(handoff_2, c("title", "status"), c("underlying_title", "underlying_status"))
handoff_2 <- demInternal::merge_ghdx_record_fields(
  handoff_2,
  nid_col = "nid",
  stop_if_invalid = FALSE
)

# additional HHD outliers
handoff_2 <- hhd_outliers(handoff_2)

# additional manual outliering
handoff_2[
  title %like% "October Household Survey" & outlier == 0,
  ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Manually outliered; "))
] # ZAF and subnationals

# remove status and underlying_status for now
handoff_2[, ':=' (status = NULL, underlying_status = NULL)]

# Run checks for missing data --------------------------------------------------

# check for missing 5q0 data
data_5q0 <- mortdb::get_mort_outputs(
  model_name = "5q0",
  model_type = "data",
  run_id = data_5q0_version,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  demographic_metadata = TRUE
)[outlier == 0]

child_nids <- unique(handoff_2[age_group_id %in% c(2, 3, 388, 389, 238, 34)]$nid)

data_5q0_missing <- data_5q0[!(nid %in% child_nids)]

handoff_2[source_type_name %in% c("VR", "SRS"), vr_loc_yr := paste0(location_id, "_", year_id)]
data_5q0_missing[source_name %in% c("VR", "SRS"), vr_loc_yr := paste0(location_id, "_", year_id)]

data_5q0_missing <- data_5q0_missing[!(!is.na(vr_loc_yr) & vr_loc_yr %in% unique(handoff_2$vr_loc_yr))]

data_5q0_missing <- demInternal::merge_ghdx_record_fields(
  data_5q0_missing,
  stop_if_invalid = FALSE
)

data_5q0_missing_sub <- unique(
  data_5q0_missing[, c("nid", "ihme_loc_id", "source_name", "method_name", "title")]
)

if (nrow(data_5q0_missing) > 0) {

  warning ("There's data in 5q0 that isn't included here. Please review.")

  readr::write_csv(
    data_5q0_missing_sub,
    fs::path("FILEPATH")
  )

}

# check for missing 45q15 data
data_45q15 <- mortdb::get_mort_outputs(
  model_name = "45q15",
  model_type = "data",
  run_id = data_45q15_version,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  demographic_metadata = TRUE
)[outlier == 0]

adult_nids <- unique(handoff_2[age_group_id %in% 8:16]$nid)

data_45q15_missing <- data_45q15[!(nid %in% adult_nids)]

data_45q15_missing[source_name %in% c("VR", "SRS"), vr_loc_yr := paste0(location_id, "_", year_id)]

data_45q15_missing <- data_45q15_missing[!(!is.na(vr_loc_yr) & vr_loc_yr %in% unique(handoff_2$vr_loc_yr))]
handoff_2[, vr_loc_yr := NULL]

data_45q15_missing <- demInternal::merge_ghdx_record_fields(
  data_45q15_missing,
  stop_if_invalid = FALSE
)

data_45q15_missing_sub <- unique(
  data_45q15_missing[, c("nid", "ihme_loc_id", "source_name", "method_name", "title")]
)

if (nrow(data_45q15_missing) > 0) {

  warning ("There's data in 45q15 that isn't included here. Please review.")

  readr::write_csv(
    data_45q15_missing,
    fs::path("FILEPATH")
  )

}

# Final checks and appending on of HDSS and covariate data ---------------------

# final checks
assertable::assert_values(
  handoff_2,
  colnames = "year_id",
  test = "in",
  test_val = 1950:2023
)

assertable::assert_values(
  handoff_2,
  colnames = "location_id",
  test = "in",
  test_val = locs_cp$location_id
)

assertable::assert_values(
  handoff_2,
  colnames = "sex_id",
  test = "in",
  test_val = 1:2
)

assertable::assert_values(
  handoff_2,
  colnames = "age_group_id",
  test = "in",
  test_val = age_map_gbd$age_group_id
)

assertable::assert_values(
  handoff_2,
  colnames = "onemod_process_type",
  test = "in",
  test_val = c("standard gbd age group data", "age-sex split data", "standard+split_data")
)

assertable::assert_values(
  handoff_2,
  colnames(
    handoff_2[,
      !c("nid", "underlying_nid", "underlying_title", "deaths", "deaths_adj",
         "mx_adj", "mx_se_adj", "completeness", "complete_vr", "source_type",
         "source_type_id", "enn_lnn_source", "extraction_source", "rank", "outlier_note")
    ]
  ),
  test = "not_na"
)

# Comparison check -------------------------------------------------------------

arrow::write_parquet(
  data.frame(handoff_2),
  fs::path("FILEPATH")
)

prev_version <- setDT(
  arrow::read_parquet(
    fs::path(
      "FILEPATH"
    )
  )
)

prev_version[, outlier2 := outlier]
handoff_2[, outlier2 := outlier]
summarize_result_differences(
  old_handoff = prev_version,
  new_handoff = handoff_2,
  id_cols = c(
    "location_id", "year_id", "sex_id", "age_group_id", "age_start", "age_end",
    "nid", "underlying_nid", "ihme_loc_id", "source_type_name", "source_type_id",
    "enn_lnn_source", "onemod_process_type", "outlier2", "extraction_source", "rank"
  ),
  measure_cols = c(
    "deaths", "population", "sample_size", "mx", "mx_se", "outlier",
    "outlier_note"
  ),
  handoff_name = "final_handoff",
  old_run_id = comparison_h2,
  age_map = age_map_gbd,
  location_map = locs,
  digits = 6
)
handoff_2[, ":=" (outlier2 = NULL, rank = NULL, extraction_source = NULL)]

# save
arrow::write_parquet(
  data.frame(handoff_2),
  fs::path("FILEPATH")
)

# handoff 1 and 2 outliering comparison
keep_cols <- c(
  "location_id", "year_id", "sex_id", "age_group_id", "nid",
  "underlying_nid", "source_type_name", "outlier",
  "mx", "mx_adj"
)

h1_outliering <- arrow::read_parquet(
  fs::path("FILEPATH")
)[, ..keep_cols]
h2_outliering <- handoff_2[onemod_process_type == "standard gbd age group data", ..keep_cols]

setnames(h1_outliering, c("outlier", "mx", "mx_adj"), c("h1_outlier", "h1_mx", "h1_mx_adj"))
setnames(h2_outliering, c("outlier", "mx", "mx_adj"), c("h2_outlier", "h2_mx", "h2_mx_adj"))

full_outliering <- merge(
  h1_outliering,
  h2_outliering,
  by = setdiff(keep_cols, c("outlier", "mx", "mx_adj"))
)

full_outliering <- unique(full_outliering)

full_outliering <- merge(
  age_map,
  full_outliering,
  by = "age_group_id",
  all.y = TRUE
)

full_outliering <- merge(
  locs[, c("location_name", "ihme_loc_id", "location_id")],
  full_outliering,
  by = "location_id",
  all.y = TRUE
)

readr::write_csv(
  full_outliering,
  fs::path("FILEPATH")
)

# save separate files based on covariate availability
for (yy in c(1950, 1980)) {

  print(yy)

  handoff_2_yy <- handoff_2[year_id >= yy]

  # make a data table of all expected combinations of ids
  handoff_2_square <- CJ(
    location_id = locs_cp$location_id,
    sex_id = 1:2,
    year_id = yy:estimation_year_end,
    age_group_id = unique(age_map_gbd$age_group_id)
  )

  handoff_2_square <- merge(
    handoff_2_square,
    handoff_2_yy,
    by = c("location_id", "sex_id", "age_group_id", "year_id"),
    all = TRUE
  )

  # append on hdss data to handoff 2
  handoff_2_square <- rbind(handoff_2_square, hdss_data, fill = TRUE)

  # generate covariate files (if not already in filesystem)
  cov_path <- fs::path("FILEPATH")

  if (!file.exists(cov_path)) {
    generate_covariates(get(paste0("covariates_", yy)), yy, release_id)
  }

  cov_file <- readRDS(cov_path)

  # merge on covariates
  for (cov_dt in cov_file) {
    handoff_2_square <- merge_covariates(handoff_2_square, cov_dt, yy)
  }

  # merge on non-standard covid covariates
  covid_asdr <- fread(covariates_covid_asdr_path)
  covid_asdr[, age_group_id := NULL]

  handoff_2_square <- merge(
    handoff_2_square,
    covid_asdr,
    by = c("year_id", "location_id", "sex_id"),
    all.x = TRUE
  )

  covid_age_sex <- fread(covariates_covid_age_sex_path)

  handoff_2_square <- merge(
    handoff_2_square,
    covid_age_sex,
    by = c("year_id", "location_id", "age_group_id", "sex_id"),
    all.x = TRUE
  )

  # merge on hiv_asdr, island, and paf covariates
  hiv_asdr <- fread(covariates_hiv_asdr_path)

  handoff_2_square <- merge(
    handoff_2_square,
    hiv_asdr,
    by = c("year_id", "location_id", "sex_id"),
    all.x = TRUE
  )
  handoff_2_square[is.na(hiv_asdr), hiv_asdr := 0]

  island <- fread(covariates_island_path)

  handoff_2_square <- merge(
    handoff_2_square,
    island,
    by = "location_id",
    all.x = TRUE
  )

  paf <- fread(covariates_paf_path)

  handoff_2_square <- merge(
    handoff_2_square,
    paf,
    by = c("year_id", "location_id", "age_group_id", "sex_id"),
    all.x = TRUE
  )

  # add vr reliability indicator
  handoff_2_square[, vr_reliability_adj := ifelse(location_id %in% vr_reliability$location_id, 1, 0)]

  # change location_ids to unique ids for HDSS sites
  handoff_2_square[, location_id := ifelse(!is.na(location_id_new), as.numeric(location_id_new), location_id)]
  handoff_2_square[, location_id_new := NULL]

  assertable::assert_nrows(
    handoff_2_square[nid %in% hdss_data$nid],
    target_nrows = nrow(hdss_data)
  )

  # check max year
  if (max(handoff_2_square$year_id) != 2024) stop ("Dataset not square through 2024.")

  # check that all location-year-age-sex groups are present
  assertable::assert_ids(
    handoff_2_square[location_id %in% locs_cp$location_id],
    list(
      location_id = locs_cp$location_id,
      sex_id = 1:2,
      year_id = yy:estimation_year_end,
      age_group_id = unique(age_map_gbd$age_group_id)
    ),
    assert_dups = FALSE
  )

  # get a named numeric vector with the number of NA values in each column
  na_count_per_column <- colSums(is.na(handoff_2_square))
  print(na_count_per_column)

  # check the number of rows with death counts and number of total deaths
  handoff_2_square[!is.na(deaths)] |> nrow()
  handoff_2_square[, sum(deaths, na.rm = TRUE)]

  # Save year-specific version -------------------------------------------------

  arrow::write_parquet(
    handoff_2_square,
    fs::path("FILEPATH")
  )

}
