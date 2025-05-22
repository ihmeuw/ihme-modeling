
## Meta ------------------------------------------------------------------------

# Description: Compile VR, CBH, SIBS, and Census_Survey datasets for gbd ages

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in, format, and compile VR, CBH, SIBS, & Census_Survey data
#   4. Save
#   5. Final cleaning and appending on of HDSS and covariate data
#   6. Save year-specific version

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

# Read in internal inputs ------------------------------------------------------

age_map_gbd <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))
locs_cp <- fread(fs::path("FILEPATH"))

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# read in formatted hdss data
hdss_data <- fread(fs::path("FILEPATH"))

# read in vr reliability sheet
vr_reliability <- fread(fs::path("FILEPATH"))

# Read in, format, and compile VR, CBH, SIBS, & Census_Survey data -------------

vr <- fread(fs::path("FILEPATH"))
cbh <- fread(fs::path("FILEPATH"))
sibs <- fread(fs::path("FILEPATH"))
census_survey <- fread(fs::path("FILEPATH"))

# add age_group_id
vr <- merge(
  vr,
  age_map_gbd,
  by = c("age_start", "age_end"),
  all.x = TRUE
)

# align dates
vr[, year_id := date_start]
cbh[, `:=` (date_start = as.numeric(substr(date_start, 1, 4)), date_end = as.numeric(substr(date_end, 1, 4)))]
cbh[, year_id := floor((date_start + date_end) / 2)]
sibs[, `:=` (date_start = as.numeric(substr(date_start, 1, 4)), date_end = as.numeric(substr(date_end, 1, 4)))]
sibs[, year_id := floor((date_start + date_end) / 2)]
census_survey[, year_id := date_start]

vr[, `:=` (date_start = NULL, date_end = NULL)]
cbh[, `:=` (date_start = NULL, date_end = NULL)]
sibs[, `:=` (date_start = NULL, date_end = NULL)]
census_survey[, `:=` (date_start = NULL, date_end = NULL)]

# combine datasets
vr[, source_type_name := ifelse(source_type == "Civil Registration", "VR", source_type)]
cbh[, source_type_name := "CBH"]
sibs[, source_type_name := "SIBS"]
census_survey[, source_type_name := ifelse(source_type == "Census", "Census", "Survey")]

handoff_1 <- rbind(
  vr[, !c("population", "title", "underlying_title")],
  cbh,
  sibs,
  census_survey,
  fill = TRUE
)
handoff_1[, source_type_name := as.character(source_type_name)]

handoff_1[, location_id := as.numeric(location)]

handoff_1 <- handoff_1[location_id %in% locs$location_id]

handoff_1 <- merge(
  handoff_1,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

# add population to all files
handoff_1 <- merge(
  handoff_1,
  pop[, c("location_id", "year_id", "sex_id", "age_group_id", "population")],
  by = c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)

# add titles to all files
handoff_1 <- demInternal::merge_ghdx_record_fields(handoff_1, nid_col = "underlying_nid")
setnames(handoff_1, "title", "underlying_title")
handoff_1 <- demInternal::merge_ghdx_record_fields(handoff_1)

# reorganize columns
handoff_1 <- handoff_1[, c(
  "location_id", "year_id", "sex_id", "age_group_id", "nid", "underlying_nid",
  "title", "underlying_title", "ihme_loc_id", "source_type_name", "age_start",
  "age_end", "deaths", "mx", "mx_semi_adj", "mx_adj", "mx_se", "mx_se_semi_adj",
  "mx_se_adj", "qx", "qx_semi_adj", "qx_adj", "qx_se", "qx_se_semi_adj",
  "qx_se_adj", "sample_size", "sample_size_semi_adj", "sample_size_adj",
  "population", "series_name", "source_type_id", "source_type",
  "enn_lnn_source", "rank", "extraction_source", "outlier", "outlier_note"
  )
]

assertable::assert_values(
  handoff_1,
  colnames = "year_id",
  test = "in",
  test_val = estimation_year_start:(as.numeric(format(Sys.Date(), "%Y")) - 1)
)

assertable::assert_values(
  handoff_1,
  colnames = "location_id",
  test = "in",
  test_val = locs_cp$location_id
)

assertable::assert_values(
  handoff_1,
  colnames = "sex_id",
  test = "in",
  test_val = 1:2
)

assertable::assert_values(
  handoff_1,
  colnames = "age_group_id",
  test = "in",
  test_val = age_map_gbd$age_group_id
)

assertable::assert_values(
  handoff_1[source_type_name %in% c("CBH", "SIBS")],
  colnames = "sample_size",
  test = "lte",
  test_val = handoff_1[source_type_name %in% c("CBH", "SIBS")]$population
)

# Save -------------------------------------------------------------------------

arrow::write_parquet(
  handoff_1,
  fs::path("FILEPATH")
)

# Comparison check -------------------------------------------------------------

prev_version <- setDT(
  arrow::read_parquet(
    fs::path_norm(
      fs::path(
        "FILEPATH"
      )
    )
  )
)

summarize_result_differences(
  old_handoff = prev_version,
  new_handoff = handoff_1,
  id_cols = c(
    "location_id", "year_id", "sex_id", "age_group_id", "age_start", "age_end",
    "nid", "underlying_nid", "ihme_loc_id", "source_type_name", "source_type_id",
    "rank", "enn_lnn_source"
  ),
  measure_cols = c(
    "deaths", "population", "sample_size", "mx", "mx_se", "outlier",
    "outlier_note"
  ),
  handoff_name = "age_pattern_handoff",
  old_run_id = comparison_h1,
  age_map = age_map_gbd,
  location_map = locs,
  digits = 6
)

# Final cleaning and appending on of HDSS and covariate data -------------------

# remove SIBS data w/o recall bias adjustment
handoff_1[source_type_name == "SIBS", sample_size := sample_size_adj]

handoff_1 <- handoff_1[,
  !c("mx_semi_adj", "mx_se_semi_adj", "qx_semi_adj", "qx_se_semi_adj", "sample_size_semi_adj", "sample_size_adj")
]

# reassign outlier values for cbh
handoff_1[outlier > 1, outlier := 1]

# remove VR columns that aren't needed for handoff
handoff_1 <- handoff_1[,
  !c("series_name", "rank", "source_type_id", "source_type", "outlier_note", "extraction_source")
]

# remove additional unneeded columns
handoff_1 <- handoff_1[, !c("ihme_loc_id", "age_start", "age_end", "enn_lnn_source")]

# final checks
assertable::assert_nrows(
  handoff_1[source_type_name != "SIBS" & !is.na(mx_adj)],
  target_nrows = 0
)

assertable::assert_values(
  handoff_1[source_type_name == "SIBS"],
  colnames = colnames(handoff_1[, !c("underlying_nid", "underlying_title", "deaths", "qx_se")]), # qx_se missing from age-sex
  test = "not_na"
)

assertable::assert_values(
  handoff_1[source_type_name != "SIBS", !c("mx_adj", "mx_se_adj", "qx_adj", "qx_se_adj")],
  colnames = colnames(handoff_1[, !c("underlying_nid", "underlying_title", "deaths", "qx_se")]), # qx_se missing from age-sex
  test = "not_na"
)

assertable::assert_values(
  handoff_1[source_type_name == "VR"],
  colnames = "deaths",
  test = "not_na"
)

# save separate files based on covariate availability
for (yy in c(1950, 1980)) {

  print(yy)

  handoff_1_yy <- handoff_1[year_id >= yy]

  # make a data table of all expected combinations of ids
  handoff_1_square <- CJ(
    location_id = locs_cp$location_id,
    sex_id = 1:2,
    year_id = yy:estimation_year_end,
    age_group_id = unique(age_map_gbd$age_group_id)
  )

  handoff_1_square <- merge(
    handoff_1_square,
    handoff_1_yy,
    by = c("location_id", "sex_id", "age_group_id", "year_id"),
    all = TRUE
  )

  # append on hdss data to handoff 1
  handoff_1_square <- rbind(handoff_1_square, hdss_data, fill = TRUE)

  # generate covariate files (if not already in filesystem)
  cov_path <- fs::path(main_dir, "inputs", paste0("covariates_", yy, ".RDS"))

  if (!file.exists(cov_path)) {
    generate_covariates(get(paste0("covariates_", yy)), yy, release_id)
  }

  cov_file <- readRDS(cov_path)

  # merge on covariates
  for (cov_dt in cov_file) {
    handoff_1_square <- merge_covariates(handoff_1_square, cov_dt, yy)
  }

  # merge on non-standard covid covariates
  covid_asdr <- fread(covariates_covid_asdr_path)
  covid_asdr[, age_group_id := NULL]

  handoff_1_square <- merge(
    handoff_1_square,
    covid_asdr,
    by = c("year_id", "location_id", "sex_id"),
    all.x = TRUE
  )

  covid_age_sex <- fread(covariates_covid_age_sex_path)

  handoff_1_square <- merge(
    handoff_1_square,
    covid_age_sex,
    by = c("year_id", "location_id", "age_group_id", "sex_id"),
    all.x = TRUE
  )

  # merge on hiv_asdr, island, and paf covariates
  hiv_asdr <- fread(covariates_hiv_asdr_path)

  handoff_1_square <- merge(
    handoff_1_square,
    hiv_asdr,
    by = c("year_id", "location_id", "sex_id"),
    all.x = TRUE
  )
  handoff_1_square[is.na(hiv_asdr), hiv_asdr := 0]

  island <- fread(covariates_island_path)

  handoff_1_square <- merge(
    handoff_1_square,
    island,
    by = "location_id",
    all.x = TRUE
  )

  paf <- fread(covariates_paf_path)

  handoff_1_square <- merge(
    handoff_1_square,
    paf,
    by = c("year_id", "location_id", "age_group_id", "sex_id"),
    all.x = TRUE
  )

  # add vr reliability indicator
  handoff_1_square[, vr_reliability_adj := ifelse(location_id %in% vr_reliability$location_id, 1, 0)]

  # change location_ids to unique ids for HDSS sites
  handoff_1_square[, location_id := ifelse(!is.na(location_id_new), as.numeric(location_id_new), location_id)]
  handoff_1_square[, location_id_new := NULL]

  assertable::assert_nrows(
    handoff_1_square[nid %in% hdss_data$nid],
    target_nrows = nrow(hdss_data)
  )

  # check max year
  if (max(handoff_1_square$year_id) != 2024) stop ("Dataset not square through 2024.")

  # check that all location-year-age-sex groups are present
  assertable::assert_ids(
    handoff_1_square[location_id %in% locs_cp$location_id],
    list(
      location_id = locs_cp$location_id,
      sex_id = 1:2,
      year_id = yy:estimation_year_end,
      age_group_id = unique(age_map_gbd$age_group_id)
    ),
    assert_dups = FALSE
  )

  # get a named numeric vector with the number of NA values in each column
  na_count_per_column <- colSums(is.na(handoff_1_square))
  print(na_count_per_column)

  # check the number of rows with death counts and number of total deaths
  handoff_1_square[!is.na(deaths)] |> nrow()
  handoff_1_square[, sum(deaths, na.rm = TRUE)]

  # Save year-specific version -------------------------------------------------

  arrow::write_parquet(
    handoff_1_square,
    fs::path("FILEPATH")
  )

}
