###########################################################
##                                                       ##
## Purpose: Upload stillbirth data to mortality database ##
##                                                       ##
###########################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(data.table)

library(mortcore)
library(mortdb)

user <- "USERNAME"

# Get settings
args <- commandArgs(trailingOnly = TRUE)
settings_dir <- args[1]

if (interactive()) {
  version_data <- "Run id"
  settings_dir <- "FILEPATH/run_settings.csv"
}

load(settings_dir)
list2env(settings, envir = environment())

# Load data
data <- fread(paste0(data_dir, version_data, "/FILEPATH/stillbirth_input_data.csv"))

# Rename variables
setnames(data, "sbr", "mean")
setnames(data, "total_births", "sample_size")

# fill in missing location ids
locations <- mortdb::get_locations(gbd_year = gbd_year)

data$location_id <- NULL

data <- merge(
  data,
  locations[, c("location_id", "ihme_loc_id")],
  by = "ihme_loc_id",
  all.x = TRUE
)

# add std_def_id
std_defs <- mortdb::get_mort_ids(type = "std_def")
setnames(data, "std_def", "std_def_short")

data <- merge(
  data,
  std_defs[, c("std_def_id", "std_def_short")],
  by = "std_def_short",
  all.x = TRUE
)

# add new variables
if (is.null(data$underlying_nid)) data[, underlying_nid := NA]

data[, dem_measure_id := 6] # sbr

source_types <- mortdb::get_mort_ids(type = "source_type")[, c("source_type_id", "type_short")]

data <- merge(
  data,
  source_types,
  by.x = "source_type",
  by.y = "type_short",
  all.x = TRUE
)

data[, blencowe := 0]

data[, outlier := 0]

# Generate counts tables

count_nid <- as.data.table(unique(data[, c("nid", "source_type")]))
source_type_by_nid <- as.data.table(table(count_nid$source_type))

colnames(source_type_by_nid) <- c("source_type", "nids")

readr::write_csv(
  source_type_by_nid,
  paste0(data_dir, version_data, "/FILEPATH/data_count_by_def_and_nid.csv")
)

data[, locyr := paste0(ihme_loc_id, " ", year_id)]

count_locyr <- as.data.table(unique(data[, c("locyr", "source_type")]))
source_type_by_locyr <- as.data.table(table(count_locyr$source_type))

colnames(source_type_by_locyr) <- c("source_type", "location_years")

readr::write_csv(
  source_type_by_locyr,
  paste0(data_dir, version_data, "/FILEPATH/data_count_by_def_and_locyr.csv")
)

# Produce final output file
data_final <- data[, c("year_id", "location_id", "sex_id", "source_type_id", "source",
                       "definition", "blencowe", "nid", "underlying_nid", "std_def_id",
                       "dem_measure_id", "mean", "lower", "upper", "sample_size", "outlier")]

data_final <- unique(data_final)

not_na_colnames <- c("year_id", "location_id", "sex_id", "source_type_id", "source",
                     "blencowe", "nid", "std_def_id", "dem_measure_id", "mean", "outlier")
assertable::assert_values(data_final, not_na_colnames, test = "not_na")

not_negative_colnames <- c("sbr")
assertable::assert_values(data_final, not_negative_colnames, test = "gte", test_val = 0)

assertable::assert_values(data_final, "age_group_id", test = "equal", test_val = 22)
assertable::assert_values(data_final, "blencowe", test = "in", test_val = c(0, 1))
assertable::assert_values(data_final, "dem_measure_id", test = "equal", test_val = 6)
assertable::assert_values(data_final, "sex_id", test = "equal", test_val = 3)
assertable::assert_values(data_final, "std_def_id", test = "not_equal", test_val = 10)

readr::write_csv(data_final, paste0(data_dir, version_data, "/FILEPATH/stillbirth_data_database.csv"))
