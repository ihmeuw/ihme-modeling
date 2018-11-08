## Create HIV adjustment summary results
rm(list=ls())
library(data.table); library(assertable); library(readr); library(tidyr)

root <- "ROOT_FILEPATH"
user <- "USERNAME"

## Import libraries
library(mortdb)
source(paste0("/FILEPATH/", user, "/hiv_adjust/reckoning_diagnostics.R"))

## Setup data
hiv_version <- commandArgs(trailingOnly = T)[1]
gbd_year <- as.integer(commandArgs(trailingOnly = T)[2])
start_year <- as.integer(commandArgs(trailingOnly = T)[3])

years <- c(start_year:gbd_year)

master_dir <- paste0("/FILEPATH/hiv_adjust/", hiv_version)
input_dir <- paste0(master_dir, "/inputs")
results_dir <- paste0(master_dir,"/results")
out_dir <- paste0(root, "/FILEPATH/database_upload")

measure_type <- get_mort_ids("hiv_measure_type")
measure_type <- measure_type[1:2]
category_type <- get_mort_ids("hiv_category")
category_type <- category_type[1:2]

country_map <- fread(paste0(input_dir, "/ensemble_groups.csv"))

adjust_locs <- data.table(get_locations(level="countryplus", gbd_year=gbd_year))

hiv_map <- merge(country_map, category_type, by.x = "group", by.y = "category")[, list(location_id, hiv_category_id)]
assert_values(hiv_map, colnames(hiv_map), "not_na")

## Pull in results
run_adj_locs <- unique(adjust_locs[level == 3,location_id])

results <- assertable::import_files(folder = results_dir, filenames = paste0("results_", run_adj_locs, ".csv"))
results[, location_id := as.integer(location_id)]
assert_values(results, "location_id", "not_na")

## Format for output
results <- merge(results, measure_type, by = "measure_type")
results <- merge(results, adjust_locs[, list(location_id, ihme_loc_id)], by = "location_id")
results <- merge(results, hiv_map, by = "location_id", all.x=T)
id_vars <- list(location_id=unique(adjust_locs$location_id), measure_type=unique(results$measure_type), sex_id=c(1:3), year_id=years, age_group_id=c(1, 23, 24, 40))
assert_ids(results, id_vars = id_vars)

## Fill in multi-level countries with appropriate HIV designation
results[grepl("GBR_", ihme_loc_id), hiv_category_id := unique(results[ihme_loc_id == "GBR", hiv_category_id])]
results[grepl("IND_", ihme_loc_id), hiv_category_id := unique(results[ihme_loc_id == "IND", hiv_category_id])]
results[grepl("KEN_", ihme_loc_id), hiv_category_id := unique(results[ihme_loc_id == "KEN", hiv_category_id])]
results[ihme_loc_id == "CHN_44533", hiv_category_id := 4]

results[, run_id := NULL]

out_cols <-  c("year_id", "location_id", "sex_id", "age_group_id", 
               "hiv_category_id", "hiv_measure_type_id",
               "mean", "upper", "lower")
results <- results[, .SD, .SDcols = out_cols]

dump_diagnostic_file(results[is.na(mean) | is.na(lower) | is.na(upper),], hiv_version, "upload_hiv_adjust", "NAs_converted_to_0.csv")
results[is.na(mean), mean := 0]
results[is.na(lower), lower := 0]
results[is.na(upper), upper := 0]

write.csv(results, paste0(out_dir, "/hiv_adjustment_estimate_v", hiv_version, ".csv"), row.names = F)

upload_results(paste0(out_dir, "/hiv_adjustment_estimate_v", hiv_version, ".csv"),
              "hiv adjustment", "estimate",
              run_id = hiv_version)

write.csv(hiv_map, paste0(out_dir, "/lookup_hiv_location_category_map.csv"), row.names = F)
