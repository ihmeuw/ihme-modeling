# Upload estimates from the first stage, second stage, and final stage of the model

##### Set up Environment #####
rm(list=ls())

library(data.table); library(assertable); library(readr);
library(mortdb, lib = "FILEPATH")

version_id <- as.integer(commandArgs(trailingOnly = T)[1])
data_run_id <- as.integer(commandArgs(trailingOnly = T)[2])
output_dir <- as.character(commandArgs(trailingOnly = T)[3])
gbd_year <- as.integer(commandArgs(trailingOnly = T)[4])

##### Load Results #####

results <- fread("FILEPATH")

##### Format Results #####

# Merge on location_ids
locations <- as.data.table(get_locations(gbd_year = gbd_year))

results <- merge(results, locations[, c("ihme_loc_id", "location_id")], by = c("ihme_loc_id"))

setnames(results, c("med_stage1", "med_stage2", "mort_med", "mort_lower", "mort_upper"), c("mean1", "mean2", "mean3", "upper3", "lower3"))

results[,year_id := floor(year)]
results[sex == "female", sex_id := 2]
results[sex == "male", sex_id := 1]
results[,viz_year := year]
results[,age_group_id := 199]

results[, c("unscaled_mort", "sex", "ihme_loc_id", "year") := NULL]

results <- melt(results, id.vars = c("viz_year", "year_id", "sex_id", "age_group_id", "location_id"))

results[grepl("mean", variable), estimate_stage_id := substr(variable, 5, 5)]
results[!grepl("mean", variable), estimate_stage_id := 3]
results[grepl("mean", variable), variable := "mean"]
results[!grepl("mean", variable), variable := substr(variable, 1, 5)]

results <- dcast.data.table(results, estimate_stage_id + viz_year + year_id + sex_id + age_group_id + location_id ~ variable, value.name = "value")

results[,estimate_stage_id := as.integer(estimate_stage_id)]

id_vars <- list(location_id = unique(locations$location_id)[locations$location_id != 6], year_id = c(1950:gbd_year), sex_id = c(1:2), estimate_stage_id = c(1:3))
assert_ids(results, id_vars)
assert_values(results, c("mean"), "not_na")
assert_values(results[estimate_stage_id == 3], c("lower", "upper"), "not_na")

##### Save and Upload #####

write_csv(results, path = "FILEPATH")

upload_results(filepath = "FILEPATH", model_name = "45q15", model_type = "estimate", run_id = version_id)
