# Upload estimates from the first stage, second stage, and final stage of the model

##### Set up Environment #####
rm(list=ls())

library(data.table); library(assertable); library(readr); library(argparse);
library(mortdb, lib = "FILEPATH")

# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id 45q15 model run')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help="GBD year")
parser$add_argument('--data_45q15_version', type="character", required=TRUE,
                    help="45q15 data version")
parser$add_argument('--ddm_estimate_version', type="character", required=TRUE,
                    help="DDM estimate version")
parser$add_argument('--estimate_5q0_version', type="character", required=FALSE,
                    help="5q0 estimate version")
parser$add_argument('--population_estimate_version', type="character", required=TRUE,
                    help="Population estimate version")
parser$add_argument('--mark_best', type="character", required=TRUE,
                    help="Mark run as best: True/False")

args <- parser$parse_args()
gbd_year <- args$gbd_year
version_id <- args$version_id
data_45q15_version <- args$data_45q15_version
ddm_estimate_version <- args$ddm_estimate_version
estimate_5q0_version <- args$estimate_5q0_version
population_estimate_version <- args$population_estimate_version
mark_best <- args$mark_best


output_dir <- "FILEPATH"

##### Load Results #####

results <- fread(paste0(output_dir, "/draws/estimated_45q15_noshocks_wcovariate.csv"))


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

# Archive outlier set
archive_outlier_set("45q15", version_id)

##### Save and Upload #####

fwrite(results, paste0(output_dir, "/data/45q15_estimate_upload.csv"))

upload_results(filepath = paste0(output_dir, "/data/45q15_estimate_upload.csv"), model_name = "45q15", model_type = "estimate", run_id = version_id)

if (mark_best) {
  update_status(model_name = "45q15",
                model_type = "estimate",
                run_id = version_id,
                new_status = "best", assert_parents=F, send_slack = F)
}