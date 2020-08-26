# Description: Saves inputs for use in 45q15 modeling process

rm(list=ls())
library(data.table); library(assertable); library(DBI); library(readr); library(plyr); library(argparse); library(mortdb, lib = "FILEPATH");

if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH"
  username <- Sys.getenv("USER")
} else {
  root <- "FILEPATH"
}

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The 45q15 version for this run')
parser$add_argument('--hiv_cdr_model_version_id', type="integer", required=TRUE,
                    help='The HIV CDR covariate model version id')
parser$add_argument('--edu_model_version_id', type="integer", required=TRUE,
                    help='Mean education covariate model version id')
parser$add_argument('--ldi_model_version_id', type="integer", required=TRUE,
                    help='LDI PC covariate model version id')
parser$add_argument('--population_estimate_version', type="integer", required=TRUE,
                    help='Population estimate version')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD Year')
parser$add_argument('--decomp_step', type="character", required=TRUE,
                    help='decomp_step for get_covariate_estimates')

args <- parser$parse_args()
version_id <- args$version_id
hiv_cdr_model_version_id <- args$hiv_cdr_model_version_id
edu_model_version_id <- args$edu_model_version_id
ldi_model_version_id <- args$ldi_model_version_id
pop_run_id <- args$population_estimate_version
gbd_year <- args$gbd_year
decomp_step <- "iterative"

gbd_round_id <- get_gbd_round(gbd_year)

if (gbd_year == 2019){
  source("FILEPATH/get_covariate_estimates.R")
} else {
  source("FILEPATH/get_covariate_estimates.R")
}
output_dir <- "FILEPATH"

##########################
# Pull covariate estiamtes
##########################
lag_distributed_income_per_capita <- get_covariate_estimates(covariate_id = 57, gbd_round_id = gbd_round_id, model_version_id = ldi_model_version_id, decomp_step = decomp_step)
education_yrs_per_capita <- get_covariate_estimates(covariate_id = 33, gbd_round_id = gbd_round_id, model_version_id = edu_model_version_id, decomp_step = decomp_step)
hiv <- get_covariate_estimates(covariate_id = 1196, gbd_round_id = gbd_round_id, model_version_id = hiv_cdr_model_version_id, decomp_step = decomp_step)

fwrite(hiv, paste0(output_dir, "/data/hiv.csv"))
fwrite(lag_distributed_income_per_capita, paste0(output_dir, "/data/ldi_pc.csv"))
fwrite(education_yrs_per_capita, paste0(output_dir, "/data/educ_yrs_pc.csv"))

###############
# Get locations
###############
codes <- get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year)
codes <- as.data.frame(codes)
old_ap <- codes[codes$ihme_loc_id == "IND_44849",]
fwrite(codes, paste0(output_dir, "/data/locations.csv"))

codes <- codes[codes$location_id == 44849 | codes$level_all == 1, ] # Eliminate those that are estimates but that are handled uniquely (ex: IND six minor territories)
codes$region_name <- gsub(" ", "_", gsub(" / ", "_", gsub(", ", "_", gsub("-", "_", codes$region_name))))
codes <- merge(codes, data.frame(sex=c("male", "female")), all=T)
codes <- codes[order(codes$region_name, codes$ihme_loc_id, codes$sex), ]

###########################
# Pull population estimates
###########################
year_ids <- c(1950:gbd_year)
pop_loc_ids <- unique(codes$location_id)
population <- get_mort_outputs("population", "estimate", run_id = pop_run_id, sex_ids = c(1,2), location_ids = pop_loc_ids, age_group_ids = c(8:16))
setnames(population, "mean", "population")
population[, c('ihme_loc_id', 'lower', 'upper', 'upload_population_estimate_id') := NULL]

# Assertions
assert_values(data = population, colnames = "population", test = "gt", test_val = 0)
pop_ids_to_check <- list(location_id = unique(codes[codes$location_id != 44849, "location_id"]), sex_id = c(1:2), year_id = 1950:gbd_year, age_group_id = c(8:16))
assert_ids(data = population, id_vars = pop_ids_to_check)

fwrite(population, paste0(output_dir, "/data/population.csv"))

######################
# Linear model targets
######################
if (gbd_year == 2019){
  targets <- get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year)
  gbd_standard <- get_locations(gbd_type = "standard_modeling", level = "all")
  mortality_locations <- get_locations(gbd_type = "ap_old", level = "estimate")
  national_parents <- mortality_locations[level == 4, parent_id]
  standard_with_metadata <- mortality_locations[location_id %in% unique(c(gbd_standard$location_id, national_parents, 44533))]

  targets[location_id %in% standard_with_metadata$location_id, primary := T]
  targets[is.na(primary), primary := F]
  targets[, secondary := T]
}
if (gbd_year == 2017) {
  locations <- as.data.table(get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year))
  locations_prev <- as.data.table(get_locations(level = "estimate", gbd_type = "ap_old", gbd_year = gbd_year - 1))
  locations_prev = locations_prev[!grepl("SAU_", ihme_loc_id)]
  locations_prev$primary <- TRUE

  # Current round's locations, these will be the secondary locations
  locations$secondary <- TRUE

  targets <- as.data.table(merge(locations, locations_prev, by = c("location_id", "ihme_loc_id"), all = T))
  targets <- targets[, list(location_id, ihme_loc_id, primary, secondary)]
  targets[is.na(primary), primary := FALSE]
}

fwrite(targets, paste0(output_dir, "/data/02_fit_prediction_model_targets.csv"))
