
# Description: Saves inputs for use in 45q15 modeling process

rm(list=ls())
library(data.table); library(assertable); library(DBI); library(readr); library(plyr); library(argparse); library(mortdb);

if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH"
  username <- Sys.getenv("USER")
} else {
  root <- "FILEPATH"
}

if (!interactive()) {
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
  parser$add_argument('--end_year', type="integer", required=TRUE,
                      help="last year we produce estimates for")
  parser$add_argument('--release_id', type="character", required=TRUE,
                      help='release_id for get_covariate_estimates')
  args <- parser$parse_args()
  list2env(args, .GlobalEnv)
  pop_run_id <- population_estimate_version
} else {
  version_id <-
  hiv_cdr_model_version_id <-
  edu_model_version_id <-
  ldi_model_version_id <-
  pop_run_id <-
  gbd_year <-
  end_year <-
  release_id <-
}

source("FILEPATH")

output_dir <- paste0("FILEPATH")

##########################
# Pull covariate estiamtes
##########################
lag_distributed_income_per_capita <- get_covariate_estimates(covariate_id = 57, release_id = release_id, model_version_id = ldi_model_version_id)
education_yrs_per_capita <- get_covariate_estimates(covariate_id = 33, release_id = release_id, model_version_id = edu_model_version_id)
hiv <- get_covariate_estimates(covariate_id = 5, release_id = release_id, model_version_id = hiv_cdr_model_version_id)

# Use new covid data
covid_inf_pc <- fread("FILEPATH")

covid_inf_pc <- covid_inf_pc[
  ,
  .(model_version_id = -999,
    covariate_id = -999,
    covariate_name_short = "covid_asdr",
    location_id,
    year_id,
    age_group_id,
    sex_id,
    mean_value = covid_asdr,
    lower_value = 0,
    upper_value = 0)
]

# LDI, COVID-INF, and HIV only go back to 1980
# fill out 0s for all years back to 1950, except LDI where
# keep the min. value.
covid_1950 <- expand.grid(
  model_version_id = -999,
  covariate_id = -999,
  covariate_name_short = "covid_asdr",
  location_id = unique(covid_inf_pc$location_id),
  year_id = 1950:1979,
  age_group_id = unique(covid_inf_pc$age_group_id),
  sex_id = unique(covid_inf_pc$sex_id),
  mean_value = 0,
  lower_value = 0,
  upper_value = 0
)

covid_inf_pc <- rbind(covid_inf_pc, covid_1950, fill = TRUE, use.names = TRUE)

hiv_1950 <- expand.grid(
  model_version_id = unique(hiv$model_version_id),
  covariate_id = unique(hiv$covariate_id),
  covariate_name_short = unique(hiv$covariate_name_short),
  location_id = unique(hiv$location_id),
  year_id = 1950:1979,
  age_group_id = unique(hiv$age_group_id),
  sex_id = unique(hiv$sex_id),
  mean_value = 0,
  lower_value = 0,
  upper_value = 0
)

hiv <- rbind(hiv, hiv_1950, fill = TRUE, use.names = TRUE)

fwrite(hiv, paste0("FILEPATH"))
fwrite(lag_distributed_income_per_capita, paste0("FILEPATH"))
fwrite(education_yrs_per_capita, paste0("FILEPATH"))
fwrite(covid_inf_pc, paste0("FILEPATH"))

###############
# Get locations
###############
codes <- get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year)
codes <- as.data.frame(codes)
old_ap <- codes[codes$ihme_loc_id == "IND_44849",]
fwrite(codes, paste0("FILEPATH"))

codes <- codes[codes$location_id == 44849 | codes$level_all == 1, ]
codes$region_name <- gsub(" ", "_", gsub(" / ", "_", gsub(", ", "_", gsub("-", "_", codes$region_name))))
codes <- merge(codes, data.frame(sex=c("male", "female")), all=T)
codes <- codes[order(codes$region_name, codes$ihme_loc_id, codes$sex), ]

###########################
# Pull population estimates
###########################
year_ids <- c(1950:end_year)
pop_loc_ids <- unique(codes$location_id)
population <- get_mort_outputs("population", "estimate", run_id = pop_run_id, sex_ids = c(1,2), location_ids = pop_loc_ids, age_group_ids = c(8:16))
setnames(population, "mean", "population")
population[, c('ihme_loc_id', 'lower', 'upper', 'upload_population_estimate_id') := NULL]

# Assertions
assert_values(data = population, colnames = "population", test = "gt", test_val = 0)
pop_ids_to_check <- list(location_id = unique(codes[codes$location_id != 44849, "location_id"]), sex_id = c(1:2), year_id = 1950:end_year, age_group_id = c(8:16))
assert_ids(data = population, id_vars = pop_ids_to_check)

fwrite(population, paste0("FILEPATH"))

######################
# Linear model targets
######################
targets <- get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year)
gbd_standard <- get_locations(gbd_type = "standard_modeling", level = "all")
mortality_locations <- get_locations(gbd_type = "ap_old", level = "estimate")
national_parents <- mortality_locations[level == 4, parent_id]
standard_with_metadata <- mortality_locations[location_id %in% unique(c(gbd_standard$location_id, national_parents, 44533))]

targets[location_id %in% standard_with_metadata$location_id, primary := T]
targets[is.na(primary), primary := F]
targets[, secondary := T]

fwrite(targets, paste0("FILEPATH"))
