#!/usr/local/bin/R
#########################################
## Description: Loads function that downloads mortality model draws from a centralized location,
##                  then formats them for the cancer nonfatal pipeline
## Input(s): gbd cause, location_id
## Output(s): saves formatted draws for the cause and location id in the nonfatal workspace
## How To Use: intended for submission as a cluster job in the nonfatal pipeline (see "Run Functions" below),
##                  can also be sourced to retrieve results for a single cause-location_id
#########################################
## load Libraries

source(file.path(h, 'cancer_estimation/utilities.R'))
source(get_path('nonfatal_functions', process="nonfatal_model"))
source(get_path('get_draws', process="nonfatal_model"))
source(get_path('common_model_functions', process="cancer_model"))

##########################################
## Define Functions
##########################################
format_mortality <-function(mortality_cause, location_id) {
## Retrieves and formats mortality draws for the specified gbd cause and location_id, then
##     finalizes (thus saving) the results
    output_folder <- paste0(get_path("mortality_draws_output", process="nonfatal_model"), "/", mortality_cause)
    ensure_directory_presence(output_folder)
    output_file <- paste0(output_folder, "/", location_id, ".dta")
    df <- get_draws(gbd_id_field="cause_id", gbd_id=nonfatal_model.get_cause_id(mortality_cause), source="codem", measure_ids=1, location_ids=location_id, sex_ids=c(1, 2))
    useful_df <- subset(df, , c('cause_id', 'location_id', 'year_id', 'sex_id', 'age_group_id', paste0("draw_", seq(0,999) ) ) )
    setnames(useful_df, old = c("year_id", "sex_id"), new=c("year", "sex") )
    nonfatal_model.finalize_draws(useful_df, output_file=output_file)
}

##########################################
## Run Functions
##########################################
if (!interactive()) {
    acause <- commandArgs()[3]
    location_of_interest <- commandArgs()[4]
    format_mortality(acause, location_of_interest)
}
