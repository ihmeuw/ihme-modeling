#!/usr/local/bin/R
#########################################
## Description: Loads function that retrieves mi_ratio model draws from a centralized location,
##                  then formats them for the cancer nonfatal pipeline
## Input(s): gbd cause, location_id
## Output(s): saves formatted draws for the cause and location id in the nonfatal workspace
## How To Use: intended for submission as a cluster job in the nonfatal pipeline (see "Run Functions" below),
##                  can also be sourced to retrieve results for a single cause-location_id
## Notes: An S4 object error on loading the HDF5 files often results when the connection is out of memory.
##                Try running rm(list=ls()) first, or restart R
##        Currently assumes that mi_ratio model was run in logit space
#########################################
## load Libraries

source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
source(get_path('nonfatal_functions', process="nonfatal_model"))
source(get_path('common_model_functions', process="cancer_model"))
for (pkg in c('car', 'foreign', 'plyr', 'reshape2', 'data.table')){
  if (!require(pkg,character.only=TRUE)) install.packages(pkg)
  library(pkg,character.only=TRUE)
}

##########################################
## Define Functions
##########################################
format_mi_draws <- function(this_cause, location_id) {
## Retrieves and formats mi_ratio model results for the indicated gbd cause and location_id, then
##       finalizes (thus saving) the results

    # Create maps of run_ids and upper_/lower_caps
    run_id_map <- cancer_model.load_mi_runID_map(model_input_number= model_input)
    run_id_map <- subset(run_id_map, run_id_map$acause == this_cause, )
    run_id <- unique(run_id_map$run_id)
    cap_maps <- cancer_model.load_mi_caps(model_input_numbers = run_id_map$model_input)
    upper_caps <- cap_maps$upper_caps
    lower_caps <- cap_maps$lower_caps

    # load data and add run information
    print('Loding and formatting data...')
    input_file <- paste0(get_path("STGPR_outputs", process="cancer_model"), '/', run_id,  '/draws_temp_1/', location_id, '.csv')
    input_dt <- fread(input_file)
    input_dt$run_id <- run_id
    input_dt$acause <- this_cause

    # convert age_group_id 30 back to 21 (group "21" converted to "30" to enable processing by ST-GPR)
    input_dt[age_group_id == 30, age_group_id := 21]
    input_dt$age_group_id <- as.numeric(as.character(input_dt$age_group_id))
    setnames(input_dt, old = c('sex_id','year_id'), new = c('sex','year'))

    # Use data from the smallest existing age group to fill-in any missing data
    full_dt <- cancer_model.replace_missing_ages(input_dt, age_set ='nonfatal')

    # Attach run information and caps
    print("Adjusting to fit caps")
    full_dt <- merge(full_dt, run_id_map, all=T)
    merge_dt <- merge(full_dt, upper_caps, by= c('model_input', 'age_group_id'), all.x=T)
    has_caps <- merge(merge_dt, lower_caps, by=c('model_input', 'age_group_id', 'acause'), all.x=T)

    # revert to cartesian space and adjust outputs by the caps, first ensuring
    #       that data are within the correct boundaries
    print("Calculating final mi and saving...")
    draw_cols = names(has_caps)[grepl('draw', names(has_caps))]
    draws <- subset(has_caps, ,draw_cols)
    caps <- subset(has_caps, ,c("uppder_cap", "lower_cap"))
    draws <- as.data.frame(draws)
    mi_draws <- as.data.table(sapply(draw_cols, revertAndAdjust, draws_columns = draws, cap_columns=caps))

    # save and output as csv
    setnames(mi_draws, colnames(mi_draws), paste0('mi_', seq(0,999)))
    final_data <- cbind(subset(has_caps, ,!colnames(has_caps) %in% draw_cols), mi_draws)
    setnames(final_data, 'acause', 'mi_cause_name')
    output_folder <- paste0(get_path("mi_draws_output", process="nonfatal_model"), "/", this_cause)
    archive_folder <- paste0(output_folder,"/_archive")
    dir.create(archive_folder, recursive=TRUE)
    write.dta(data = final_data, file = paste0(output_folder,"/", location_id, ".dta"))
    write.dta(data = final_data, file = paste0(archive_folder, "/", location_id, ".dta"))
}

revertAndAdjust <- function(col, draws_columns, cap_columns){
## adjusts the draws in the indicated column by the upper and lower caps, then
##     returns the correctd data frame
    this_data <- draws_columns[,col]
    this_data[this_data < 0] <- 0
    this_data[this_data > 1] <- 1
    this_data <-this_data*cap_columns$upper_cap
    this_data[this_data < cap_columns$lower_cap] <- cap_columns$lower_cap[this_data < cap_columns$lower_cap]
    return(this_data)
}

##########################################
## Run Functions
##########################################
if(!interactive()) {
  format_acause <- commandArgs()[3]
  location_of_interest <- commandArgs()[4]
  format_mi_draws(format_acause, location_of_interest)
}

################################################################################
## END
################################################################################
