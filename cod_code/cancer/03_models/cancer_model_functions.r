#!/usr/local/bin/R
#########################################
## Description: Loads functions common to the cancer modeling processes
## Input(s)/Output(s): see individual functions
## How To Use: intended to be sourced by a cancer model script
#########################################


source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
source(get_path("common_model_test_functions", process="cancer_model"))
library(data.table)

################################################################################
### Set and Get Functions
################################################################################
cancer_model.set_modeledCauseList <- function(cancer_model ="mi_model"){
## make a list of all cancer causes modeled for M/I
    cause_input <- read.csv(get_path("causes"), stringsAsFactors=FALSE)
    if (!(cancer_model %in% c("mi_model", "epi_model"))) {
        stop("ERROR: set_modeledCauseList can currently only make causes for 'mi_model' and 'epi_model'")
    }
    cancer_model.fullCauseList <<- as.character(cause_input[cause_input[cancer_model] == 1 , 'acause'])
}

cancer_model.get_modeledCauseList <- function(which_model = "mi_model", starting_with="neo_") {
## Returns a cause list for the selected model with optional subset
    if (!(exists("cancer_model.fullCauseList"))) cancer_model.set_modeledCauseList(which_model)
    cause_list <- cancer_model.fullCauseList
    cause_list = unique(cause_list[grepl(starting_with, cause_list)])
    return(cancer_model.fullCauseList)
}

cancer_model.get_me_map <- function() {
## Returns a list of modelable_entity names ##
    cause_list <- cancer_model.get_modeledCauseList()
    me_map <- data.frame(acause = cause_list, me_name = paste0(cause_list, '_mi_ratio') )
    return(me_map)
}

################################################################################
### Other Functions
################################################################################
cancer_model.load_mi_runID_map <- function(model_input_number=NULL, keep_specific_runIDs=c(), load_all=FALSE){
## create a map linking each cause with the run information specified
## if model_input_number is not specified, uses outputs from the most recent model_input_number
## keep_specific_runIDs accepts a list of runs that superceed the run_ids that would be selected otherwise
## set load_all to TRUE to load the full map (with redundancies)
    run_db <- read.csv(get_path("mi_run_map", process="cancer_model"), stringsAsFactors=FALSE)
    me_map <- cancer_model.get_me_map()
    cancer_model.test_load_runID_inputs(run_db, me_map, input_number=model_input_number, keep_specific_runIDs)

    if (load_all){
        run_id_map <- run_db
    } else {
        exception_run_ids <- run_db[run_db$run_id %in% keep_specific_runIDs,]
        if (!is.null(model_input_number))  {
            run_db <- run_db[run_db$model_input == model_input_number, ]
        }
        most_recent_runs  <- subset(run_db, !duplicated(run_db$me_name, fromLast=T), )
        recent_without_exceptions <- most_recent_runs[!(most_recent_runs$run_id %in% keep_specific_runIDs),]
        run_id_map <- rbind(exception_run_ids, recent_without_exceptions)
    }

    output_map <- merge(run_id_map, me_map, on='me_name', all.x =TRUE, all.y=FALSE)
    cancer_model.test_runID_map(output_map)
    return(output_map)
}

cancer_model.load_modeledLocationsMap <- function() {
## Returns a formatted dataframe of epi locations
    modeled_locations <- read.csv(get_path("modeled_locations"))
    location_info <- modeled_locations[
        + (modeled_locations$is_estimate == 1 &
        + !is.na(modeled_locations$country_id) &
        + modeled_locations$location_set_id == 9) |
        + modeled_locations$location_id %in% c(seq(4618,4626), 4636, 95)  ## add GBR locations that are useful or required for data_prep
        + !(modeled_locations$location_type %in% c("global", "superregion", "region")),
        c('location_id', 'location_name', 'country_id', 'parent_id', 'location_type')]
    location_info[location_type %in% c("admin0", "nonsovereign"), country_id := location_id]
    location_info[is.na(location_info$country_id), country_id:=parent_id]

    if (nrow(location_info) == 0)  stop("ERROR: problem creating location list. List is empty.")
    return(location_info)
}

cancer_model.replace_missing_ages <- function(expand_this, age_set='') {
## Replaces data for age groups that are missing from the age_set requested
## Inputs: Requires a data table or data frame with an age_group_id column. Accepts an optional age_set (to distinguish which age categories to add).
## Outputs: A data table with added age categories using either the data of youngest older neighbor or of the oldest neighbor (for ages greater than those present)
    print("     adding results for missing age groups...")

    get_replacement <- function(missing_age){
        ## specific to replace_missing_ages function.  returns the replacement age for the missing_age values passed.
        ##      requires variables within the replace_missing_ages scope
        if (missing_age %in% ages_present) stop("age is not missing")
        else if ((missing_age < 6) & (1 %in% ages_present)) return(1)
        else if (length(which(missing_age < ages_present)) > 0) return(min(ages_present[missing_age < ages_present]))
        else return(max(ages_present))
    }

    expand_this <- as.data.table(expand_this)

    ## ensure presence of acause variable
    no_cause_value = "no causes specified"
    if (!('acause' %in% names(expand_this))) {
        has_no_causes = TRUE
        expand_this$acause = no_cause_value
    } else has_no_causes = FALSE

    ## generate a list of expected ages
    ## NOTE: because the age groups are out of order, a 0 will stand-in for age_group_id 28 and be replaced after new data are added
    if (age_set %in% c('nonfatal', 'full')){
        all_ages = c(seq(1,21), 0, seq(30, 33), 235)
    } else {
        all_ages = c(1, seq(6,21))
    }

    ## replace missing ages for each cause (NOTE: may be faster with an apply statement)
    all_new_data <- data.table()
    for (cause in sort(unique(expand_this$acause))){
        dt <- subset(expand_this, expand_this$acause == cause,)

        # make vector of the ages that are either present or missing
        ages_present = unique(dt$age_group_id)
        ages_missing = all_ages[!(all_ages %in% ages_present)]

        # create data table to expand the rest of the dataa
        data_expander <- data.table('age_group_id'=as.numeric(lapply(ages_missing, get_replacement)),'new_age'=ages_missing)
        data_expander[new_age ==0, new_age:=28]

        # merge expander with the rest of the data
        expanded_data <- merge(data_expander, dt, by='age_group_id', all.x = TRUE, allow.cartesian=TRUE)
        setnames(expanded_data, old = c('age_group_id','new_age'), new = c('modeled_age','age_group_id'))

        # append to the rest of the new data
        all_new_data <- rbind(all_new_data, expanded_data)
    }

    ## add new data to the input data
    output_this <- rbind(expand_this, subset(all_new_data,,!colnames(all_new_data) %in% 'modeled_age'))

    ## remove acause column if no acauses present
    if (has_no_causes) output_this <- subset(output_this, ,!colnames(output_this) %in% 'acause')

    ## return results
    return(output_this)
}

cancer_model.load_mi_caps <- function(model_input_numbers=c()){
## Returns a named list containing formatted maps of upper and lower caps for the provided model_input numbers
  print('Loading maps for upper_cap and lower_cap...')
  ## upper_cap
  upper_cap_columns <- c('upper_cap', 'age_group_id', 'model_input')
  upper_cap_input <- fread(get_path("mi_ratio_upperCaps", process="cancer_model"), showProgress = FALSE)
  upper_caps <- subset(upper_cap_input, ,upper_cap_columns)
  upper_caps <- subset(upper_caps, (upper_caps$model_input %in% model_input_numbers), )
  cancer_model.test_load_mi_caps(upper_caps, upper_cap_columns)
  upper_caps <- cancer_model.replace_missing_ages(upper_caps, age_set ='nonfatal')

  ## lower_cap
  lower_cap_columns <- c('lower_cap', 'age_group_id', 'model_input', 'acause')
  lower_cap_input <- fread(get_path("mi_ratio_lowerCaps", process="cancer_model") , showProgress = FALSE)
  lower_caps <- subset(lower_cap_input, ,lower_cap_columns)
  lower_caps <- subset(lower_caps, (lower_caps$model_input %in% model_input_numbers),)
  cancer_model.test_load_mi_caps(lower_caps, lower_cap_columns)
  lower_caps <- cancer_model.replace_missing_ages(lower_caps, age_set ='nonfatal')

  return(list("upper_caps"=upper_caps, "lower_caps"=lower_caps))
}

