#!/usr/local/bin/R
#########################################
## Description: Loads functions common to the cancer modeling processes
## Input(s)/Output(s): see individual functions
## How To Use: intended to be sourced by a cancer model script
#########################################


source(file.path(h, 'cancer_estimation/r_utils/utilities.r'))  
library(data.table)
library(foreign)
source(get_path("r_test_utilities"))

################################################################################
### Get and Load Functions
################################################################################
cancer_model.get_mirInputSelection <- function(){
    ## Returns the model_input_id for the current input. If not set, first 
    ##      prompts the user to enter the desired model_input_id before returning
    ##
    if (!(exists("cancer_model.__modelInputSelection"))) {
        selected_input <- as.numeric(
            readline(prompt="Enter a model input ID from input config: ")
        )
        cancer_model.verify_miModel_inputSelection(selected_input)
        cancer_model.__modelInputSelection <<- selected_input
    }
    return(cancer_model.__modelInputSelection)
}

cancer_model.verify_miModel_inputSelection <- function(input_selection){
    ##
    ##
    if (!is.na(as.numeric(input_selection))) {
        not_a_number = FALSE
    } else not_a_number = TRUE
    input_config <- cancer_model.load_mi_InputConfig()
    possibleModelNumbers <- unique(input_config$model_input_id )
    if ((input_selection %in% possibleModelNumbers)) {
        not_in_selection_range = FALSE
    } else not_in_selection_range = TRUE
    if (not_a_number | not_in_selection_range) {
        stop(
        "ERROR: model number must be a present in the input_config.
          See the input_config for a list of possible model numbers.")
    }
}

cancer_model.load_modeledCauseList <- function(which_model = "mi_model", starting_with="neo_") {
    ## Returns a cause list for the selected model with optional subset
    ##
    if (!(exists("cancer_model.__fullCauseList"))) {
        # make a list of all cancer causes modeled for M/I
        cause_input <- read.csv(get_path("causes"), stringsAsFactors=FALSE)
        if (!(which_model %in% c("mi_model", "epi_model"))) {
            stop("ERROR: set_modeledCauseList can currently only make causes for 'mi_model' and 'epi_model'")
        }
        cancer_model.__fullCauseList <<- as.character(cause_input[cause_input[which_model] == 1 , 'acause'])
    } 
    cause_list <- cancer_model.__fullCauseList
    cause_list = unique(cause_list[grepl(starting_with, cause_list)])
    return(cause_list)
}

cancer_prep.load_country_ids <- function(){
    ## Loads and returns a table of country_ids by location_id
    modeled_locations <- read.csv(get_path("modeled_locations"))
    location_tags <- modeled_locations[
                        !(modeled_locations$location_type_id %in% c(1,6,7,10)),]
    location_tags <- location_tags[,c('location_id', 'country_id')]
    return(location_tags)
}

cancer_model.get_HAQValues <- function() {
    ## Combines relevant information from modeled_locations with haq values and 
    ##      quintiles 
    ##
    if (!exists("cancer_model.__HAQmap")) {
        print('Loading HAQ values...')
        mir_location_metadata <- get_path("mir_location_metadata", process="cancer_model")
        output_cols = c('location_id', 'country_id', 'year', 'haq', 'haq_quintile')
        location_metadata = tryCatch({
          source(file.path(get_path("shared_r_libraries") , "get_covariate_estimates.R"))
          ## set paths
          ensure_dir(mir_location_metadata)
          location_tags <- cancer_prep.load_country_ids()
          ## Attach haq to the modeled locations
          haq <- get_covariate_estimates(covariate_id=1099)
          haq <- merge(location_tags, haq, by = c('location_id'))
          qtile <- subset(haq,
                          (location_id==country_id & year_id==get_gbd_parameter("max_year")), 
                          c('country_id', 'mean_value'))
          qtile <- within(qtile, 
                          haq_quintile <- as.integer(
                            cut(qtile$mean_value, 
                                quantile(qtile$mean_value, probs = seq(0, 1, .2), type=4), 
                                include.lowest=TRUE )))
          setnames(haq, old=c('year_id', 'mean_value'), new=c('year', 'haq'))
          location_metadata <- merge(location_tags, haq)    
          location_metadata <- merge(location_metadata, qtile[,c('country_id', 'haq_quintile')]) 
          cancer_model.test_get_HAQValues(location_metadata)
          write.csv(location_metadata[,output_cols], 
                    file =  mir_location_metadata, 
                    row.names=FALSE)
          return(location_metadata)
        }, error = function(e) {
             read.csv(get_path("mir_location_metadata", process="cancer_model"))
        })
        cancer_model.__HAQmap <<- location_metadata[,output_cols]
    }
    return(cancer_model.__HAQmap)
}


cancer_model.get_runRecords <- function(run_id_list=c(), model_input_id=NULL){
    ## Creates a map linking each cause with the run information specified
    ##  - if a run_id_list is specified, loads data for the run_ids on the list 
    ##      and for no other run_ids
    ##  - if model_input_id is specified, uses the most recent run_id for each cause
    ##  - if model_input_id is not specified, uses the data marked as is_current_best
    ##
    # use the arguments that were passed to make a run_id list
    run_record_location <- get_path("mir_run_record", process="cancer_model")
    run_record <- read.csv(run_record_location)
    # subset run record by the arguments
    if ((length(run_id_list) == 0) & is.null(model_input_id)){
        model_input_id <- max(run_record$model_input_id)
    }
    if (length(run_id_list) > 0) {
        run_record <- run_record[run_record$run_id %in% run_id_list, ]
        model_input_id <- unique(run_record$model_input_id)
    }
    run_record <- run_record[run_record$model_input_id == model_input_id, ]
    run_record <- run_record[!rev(duplicated(rev(run_record$me_name))),] # take the most recent run of each entry
    # validate run record
    if (length(run_record) == 0) {
        stop("ERROR: run record has no entries that match the provided specifications")
    }
    return(run_record)
}


cancer_model.replace_missing_ages <- function(expand_this) {
    ## Replaces data for age groups that are missing from the age_set requested
    ## Inputs: Requires a data table or data frame with an age_group_id column. 
    ##      Accepts an optional age_set (to distinguish which age categories to 
    ##      add).
    ## Outputs: A data table with added age categories using either the data of 
    ##      youngest older neighbor or of the oldest neighbor (for ages greater 
    ##      than those present)
    ##
    print("     adding results for missing age groups...")

    get_replacement <- function(missing_age){
        # specific to replace_missing_ages function.  returns the replacement 
        #        age for the missing_age values passed.
        #    NOTE: requires variables within the replace_missing_ages scope
        if (missing_age %in% ages_present) stop("age is not missing")
        else if ((missing_age < 6) & (1 %in% ages_present)) return(1)
        else if (length(which(missing_age < ages_present)) > 0) {
                return(min(ages_present[missing_age < ages_present]))
        }
        else return(max(ages_present))
    }

    expand_this <- as.data.table(expand_this)

    # ensure presence of acause variable
    no_cause_value = "no causes specified"
    if (!('acause' %in% names(expand_this))) {
        has_no_causes = TRUE
        expand_this$acause = no_cause_value
    } else has_no_causes = FALSE

    # generate a list of expected ages
    # NOTE: because the age groups are out of order, a 0 will stand-in for 
    #       age_group_id 28 and be replaced after new data are added
    all_ages = c(seq(1,20), 0, seq(30, 32), 235)

    # replace missing ages for each cause
    all_new_data <- data.table()
    for (cause in sort(unique(expand_this$acause))){
        dt <- subset(expand_this, expand_this$acause == cause,)

        # make vector of the ages that are either present or missing
        ages_present = unique(dt$age_group_id)
        ages_missing = all_ages[!(all_ages %in% ages_present)]

        # create data table to expand the rest of the dataa
        data_expander <- data.table(
                'age_group_id'=as.numeric(lapply(ages_missing, get_replacement)),
                'new_age'=ages_missing)
        data_expander[new_age ==0, new_age:=28]

        # merge expander with the rest of the data
        expanded_data <- merge(data_expander, dt, 
                                by='age_group_id', 
                                all.x = TRUE, 
                                allow.cartesian=TRUE)
        setnames(expanded_data, 
                old = c('age_group_id','new_age'), 
                new = c('modeled_age','age_group_id'))

        # append to the rest of the new data
        all_new_data <- rbind(all_new_data, expanded_data)
    }

    # add new data to the input data
    output_this <- rbind(expand_this, 
                        subset(all_new_data,,!colnames(all_new_data) %in% 'modeled_age'))
    # remove acause column if no acauses present
    if (has_no_causes) {
        output_this <- subset(output_this, ,!colnames(output_this) %in% 'acause')
    }
    # return results
    return(output_this)
}

