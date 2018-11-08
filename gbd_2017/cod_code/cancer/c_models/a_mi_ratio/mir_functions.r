#!/usr/local/bin/R
#########################################
## Description: Functions used in mi ratio modeling
#########################################
library(here)  
if (!exists("code_repo"))  {
    code_repo <-  sub("*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) {
        code_repo <- file.path(code_repo, 'cancer_estimation')
    }
}
source(file.path(code_repo, 'r_utils/utilities.r')) 
source(get_path('cdb_utils_r'))
source(get_path("r_test_utilities"))
require(rhdf5)
library(foreign)
library(data.table)



mir.replace_missing_ages <- function(expand_this) {
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
        if (missing_age %in% ages_present) stop("age is not missing")
        else if ((missing_age < 6) & (1 %in% ages_present)) return(1)
        else if (any(ages_present[missing_age < ages_present]) ){
                return(min(ages_present[missing_age < ages_present]))
        } else if (all(ages_present[missing_age < ages_present])) {
            return(max(ages_present))
        }
        else stop("age could not be identified")
    }
    expand_this <- as.data.table(expand_this)
    # ensure presence of acause variable
    no_cause_value = "no causes specified"
    if (!('acause' %in% names(expand_this))) {
        has_no_causes = TRUE
        expand_this$acause = no_cause_value
    } else has_no_causes = FALSE
    # generate a list of expected ages
    all_ages = c(seq(1,20), 0, seq(30, 32), 235)
    # replace missing ages for each cause
    all_new_data <- data.table()
    for (cause in sort(unique(expand_this$acause))){
        dt <- subset(expand_this, acause == cause,)
        dt[, age_group_id:=as.numeric(as.character(age_group_id))]
        # make vector of the ages that are either present or missing
        ages_present = unique(dt$age_group_id)
        ages_missing = all_ages[all_ages %ni% ages_present]
        # create data table to expand the rest of the dataa
        data_expander <- data.table(
                            'age_group_id'=as.numeric(
                                    lapply(ages_missing, get_replacement)),
                            'new_age'=ages_missing)
        data_expander[new_age ==0,new_age:=28]
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
                        subset(all_new_data, ,
                            !colnames(all_new_data) %in% 'modeled_age'))
    # remove acause column if no acauses present
    if (has_no_causes) {
        output_this <- subset(output_this, ,!colnames(output_this) %in% 'acause')
    }
    # return results
    output_this[,age_group_id := as.numeric(as.character(age_group_id))]
    return(output_this)
}


mir.add_runRecord <- function(me_name, mir_model_version_id, mmid, modelID, runID, 
                                                            nDraws, run_note) {
    ## adds run history to the existing history, linking run_ids to model_inputs  
    ##
    print("    updating run record.")
    this_round = as.integer(get_gbd_parameter("current_gbd_round"))
    this_run <- as.data.frame(cbind(me_name=me_name, gbd_round_id=this_round,
                            mir_model_version_id=mir_model_version_id,
                            my_model_id=mmid, stgpr_model_id=modelID, run_id=runID, 
                            nDraws=nDraws, notes=as.character(run_note), is_best=0, 
                            inserted_by=Sys.info()["user"], 
                            last_update_by=Sys.info()["user"],
                            last_update_action="inserted"))
    if (nrow(this_run) > 1) {
        print(this_run)
       stop("Error generating run information for history")
    }
    cdb.append_to_table("mir_model_run", this_run)
 }


## -------------------------------------------------- ##
## TEST FUNCTIONS
## -------------------------------------------------- ##

mir.test_load_stgprConfig <- function(testDf){
    ##
    ##
    testResults = list('duplicates check' = c(), 'columns missing values'=c())
    if (any(duplicated(testDf$my_model_id))) {
        testResults['duplicates check'] <- paste("duplicate my_model_id",
                                            testDf$my_model_id[
                                                duplicated(testDf$my_model_id)])
    }
    testResults['columns missing values'] <- test_utils.findMissingValues(testDf,
                                                                colnames(testDf))
    if (nrow(testDf) == 0) testResults['no data'] <- 'no data in file'
    test_utils.checkTestResults(testResults, "mir.load_stgprConfig")
}

mir.test_update_runRecords <- function(outputDF, inputDF){
    ##
    ##
    testResults = list('lost_rows' = c(), 'columns missing values'=c())
    if (nrow(outputDF) < nrow(inputDF)) testResults['lost rows'] = "somehow rows are being deleted"
    testResults['columns missing values'] <- test_utils.findMissingValues(outputDF, names(outputDF))
    test_utils.checkTestResults(testResults, "mir.add_runRecords")
}

mir.test_get_HAQValues <- function(locationDf){
    results = list('columns missing values' = c(), 
                    'missing columns'=c(), 
                    'duplicates check'=c())
    uidCols = c('location_id' ,'year_id')
    required_columns = c(uidCols, 'haq')
    results['duplicates check'] = test_utils.duplicateChecker(locationDf, uidCols)
    results['missing columns'] = test_utils.findMissingColumns(locationDf, required_columns)
    results['columns missing values'] = test_utils.findMissingValues(locationDf, required_columns)
    test_utils.checkTestResults(results, "get_HAQValues")
}


mir.test_load_mi_caps <- function(cap_map, required_cols){
    results = list('columns missing values' = c(), 'missing columns'=c(), 
                    'duplicates check'=c())
    results['duplicates check'] <- test_utils.duplicateChecker(cap_map, required_cols)
    results['missing columns'] <- test_utils.findMissingColumns(cap_map, required_cols)
    results['columns missing values'] <- test_utils.findMissingValues(cap_map, required_cols)
    test_utils.checkTestResults(results, "test_load_mi_caps")
}


