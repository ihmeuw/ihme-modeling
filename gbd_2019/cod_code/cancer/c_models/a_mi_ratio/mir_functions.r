
################################################################################
## Description: 
## How To Use: 
################################################################################
library(here)  
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r')) # Loads utilities functions, eg. get_path
source(get_path('cdb_utils_r'))
source(get_path("r_test_utilities"))
require(rhdf5)
library(foreign)
library(data.table)



mir.load_country_ids <- function(){
    ## Loads and returns a table of country_ids by location_id
    ##
    modeled_locations <- read.csv(get_path("modeled_locations"))
    location_tags <- modeled_locations[
                        !(modeled_locations$location_type_id %in% c(1,6,7,10)),]
    location_tags <- location_tags[,c('location_id', 'country_id')]
    return(location_tags)
}


mir.download_HAQ_estimates <- function(gbd_round_id){
    ##
    ##
    tryCatch({
        source(file.path(get_path("shared_r_libraries") , 
                        "get_covariate_estimates.R"))
        d_step = get_gbd_parameter('current_decomp_step')
        return(get_covariate_estimates(covariate_id=1099, gbd_round_id=gbd_round_id, decomp_step=d_step))
    }, error = function(e) {
        Sys.exit("Must be on cluster to refresh GBD HAQ estimates")
    })
}


mir.download_mir_run_table <-function(){
    return(cdb.get_table("mir_model_run"))
}


load_mir_input_config <- function() {
    ## Returns the configuration file for mi_model inputs
    ##
    inputConfig <- cdb.get_table("mir_model_version")
    return(inputConfig)
}


mir.get_inputConfig <- function(mir_model_version_id=NULL){
    ## Loads and returns the input_config for the current model input
    ##
   if (!exists("mir.__inputConfigDF")) {
        input_config <- load_mir_input_config()
        if (is.null(mir_model_version_id)) mir_model_version_id <- mir.get_mir_version_id()
        this_model_input = (input_config$mir_model_version_id == mir_model_version_id & 
                            !is.na(input_config$mir_model_version_id) )
        input_config <- input_config[this_model_input,]
        mir.__inputConfigDF <<- input_config
   }
   return(mir.__inputConfigDF)
}


mir.set_mir_version_id <- function(selected_input){
    ## Validate entry
    if (!is.na(as.numeric(selected_input))) {
        not_a_number = FALSE
    } else not_a_number = TRUE
    input_config <- load_mir_input_config()
    #
    not_in_selection_range = TRUE
    if ((selected_input %in% unique(input_config$mir_model_version_id))) {
        not_in_selection_range = FALSE
    }
    # Alert user if the wrong information was sent
    if (not_a_number | not_in_selection_range) {
        stop(
            "ERROR: model number must be present in the input_config.
            See the input_config for a list of possible model numbers.")
    }
    mir.__modelInputSelection <<- selected_input
    return(mir.__modelInputSelection)
}


mir.get_mir_version_id <- function(){
    ## Returns the mir_model_version_id for the current input. If not set, first 
    ##      prompts the user to enter the desired mir_model_version_id before returning
    ##
    return(mir.__modelInputSelection)
}


mir.get_bundle_info <- function(){
    ## Loads and returns the table of bundle information
    ##
     if (!exists("mir_prep.__bundleDF")) {
        bundle_info <- cdb.get_table("mir_model_entity")
        bundle_info <- subset(bundle_info, 
                            !is.null(mir_bundle_id) & is_active==1,
                            c('acause', 'mir_bundle_name', 'mir_bundle_id'))
        mir_prep.__bundleDF <<- bundle_info
    }
    return(mir_prep.__bundleDF)
}



mir.get_acauseList <- function(){
    ## Loads and returns a list of the causes with MIR models 
    ##
    if (!exists("mir.__acause_list")){
        mi_models <- mir.get_bundle_info()
        mir.__acause_list <<- sort(unique(mi_models$acause))
    }
    return(mir.__acause_list)
}


mir.get_STGPRconfig_file <- function(mir_model_version_id){
    return(paste0(get_path("stgpr_configs", process="mir_model"), 
                              "/model_version_", mir_model_version_id, ".csv"))
}


mir.load_stgprConfig <- function(mir_model_version_id=NULL) {
    ## Loads a clean version of the stgpr_config for querying
    ##
    config_file <- mir.get_STGPRconfig_file(mir_model_version_id)
    config <- read.csv(config_file)
    config <- subset(config, !is.na(model_index_id), )
    config$mir_model_version_id <- mir_model_version_id
    test_cols <- c('model_index_id', 'mir_model_version_id', 'me_name', 'stage_1_model_formula',
        'notes')
    mir.test_load_stgprConfig(config[,test_cols])
    return(config)
}


mir.get_HAQValues <- function(gbd_round_id) {
    ## Combines relevant information from modeled_locations with haq values and 
    ##      quintiles 
    ##
    if (!exists("mir.__haq_maps")) mir.__haq_maps <<- list()
    if ( is.null(mir.__haq_maps[[as.character(gbd_round_id)]]) ) {
        output_cols = c('location_id', 'country_id', 'year_id', 'haq', 'haq_quintile')
        haq_estimate <- mir.download_HAQ_estimates(gbd_round_id)
        location_tags <- mir.load_country_ids()
        haq <- merge(location_tags, haq_estimate, by = c('location_id'))
        qtile <- subset(haq,
                        (location_id==country_id & 
                            year_id==get_gbd_parameter("max_year")), 
                        c('country_id', 'mean_value'))
        qtile <- within(qtile, 
                        haq_quintile <- as.integer(
                            cut(qtile$mean_value, 
                                quantile(qtile$mean_value, probs = seq(0, 1, .2), type=4), 
                                include.lowest=TRUE )))
        setnames(haq, 'mean_value', 'haq')
        location_metadata <- merge(location_tags, haq)    
        location_metadata <- merge(location_metadata, qtile[,c('country_id', 'haq_quintile')]) 
        mir.test_get_HAQValues(location_metadata)
        mir.__haq_maps[[as.character(gbd_round_id)]] <<- location_metadata[,output_cols]
    }
    return(mir.__haq_maps[[as.character(gbd_round_id)]])
}


mir.verify_success <- function(mir_model_version_id=NULL, run_id_list=c(),
                                        prompt_toMarkBest=TRUE){
    ## check for model outputs ##
    ##
    print("Verifying model status of MIR models. This may take up to 5 minutes...")
    #load run record
    if (length(run_id_list) == 0 & is.null(mir_model_version_id)) {
        stop("One of run_id_list or mir_model_version_id must be specified")
    } else if (length(run_id_list) > 0 & !is.null(mir_model_version_id)) {
        stop("Error. Only one of run_id_list or mir_model_version_id may be specified")
    } else if (length(run_id_list) > 0) {
        run_record <- mir.load_run_records(run_id_list=run_id_list)
        mir_model_version_id <- unique(run_record$mir_model_version_id)
    } else {
        run_record <- mir.load_run_records(mir_model_version_id=mir_model_version_id) 
        run_id_list <- unique(run_record$run_id)
    } 
    # Determine which outputs are present
    completed_runs = run_record[run_record$is_successful_run == 1, 'run_id']
    incomplete_runs = run_id_list[run_id_list %ni% completed_runs]
    for (run_id in run_id_list) {
        success <- mir.check_run_output(run_id)
        if (success) {
            completed_runs <- c(completed_runs, run_id)
            incomplete_runs <- incomplete_runs[incomplete_runs != run_id]
            cdb.update_record(
                "mir_model_run", c("run_id"), c(run_id), "is_successful_run", 1)
            if (prompt_toMarkBest==TRUE) { 
                cdb.update_record(
                    "mir_model_run", c("run_id"), c(run_id), "is_best", 1) 
            }      
        } 
    }
    
    ##Alert user
    print(paste('Found outputs for these run_ids:', toString(completed_runs)))
    if (length(incomplete_runs) != 0) {
        print('ERROR! Not all files found. Missing outputs for the following run_ids:')
        print(incomplete_runs)
        stop("\nAll outputs must be generated before proceeding.")
    } 
}


mir.check_run_output <- function(run_id){
    ## verify that the model completed successfully by locating the indicator file
    model_complete_file <- paste0(get_path("STGPR_outputs", process="cancer_model"),
                                             "/", run_id, "/model_complete.csv")
    print(model_complete_file)
    return(file.exists(model_complete_file))
}


mir.load_modeledCauseList <- function(which_model = "mi_model", starting_with="neo_") {
    ## Returns a cause list for the selected model with optional subset
    ##
    if (!(exists("mir.__fullCauseList"))) {
        # make a list of all cancer causes modeled for M/I
        cause_input <- read.csv(get_path("causes"), stringsAsFactors=FALSE)
        if (!(which_model %in% c("mi_model", "epi_model"))) {
            stop("ERROR: set_modeledCauseList can currently only make causes for 'mi_model' and 'epi_model'")
        }
        mir.__fullCauseList <<- as.character(cause_input[cause_input[which_model] == 1 , 'acause'])
    } 
    cause_list <- mir.__fullCauseList
    cause_list = unique(cause_list[grepl(starting_with, cause_list)])
    return(cause_list)
}



mir.load_run_records <- function(run_id_list=c(), mir_model_version_id=NULL){
    ## Creates a map linking each cause with the run information specified
    ##  - if a run_id_list is specified, loads data for only those runs in the 
    ##      list. Otherwise, if mir_model_version_id is passed, loads the most
    ##      recent runs of each modelable entity for that model_version_id
    ## 
    # use the arguments that were passed to make a run_id list
    run_record <- mir.download_mir_run_table()
    run_record <- run_record[!is.null(run_record$run_id),]
    # subset run record by the arguments
    if (length(run_id_list) == 0 & is.null(mir_model_version_id)) {
        stop("One of run_id_list or mir_model_version_id must be specified")
    } else if (length(run_id_list) > 0 & !is.null(mir_model_version_id)) {
        stop("Error. Only one of run_id_list or mir_model_version_id may be specified")
    } else if (length(run_id_list) > 0) {
        run_record <- run_record[run_record$run_id %in% run_id_list, ]
    } else {
        # take the most recent run of each entry
        run_record <- run_record[
                        run_record$mir_model_version_id == mir_model_version_id,]
        run_record <- run_record[!rev(duplicated(rev(run_record$me_name))),] 
    }
    if (nrow(run_record) == 0) {
        stop(paste(
            "ERROR: run record has no entries that match these arguments:",
            "run_id_list =", run_id_list, 
            " mir_model_version_id =", mir_model_version_id))
    }
    return(run_record)
}


mir.load_bestMIR_runs <- function(mir_model_version_id=NULL) {
    ## Creates a map linking each cause with the run information specified
    ##
    if (is.null(mir_model_version_id)) stop("must supply valid mir_model_version_id")
    # load best runs for the model version
    run_record <- mir.download_mir_run_table()
    run_record <- run_record[run_record$mir_model_version_id == mir_model_version_id,]
    run_record <- run_record[run_record$is_best==1,] 
    # Add acause value
    run_record$acause <- gsub("_mi_ratio", "", run_record$me_name)
    # validate before returning
    if (nrow(run_record) == 0) {
        stop(paste(
            "No runs marked best for mir_model_version_id", mir_model_version_id))
    } else if (nrow(run_record) != length(unique(run_record$me_name))){
        stop(paste("One and only one run_id from this mir_model_version_id must",
         "be marked best. This is not currently the case"))
    } else if (nrow(run_record[duplicated(run_record$me_name),]) > 0){
        print("Multiple run_ids marked best for the following causes")
        stop(run_record[duplicated(run_record$acause),])
    }
    return(run_record)
}


mir.load_mi_caps <- function(type, mir_model_version_id=NULL, 
                                                        add_missing_ages=FALSE) {
    ## Returns a named list containing formatted maps of upper and lower caps 
    ##      for the provided model_input numbers
    ## -- Arguments
    ##         type : one of c("upper", "lower") 
    ##         mir_model_version_id
    ##      
    ##
    if (!is.numeric(mir_model_version_id)) stop("Must sent numeric mir_model_version_id")
    if (type == "upper") {
        caps_table = "mir_upper_cap"
        required_columns <- c('upper_cap', 'age_group_id', 
                                'mir_model_version_id')
    } else if (type == "lower") {
        caps_table = "mir_lower_cap"
        required_columns <- c('lower_cap', 'age_group_id', 
                                'mir_model_version_id', 'acause')
    }
    cap_map <- cdb.get_table(caps_table)
    version_id = mir_model_version_id
    cap_map <- subset(cap_map, 
                        mir_model_version_id==version_id,
                        required_columns)
    mir.test_load_mi_caps(cap_map, required_columns)
    if (add_missing_ages) cap_map <- mir.replace_missing_ages(cap_map)
    cap_map <- as.data.frame(cap_map)
    for (col in colnames(cap_map)){
        if (col == "acause") next
        cap_map[,col] <- as.numeric(as.character(cap_map[, col]))
    }    
    mir.test_load_mi_caps(cap_map, required_columns)
    # Result
    return(as.data.table(subset(cap_map,,required_columns)))
}


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
        #    NOTE: requires variables within the replace_missing_ages scope
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
    # NOTE: because the age groups are out of order, a 0 will stand-in for 
    #       age_group_id 28 and be replaced after new data are added
    all_ages = c(seq(1,20), 0, seq(30, 32), 235)
    # replace missing ages for each cause 
    all_new_data <- data.table()
    for (cause in sort(unique(expand_this$acause))){
        dt <- subset(expand_this, acause == cause,)
        dt[, age_group_id:=as.numeric(as.character(age_group_id))]
        # make vector of the ages that are either present or missing
        ages_present = unique(dt$age_group_id)
        ages_missing = all_ages[all_ages %ni% ages_present]
        # create data table to expand the rest of the data
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
                        subset(all_new_data,,!colnames(all_new_data) %in% 'modeled_age'))
    # remove acause column if no acauses present
    if (has_no_causes) {
        output_this <- subset(output_this, ,!colnames(output_this) %in% 'acause')
    }
    # return results
    output_this[,age_group_id := as.numeric(as.character(age_group_id))]
    return(output_this)
}


mir.add_runRecord <- function(me_name, mir_model_version_id, mmid, runID, 
                                                            nDraws, run_note) {
    ## adds run history to the existing history, linking run_ids to model_inputs  
    ##
    print("    updating run record.")
    this_round = as.integer(get_gbd_parameter("current_gbd_round"))
    this_run <- as.data.frame(cbind(me_name=me_name, gbd_round_id=this_round,
                            mir_model_version_id=mir_model_version_id,
                            my_model_id=mmid, run_id=runID, 
                            nDraws=nDraws, notes=as.character(run_note), is_best=0, 
                            inserted_by=Sys.info()["user"], 
                            last_update_by=Sys.info()["user"],
                            last_update_action="inserted"))
    if (nrow(this_run) > 1) {
       stop("Error generating run information for history")
    }
    cdb.append_to_table("mir_model_run", this_run)
 }


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


## -------------------------------------------------- ##
## TEST FUNCTIONS
## -------------------------------------------------- ##

mir.test_load_stgprConfig <- function(testDf){
    ##
    ##
    testResults = list('duplicates check' = c(), 'columns missing values'=c())
    if (any(duplicated(testDf$model_index_id))) {
        testResults['duplicates check'] <- paste("duplicate model_index_id",
                                            testDf$model_index_id[
                                                duplicated(testDf$model_index_id)])
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

