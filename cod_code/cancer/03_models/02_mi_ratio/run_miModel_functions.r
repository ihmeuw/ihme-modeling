#!/usr/local/bin/R
#########################################
## Description: Sets filepaths and determines relevant variables (from mi config files)
##                  that are then used to submit prepped M/I data for linear modeling and ST-GPR.
##                  Also facilites resubmission
## Input(s)/Output(s): see individual functions
## How To Use: must be sourced by the run_miModel script
## IMPORTANT NOTE: run this code on cluster-prod. Requires R version 3.3.1 or later
#########################################
source(get_path('common_model_functions', process="cancer_model"))
source(get_path('run_mi_test_functions', process="cancer_model"))
require('rhdf5')

################################################################################
### Define Get & Set Functions
################################################################################
run_miModel.set_submissionSpecs <- function(specList=list()) {
    if (length(specList) == 0 | identical(specList[1], character(0))){
        stop("ERROR: empty list sent to set_submissionSpecs")
    }
    run_miModel.submissionSpecs <<- specList
}

run_miModel.get_submissionSpecs <- function(){
    if (!exists("run_miModel.submissionSpecs")) print("ERROR: submissionSpecs must be set before using run_miModel functions")
    return(run_miModel.submissionSpecs)
}

run_miModel.set_logFolder <- function() {
    logDirectory <- paste0(get_path("cancer_logs"), '/ST-GPR')
    ensure_directory_presence(logDirectory)
    run_miModel.logFolder <<- logDirectory
}
run_miModel.get_logFolder <- function(){
    return(run_miModel.logFolder)
}

################################################################################
### Define other functions
################################################################################
run_miModel.run_models <- function(nDraws=1000, cause_list=c(), mm_id_list=c(), model_input_number=NULL, cluster_is_busy=FALSE) {
## launch models for each job in cause_list or my_model_id list (mm_id_list) ##
    specs <- run_miModel.get_submissionSpecs()

    if (!is.null(cause_list)& is.null(model_input_number)) stop("ERROR: model_input_number is required if a cause_list is specified.")


    config_file <- file.path(get_path("stgpr_config", process="cancer_model"))
    config <- read.csv(config_file)[,c('my_model_id', 'me_name', 'model_input', 'prior_model')]

    ## Create list of causes to be modeled by determining which causes have been prepped
    me_list <- ifelse(is.null(cause_list) | length(cause_list) == 0 , c('none'), paste0(cause_list, "_mi_ratio"))
    if (length(mm_id_list) != 0) {
    me_list <- c(me_list, as.character(unique(config[config$my_model_id %in% mm_id_list, 'me_name'])))
    }
    if (length(me_list[me_list != 'none']) == 0)  {
    cause_list <- cancer_model.get_fullCauseList()
    me_list <- paste0(cause_list, '_mi_ratio')
    }

    for (me in me_list) {
        if (me %in% c('none', '')) next
        print(paste0("Initiating model for ", me, "..."))

        ## load me-specific information from its most recent entry on the config
        this_me_info <- subset(config, !is.na(config$my_model_id) & !is.na(config$model_input) & config$me_name == me, c('my_model_id', 'me_name', 'model_input'))
        if (!is.null(mm_id_list) & any(mm_id_list %in% this_me_info$my_model_id)) {
            this_me_info <- subset(this_me_info, this_me_info$my_model_id %in% mm_id_list, )
        } else {
            this_me_info <- this_me_info[this_me_info$model_input == model_input_number,]
            this_me_info <- this_me_info[!duplicated(this_me_info[,c('me_name')], fromLast=T),  ]  # use most recent entry
        }
        print(this_me_info)
        print(subset(config, config$my_model_id == this_me_info$my_model_id, 'prior_model'))
        if (nrow(this_me_info) > 1 ) stop('ERROR: error loading info from config. non-unique entries exist in the config file')
        if (nrow(this_me_info) == 0 ) stop(paste('ERROR: error loading info from config. no config established for model_input', , 'and cause', me))

        ## Set model_input specific notes and determine the input data location
        my_model_id_number <- this_me_info$my_model_id
        if (nrow(config[config$my_model_id == my_model_id_number & !is.na(config$my_model_id),]) != 1 ) {
            stop('ERROR: error loading info from config. the current my_model_id entry is used more than once')
        }
        this_model_input <- this_me_info$model_input
        cause <- gsub("_mi_ratio", "", me)
        mi_model_inputs <- get_path('mi_model_inputs', process="cancer_model")
        data_path <- paste0(mi_model_inputs, '/model_input_', this_model_input, '/', cause, '.csv')

        ### Run Model ###
        ## Test to see if data have been registered to get data_id_number. If not, run register_data function and reload data_db to get data_id number
        data_id_number <- register_data(me_name = me, path = data_path, user_id=specs$username, notes=specs$notes, is_best=specs$mark_best, bypass=TRUE) # function developed by the covariates team

        ## Load your config file to get model_ids and run_ids assigned
        registration <- register.config(path = config_file, my.model.id = my_model_id_number, data.id = data_id_number) # function developed by the covariates team
        run_id <- registration$run_id
        model_id <- registration$model_id

        ## create log folder for the run_id
        this_log_folder <- paste0(run_miModel.get_logFolder(), '/run_id_', run_id)
        dir.create(this_log_folder, showWarnings=FALSE)

        ## Run entire pipeline for each new run_id
        covariates_model_root <- get_path("covariates_model_root", process="cancer_model")
        options(warn=-1)
        #mapply(submit.master, run_id, specs$holdouts, nDraws, specs$cluster_project, specs$nparallel, specs$slots, covariates_model_root, this_log_folder)  # function developed by the covariates team
        options(warn=-0)

        ## add run to run_map
        run_miModel.update_runID_map(me_name=me, model_input_number, model_id, run_id, notes=specs$notes)

        ## Wait between models if the cluster is busy
        if (cluster_is_busy) Sys.sleep(60)
    }
}

 run_miModel.update_runID_map <- function(me_name, modelInputNumber, modelID, runID, notes) {
## adds run history to the existing history, linking run_ids to model_inputs  ##
    run_record_location <- get_path("mi_run_map", process="cancer_model")
    run_record <- read.csv(run_record_location, stringsAsFactors=FALSE)
    run_record <- run_record[!is.na(run_record$me_name),]
    run_id_update <- rbind(run_record, cbind(me_name, model_input=modelInputNumber, model_id=modelID, run_id=runID, date=as.character(Sys.Date())), notes=notes)
    updated_run_record <- unique(run_id_update)
    run_miModel.test_update_runID_map(outputDF=updated_run_record, inputDF=run_record)
    write.csv(updated_run_record, run_record_location, row.names=FALSE)
    print(paste("run record successfully updated for run_id", runID))
 }

run_miModel.resubmit <- function(failed_run_ids, nDraws=1000, log_loc = run_miModel.get_logFolder() ) {
## resubmit failed run_ids (for use if models fail) ##
    for(rrid in failed_run_ids) {
        resub_log_folder <- paste0(log_loc, '/run_id_', rrid)
        covariates_model_root <- get_path("covariates_model_root", process="cancer_model")
        options(warn=-1)
        mapply(submit.master, rrid, specs$holdouts, nDraws, specs$cluster_project, specs$nparallel, specs$slots, covariates_model_root, resub_log_folder)  
        options(warn=-0)
    }
}

run_miModel.check_run_status <- function(run_id_number){
## checks if run_id is still being processed
    print(check_run(run_id_number))[[1]]
}

run_miModel.check_outputs <- function(run_id_list=c(), me_list=c(), model_input_number=NULL){
## check for model outputs ##
## Argument Notes: me_list (list of modelable entity names): leave empty for all
##                  model_input_number: leave empty for most recent
    print("Verifying presence of model outputs...")

    ## use the arguments that were passed to make a run_id list
    run_record_location <- get_path("mi_run_map", process="cancer_model")
    run_record <- cancer_model.load_mi_runID_map(model_input_number, keep_specific_runIDs=run_id_list)
    if (exists("me_list") & length(me_list) > 0) {
        run_record <- run_record[run_record$me_name %in% me_list]
    }

    ## Determine which outputs are present
    files_present = c()
    files_absent = c()
    for (i in run_id_list) {
        success <- run_miModel.check_run_output(i)
        if (success) {
            files_present <- c(files_present, i)
        } else files_absent <- c(files_absent, i)
    }

    ## Alert user
    if (length(files_absent) != 0) {
        print(paste('Found outputs for these run_ids:', toString(files_present)))
        print('ERROR! Not all files found. Missing outputs for the following run_ids:')
        print(run_record[run_record$run_id %in% files_absent, c('run_id', 'me_name')])
        stop("All outputs must be generated before proceeding.")
    } else {
        print(paste0("All outputs successfully found for model_input ", model_input_number))
    }
}

run_miModel.check_run_output <- function(run_id){
## verify that the output of a given run_id actually contains the final, raked data
    # check for the relevant file
    gpr_mean_file <- paste0(get_path("STGPR_outputs", process="cancer_model"), "/", run_id, "/temp_1.h5")
    if (!file.exists(gpr_mean_file)) return(FALSE)

    # verify presence of raked data
    this_data <- NULL
    try (this_data <-h5read(gpr_mean_file, 'raked'), silent=TRUE)
    if (is.null(this_data)) return(FALSE)
    else return(TRUE)
}

