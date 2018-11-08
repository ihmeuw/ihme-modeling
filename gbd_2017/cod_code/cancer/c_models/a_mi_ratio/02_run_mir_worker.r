#!/usr/local/bin/R
#########################################
## Description: Holds functions used to submit a single MIR model to STGPR
#########################################


library(here) 
library(data.table)
if (!exists("code_repo"))  {
    code_repo <-  sub("*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r'))
source(get_path('mir_functions', process="mir_model"))




run_mir_worker.load_config <- function(mir_model_version_id, my_model_id_list=c()) {
    ## Creates a list of causes to be modeled based on the arguments sent
    ##
    config = mir.load_stgprConfig(mir_model_version_id)
    if (length(my_model_id_list) == 0) {
        me_list <- unique(as.character(config$me_name))
    } else {
        config <- config[config$my_model_id %in% my_model_id_list, ]
    }
    if (length(config) == 0) stop(paste("Error loading config for", my_model_id_list))
    config['holdouts'] = 0
    config['cluster_project'] = 'proj_cancer_prep'
    config['nparallel'] = 40
    config['slots'] = 5
    return(config)
}


get_model_root <- function(is_custom_prior){
    # Load model-specific STGPR code 
    if (is_custom_prior) {
        model_root <- get_path("mir_forecast_model_root", process="mir_model")
    } else {
        model_root <- get_path("covariates_model_root", process="cancer_model")
    }
    return(model_root)
}


run_mir_worker.register_model <- function(mir_model_version_id, my_model_id, nDraws) {
    ## Adds records to the STGPR database and cancer database, then returns the 
    ##      STGPR run_id for the model. Run once.
    ##
    this_config <- run_mir_worker.load_config(mir_model_version_id, 
                                                my_model_id_list=my_model_id)
    this_config$holdouts <- 0
    me <- as.character(this_config$me_name)
    print(paste0("Starting model for ", me,", my_model_id ", my_model_id, 
                    ", mir_model_version ", mir_model_version_id, "..."))
    model_root <- get_model_root(as.logical(this_config$custom_prior))
    setwd(model_root) # required for IHME-shared function to work
    source('init.r')
    source(file.path(model_root, 'register_data.r'))
    ## Set input data, running prior model if necessary
    cause <- gsub("_mi_ratio", "", me)
    mir_inputs <- get_path('mir_inputs', process="mir_model")
    data_path <- paste0(mir_inputs, '/model_version_', mir_model_version_id, 
                                                        '/', cause, '.csv')
    ### Run Model ###
    #  run register_data function and reload data_db to get data_id number 
    #       Note: function developed by the covariates team)
    print("registering input data...")
    data_id_number <- register_data(me_name = me, 
                                     path = data_path, 
                                     user_id=as.character(this_config$author_id), 
                                     notes=as.character(this_config$notes),
                                     is_best=this_config$is_best,
                                     bypass=TRUE) 

    # Load  config file to get model_ids and run_ids assigned 
    print("registering model...")
    source(file.path(model_root, 'init.r')) # re-source to avoid error
    config_path = mir.get_STGPRconfig_file(mir_model_version_id)
    registration <- register.config(path = config_path,
                                    my.model.id = my_model_id, 
                                    data.id = data_id_number)
    run_id <- registration$run_id
    model_id <- registration$model_id
    mir.add_runRecord(me_name=me, mir_model_version_id, my_model_id, model_id, 
                            run_id, nDraws, as.character(this_config$notes))
    return(run_id)
}


run_mir_worker.launch_model <- function(run_id){
    ## Launches a single STGPR model using the model_updat_id and my_model_id
    ##
    record <- mir.get_runRecords(run_id)
    this_config <- run_mir_worker.load_config(record$mir_model_version_id, 
                                                record$my_model_id)
    # create log folder for the run_id
    log_folder <- paste0(get_path("STGPR_log_folder", 
                                          process="mir_model"),
                                          '/run_id_', run_id)
    dir.create(log_folder, showWarnings=FALSE)
    #
    print("adding run records and launching...")
    model_root <- get_model_root(as.logical(this_config$custom_prior))
    source(file.path(model_root, 'init.r')) # re-source to avoid error
    mapply(submit.master, run_id, this_config$holdouts, record$nDraws, 
            this_config$cluster_project, this_config$nparallel, 
            this_config$slots, model_root, log_folder)  
    print("    model submitted")
}
