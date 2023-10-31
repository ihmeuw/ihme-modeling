################################################################################
## Description: Holds functions used to submit a single MIR model to STGPR
## Contents:
## Contributors: USERNAME
################################################################################
## clear workspace and load libraries
library(here)
library(data.table)

source('FILEPATH/utilities.r')
source(get_path('mir_functions', process="mir_model"))
central_root <- get_path("central_root", process="cancer_model")

run_mir_worker.load_config <- function(mir_model_version_id, model_index_id_list=c()) {
    ## Creates a list of causes to be modeled based on the arguments sent
    ##
    config = mir.load_stgprConfig(mir_model_version_id)
    if (length(model_index_id_list) == 0) {
        me_list <- unique(as.character(config$me_name))
    } else {
        config <- config[config$model_index_id == model_index_id_list, ]
    }
    if (length(config) == 0) stop(paste("Error loading config for", model_index_id_list))
    config['cluster_project'] = 'proj_cancer_prep'
    config['nparallel'] = 40
    config['slots'] = 5
    return(config)
}


get_model_root <- function(is_custom_prior){
    ## Load model-specific STGPR code
    ## 
    if (is_custom_prior) {
        model_root <- get_path("mir_forecast_model_root", process="mir_model")
    } else {
        model_root <- get_path("central_root", process="cancer_model")
    }
    return(model_root)
}


run_mir_worker.register_model <- function(mir_model_version_id, model_index_id, 
                                                                        nDraws) {
    ## Adds records to the STGPR database and cancer database, then returns the 
    ##      STGPR run_id for the model. Run once.
    ##
    this_config <- run_mir_worker.load_config(mir_model_version_id, 
                                                model_index_id_list=model_index_id)
    me <- as.character(this_config$me_name)
    print(paste0("Starting model for ", me,", model_index_id ", model_index_id, 
                    ", mir_model_version ", mir_model_version_id, "..."))
    source(file.path(central_root, '/r_functions/utilities/utility.r'))

    ## Run Model
    ## Load config file to get model_ids and run_ids assigned 
    print("registering model...")

    source(file.path(central_root, '/r_functions/registration/register.R'))
    config_path = mir.get_STGPRconfig_file(mir_model_version_id)
    config <- fread(config_path)

    # subset config to specific model index to run and save path and file
    spec_model_path <- paste0(get_path("stgpr_configs", process="mir_model"), 
                               "/model_version_", mir_model_version_id, "_ix_", 
                               model_index_id, ".csv")
    fwrite(this_config, spec_model_path)

    # split into multiple csvs
    run_id <- register_stgpr_model(path_to_config = spec_model_path,
                                   model_index_id = model_index_id)
    print(run_id)
    mir.add_runRecord(me_name=me, mir_model_version_id, model_index_id, 
                            run_id, nDraws, as.character(this_config$notes))
    return(run_id)
}


run_mir_worker.launch_model <- function(run_id){
    ## Launches a single STGPR model using the model_update_id and model_index_id
    ##
    record <- mir.load_run_records(run_id_list=c(run_id))
    this_config <- run_mir_worker.load_config(record$mir_model_version_id, 
                                                record$my_model_id)
    print(paste("Launching", this_config$me_name))
    
    # create log folder for the run_id
    log_folder <- paste0(get_path("STGPR_log_folder", 
                                          process="mir_model"),
                                          '/run_id_', run_id)
    print(log_folder)
    dir.create(log_folder, showWarnings=FALSE)
    #
    print("adding run records and launching...")
    
    source(file.path(central_root, '/r_functions/registration/sendoff.R'))
    mapply(stgpr_sendoff, run_id=run_id, project=this_config$cluster_project, 
            log_path=log_folder)  
    print("    model submitted")
}
