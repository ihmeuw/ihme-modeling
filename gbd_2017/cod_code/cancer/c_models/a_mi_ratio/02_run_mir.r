#!/usr/local/bin/R
#########################################
## Description: Loads functions to process MI model inputs with centralized, IHME ST-GPR
## Output(s): Outputs for each cause are saved by run_id in the IHME ST-GPR directory
#########################################


library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r')) 
source(get_path('mir_functions', process="mir_model"))
source(get_path('run_mir_worker', process="mir_model"))
## Load Required IHME Libraries
covariates_model_root <- get_path("covariates_model_root", process="cancer_model")
setwd(covariates_model_root) # required for IHME-shared function to work
print(
    paste("ALERT: your current working directory has been changed to", 
        covariates_model_root, "to enable use of the covariates team functions.")
)
source(file.path(covariates_model_root, 'init.r'))
library(ggplot2)



run_models <- function(mir_model_version_id=NULL, my_model_id_list=c(), 
                                            nDraws=0, cluster_is_busy=FALSE) {
    ## Runs submission function for each member of list
    ##
    if (is.null(mir_model_version_id)) stop("Must send mir_model_version_id")
    if (length(my_model_id_list)== 0) stop("Must send list of my_model_ids from config")
    for (my_model_id in my_model_id_list) {
        run_id <- run_mir_worker.register_model(mir_model_version_id,
                                                 my_model_id, nDraws)
        run_mir_worker.launch_model(run_id)
        ## Wait between models to prevent bottleneck      
        if (cluster_is_busy) {
            pause_time <- 30*60
            current_time <- Sys.time()
        } else pause_time = 60
        if (length(my_model_id_list) > 1) {
            print(paste("Pausing to prevent cluster bottleneck (", pause_time,"seconds)"))
            Sys.sleep(pause_time)   
        }
    }
    print("All models submitted.")
}


resubmit <- function(failed_run_ids) {
    ## Resubmits models for each of the passed failed_run_ids 
    ##
    print("Resubmitting runs...")
    for(rrid in failed_run_ids) {
        run_mir_worker.launch_model(rrid)
        if (length(failed_run_ids) > 1) {
            pause_time = 60
            print(paste("Pausing to prevent cluster bottleneck (", pause_time,"seconds)"))
            Sys.sleep(pause_time)   
        }
    }
}



if (!interactive()) {
    print("All functions loaded")
    mvid <- as.numeric(commandArgs(trailingOnly=TRUE)[1])
    run_models(mir_model_version_id = mvid, nDraws=0)
    mir.verify_success(mir_model_version_id = mvid)
}
