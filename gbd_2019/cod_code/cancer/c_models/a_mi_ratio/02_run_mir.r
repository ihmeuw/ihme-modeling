################################################################################
## Description: Loads functions to process MI model inputs with centralized, IHME ST-GPR
## Input(s): See individual functions
## Output(s): Outputs for each cause are saved by run_id in the IHME ST-GPR directory
## How To Use: After sourcing the script...
##              Use run_models() to create new submissions for a model
##                      input. NOTE: (if you do not send a mir_model_version_id as an
##                      argument, you will be prompted for one by the function
##              Use mir.verify_success() to validate the most recent set
##                      of run_ids, or specify a list of run_ids. By default, you
##                      will be prompted to mark the models as 'best' if they all
##                      complete successfully
################################################################################
## clear workspace and load libraries
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r')) # Loads utilities functions, eg. get_path
source(get_path('mir_functions', process="mir_model"))
source(get_path('run_mir_worker', process="mir_model"))
source(get_path('stgpr_cluster_utils', process="cancer_model"))

## Load Required IHME Libraries
central_root <- get_path("central_root", process="cancer_model")
setwd(central_root)
library(ggplot2)

################################################################################
### Main functions
################################################################################
run_models <- function(mir_model_version_id=NULL,  model_index_id_list=c(), nDraws=0, 
                        cluster_is_busy=FALSE) {
    ## Runs submission function for each member of list
    ##
    if (is.null(mir_model_version_id)) stop("Must send mir_model_version_id")
    if (length( model_index_id_list)== 0) stop("Must send list of model_index_ids from config")
    for (model_index_id in  model_index_id_list) {
        run_id <- run_mir_worker.register_model(mir_model_version_id,
                                                 model_index_id, 
                                                 nDraws)
        print(paste0('registered run_id ', run_id))
        run_mir_worker.launch_model(run_id)
        ## Wait between models to prevent bottleneck      
        if (cluster_is_busy) {
            pause_time <- 30*60
            current_time <- Sys.time()
        } else pause_time = 60
        if (length( model_index_id_list) > 1) {
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


################################################################################
## Run Functions
################################################################################
print("All functions loaded")
print("REMINDER: worker scripts must be submitted in PROD cluster")
if (!interactive()) {
    mvid <- as.numeric(commandArgs(trailingOnly=TRUE)[1])
    model_ids <- as.numeric(commandArgs(trailing=TRUE)[2])
    num_draws <- as.numeric(commandArgs(trailingOnly=TRUE)[3])
    run_models(mir_model_version_id = mvid,  model_index_id_list = model_ids,
               nDraws = num_draws)
    #mir.verify_success(mir_model_version_id = mvid)
}