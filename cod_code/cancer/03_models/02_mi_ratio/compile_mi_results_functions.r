#!/usr/local/bin/R
#########################################
## Description: loads functions used by compile_mi_results script
## Input(s)/Output(s): see individual functions
## How To Use: must be sourced by the compile_mi_results script
## IMPORTANT NOTE: run this code on cluster
#########################################

compile_mi.loadMean <- function(runID) {
    ## checks for runID
    mean_file = paste0(temp_folder, "/mean_draws_", runID, ".csv")
    count = 0
    while (!file.exists(mean_file)) {
        Sys.sleep(1)
        count = count + 1
        if (count > 2000) break
    }
    mean_data <- fread(mean_file, stringsAsFactors=FALSE)
    print(paste("Loading", runID))
    return(mean_data)
}