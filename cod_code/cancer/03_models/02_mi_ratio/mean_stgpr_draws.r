#!/usr/local/bin/R
#########################################
## Description: Loads draws from the stgpr folder, if present, or the predicted model mean (if draws not present)
##                  then and saves a data frame with the mean values.
## Input(s)/Output(s): see individual functions
## How To Use: intended for submission as a cluster job in mi result retrieval (see "Run Functions" below),
##                  can also be sourced interactively to retrieve results for a single run_id
#########################################
## Load libraries

source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
for (pkg in c('plyr', 'data.table', 'rhdf5')){
  if (!require(pkg,character.only=TRUE)) install.packages(pkg)
  library(pkg,character.only=TRUE)
}

##########################################
## Define Functions
##########################################
meanDraws <- function(filepath_for_draws) {
    ## Load data from each H5F file and remove unwanted columns
    d_in <- fread(filepath_for_draws, showProgress = FALSE)
    d_in$gpr_mean <- rowMeans(subset(d_in, ,paste0("draw_", seq(0,999))))
    d_out <- subset(d_in, ,c('location_id', 'year_id', 'sex_id', 'age_group_id', 'gpr_mean'))
    return(d_out)
}

calcMeanFromOutput <- function(runID) {
    h5_folder <- get_path("STGPR_outputs", process="cancer_model")
    raked_estimate_file <- paste0(h5_folder, "/", runID, "/temp_1.h5")
    draws_folder <- paste0(h5_folder, '/', runID, '/draws_temp_1')
    draw_files <-list.files(draws_folder, pattern = "*.csv", full.names=TRUE)
    time_estimate = length(draw_files)*.5/60
    time_estimate = round(time_estimate)
    print(paste("calculating mean. this should take between", time_estimate, "and", time_estimate*2, "minutes..."))
    this_data <- do.call(rbind, lapply(draw_files, meanDraws))

    this_data <- subset(this_data, , colnames(this_data) %in%
        c('location_id', 'year_id', 'sex_id', 'age_group_id', 'gpr_mean'))
    this_data$has_draws <- 1
    this_data$run_id <- as.integer(runID)
    return(as.data.table(this_data))
}

formatDraws <- function(run_id_number) {
    print(paste("formatting draws for run_id", run_id_number))
    temp_folder <- get_path("mi_draws_processing", process="cancer_model")
    ensure_directory_presence(temp_folder)
    output_data <- calcMeanFromOutput(run_id_number)
    write.csv(output_data, paste0(temp_folder, "/mean_draws_", run_id_number, ".csv"), row.names=FALSE)
}

##########################################
## Run Functions
##########################################
if (!interactive()){
    ptm <- proc.time()
    run_id <- commandArgs()[3]
    formatDraws(run_id)
    elapsed <- proc.time() - ptm
    print(elapsed)

}
