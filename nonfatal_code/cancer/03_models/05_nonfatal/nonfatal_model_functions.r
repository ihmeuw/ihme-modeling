#!/usr/local/bin/R
#########################################
## Description: Loads functions common to the nonfatal modeling processes
## Input(s)/Output(s): see individual functions
## How To Use: intended to be sourced by a nonfatal model script
#########################################
## Load Libraries

source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
source(get_path("common_model_functions", proces="cancer_model"))
library(data.table)
library(foreign)

#########################################
## Define Functions
#########################################
nonfatal_model.get_causes <- function(starting_with="neo_") {
## wrapper for the common modeling function with epi_model specified
## returns a list of epi cancer causes that begin with 'starting_with'
    cancer_model.get_modeledCauseList(which_model = "epi_model", starting_with)
}

nonfatal_model.get_cause_id <- function(acause){
## returns the cause_id for the requested acause
    cause_info <- read.csv(get_path("causes"))
    cause_id <- cause_info[cause_info$acause == acause, "cause_id"]
    return(cause_id)
}

nonfatal_model.add_all_age <- function(df, uid_vars, data_var) {
## returns the input "df" data with "all ages" data if "all ages" are not present
    if (22 %in% unique(df$age_group_id)) {
        return (df)
    }

    all_ages_members <- c( seq(2,20), seq( 30, 32), 235)
    data_variables <- names(df)[data_var %in% names(df)]
    to_aggregate <- subset(df, df$age %in% all_ages_members, data_variables)
    all_age_data <- as.data.frame(aggregate(to_aggregate, uid_vars, sum))
    return(rbind(df, all_age_data))
}

nonfatal_model.finalize_draws <- function(df, output_file) {
## Checks for duplicates then saves the input "df" as a stata file
        uid_vars <- names(df)[!grepl("draw", names(df))]
        draw_vars <- names(df)[grepl("draw", names(df))]

        if (anyDuplicated(df, by=uid_vars)) {
            stop("duplicates found during finalization.")
        }

        ensure_directory_presence(output_file)
        output_df = subset( df, , c( uid_vars, draw_vars) )
        write.dta( output_df , output_file, version=10L)
        print(paste("file saved at", output_file))
}
