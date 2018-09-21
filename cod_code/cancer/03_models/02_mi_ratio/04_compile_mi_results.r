#!/usr/local/bin/R
#########################################
## Description: Compiles all MI model results for the most recent run of the prompted model input number
## Input(s): model_input number of interest
## Output(s): compiled data frame in the mi_ratio model storage folder
## How To Use: source script, then enter model_input number at the prompt
## Notes: An S4 object error on loading the HDF5 files often results withn the space is out of memory.
##                Try running rm(list=ls()) first, or restart R
#########################################
## Clear workspace and load libraries
rm(list=ls())

source(file.path(h, 'cancer_estimation/utilities.R'))
source(get_path('common_model_functions', process="cancer_model"))
source(get_path("run_mi_functions" , process="cancer_model") )
source(get_path("compile_mi_functions" , process="cancer_model") )
for (pkg in c('data.table', 'rhdf5')){
    if (!require(pkg,character.only=TRUE)) install.packages(pkg)
    library(pkg,character.only=TRUE)
}

## Set output files and folders
output_file_raw <- get_path("compiled_mi_outputs", process="cancer_model")
today <- Sys.Date()
output_file <- gsub("<date>", "", output_file_raw)
archive_file <- gsub("<date>", today, output_file_raw)
temp_folder <- utils.get_path("mi_draws_processing", process="cancer_model")
archive_folder <- paste0(temp_folder, "/_archive")
for (d in c(output_file, temp_folder, archive_file)) {
    ensure_directory_presence(d)
}
################################################################################
## Collect and format data
################################################################################
# Create list of run_ids
model_input <-  as.numeric(readline(prompt="Enter an model input number: "))
run_id_map <- cancer_model.load_mi_runID_map(model_input_number= model_input)
run_ids <- unique(run_id_map$run_id)

## Verify that outputs exist for all run_ids before proceeding
run_miModel.check_outputs(run_id_list=run_ids)

## Load and format maps for upper- and lower- caps
cap_maps <- cancer_model.load_mi_caps(model_input_numbers = run_id_map$model_input)
upper_caps <- cap_maps$upper_caps
lower_caps <- cap_maps$lower_caps

## Submit jobs for script that extracts data from .h5, then saves a file with the mean
for (id in run_ids){
    worker_script <- get_path("mean_stgpr_draws", process="cancer_model")
    job_name <- paste0('mi_', id)
    launch_jobs(worker_script, memory_request=5, script_arguments=c(i), job_header=job_name)
}

## Concatenate the results of the completed jobs
print("Checking for results and concatenating...")
compiled_data_raw <- do.call(rbind, lapply(run_ids, compile_mi.loadMean))
print("Formatting...")
write.csv(compiled_data_raw, file = paste0(temp_folder,file="/temp_compiled.csv"), row.names=FALSE)
if (sum(compiled_data_raw$has_draws) != nrow(compiled_data_raw) ) stop("Error retrieving draws for all runs")

## convert age_group_id 30 to 21 
compiled_data <- compiled_data_raw
compiled_data$age_group_id <- as.numeric(as.character(compiled_data$age_group_id))
compiled_data[age_group_id == 30, age_group_id := 21]
setnames(compiled_data, old = c('sex_id','year_id', 'gpr_mean'), new = c('sex','year', 'data'))
compiled_data <- subset(compiled_data, , !colnames(compiled_data) %in% 'X')
marked_data <- merge(compiled_data, run_id_map, by='run_id', all.x=TRUE)
expanded_data <- cancer_model.replace_missing_ages(marked_data)

## revert to cartesian space and adjust outputs by the caps, first ensuring that data are within the correct boundaries 
adjusted_mi <- merge(expanded_data, upper_caps, by= c('model_input', 'age_group_id'), all.x=TRUE)
adjusted_mi <- merge(adjusted_mi, lower_caps, by=c('model_input', 'age_group_id', 'acause'), all.x=TRUE)
adjusted_mi[data < 0, data := 0]
adjusted_mi[data > 1, data := 1]
adjusted_mi$mi_ratio <- adjusted_mi$data * adjusted_mi$upper_cap
adjusted_mi[mi_ratio < lower_cap, mi_ratio := lower_cap]

output <- subset(adjusted_mi, ,c('location_id', 'year', 'sex', 'age_group_id', 'acause', 'run_id', 'model_input', 'mi_ratio', 'upper_cap', 'lower_cap'))
write.csv(output, file=output_file, row.names=FALSE)
write.csv(output, file=archive_file, row.names=FALSE)
print("Done.")

################################################################################
## END
################################################################################

