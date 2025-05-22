#!/usr/local/bin/R
################################################################################
## Description: Allows user to run certain steps of the MIR pipeline
## Input(s): See individual functions

## How To Use: Run script in RStudio Session
## Contributors: INDIVIDUAL_NAME
################################################################################
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here::here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r')) # Loads utilities functions, eg. get_path
source(file.path(code_repo, '_database/cdb_utils.r'))
library(data.table)

library(writexl)
library(tidyr)

# source mir pipeline scripts
source(file.path(get_path('mi_model_code', process="common"), "00_mir_prep.r"))
source(file.path(get_path('mi_model_code', process="common"), "01_create_bundle_versions.r"))
source(file.path(get_path('mi_model_code', process="common"), "02_run_mir.r"))
source(file.path(get_path('mi_model_code', process="common"), "03_check_success.r"))
source(file.path(get_path('mi_model_code', process="common"), "03_plot_results.r"))
source(file.path(get_path('mi_model_code', process="common"), "04_compile_results.r"))

################################################################################
### Main functions                                                             #
################################################################################


#' Prints the gbd_parameter settings for the given mir run
run_mir_pipeline.prompt_gbd_parameters <- function(){
  gbd_name <- gbd_id <- get_gbd_parameter("current_gbd_name")
  release_id <- get_gbd_parameter("current_release_id")
  cat(paste0("Verify that the following gbd settings are accurate before proceeding.\n",
             "current_gbd_name: ", gbd_name, "\n",
             "current_releae_id: ", release_id, "\n\n"))
  run_mir_pipeline.user_input_with_quit(
    prompt = "Hit enter or return to continue ... ")
}


#' @return (list) containing the function names for each mir pipeline step
run_mir_pipeline.get_pipeline_steps <- function(){
  return(list("1"="prep_mir",
              "2"="launch_bundle_jobs", 
              "3"="run_models",
              "4"="launch_save_stgpr_jobs", 
              "5"="plot_all",
              "6"="generate_compiled_file"))
}

run_mir_pipeline.display_pipeline_steps <- function(){
  steps <- run_mir_pipeline.get_pipeline_steps() %>% unlist
  cat("Here is the list of mir pipeline steps available:\n")
  lapply(c(1:length(steps)), function(x) cat(paste0(x, ":", steps[[x]], "\n"))) %>% unlist
  cat("\n")
}


#' @return (vector) of the individual steps that the user wants to run
run_mir_pipeline.get_user_selected_pipeline_steps <- function(){
  steps_string <- run_mir_pipeline.user_input_with_quit(
    prompt="Enter list of mir pipeline steps to run separated by a space: ")
  input_steps <- strsplit(steps_string, split = " ") %>% 
      unlist() %>% as.numeric()
  # validate that these steps exist
  tryCatch({
    steps <- lapply(input_steps,
                    function(x) run_mir_pipeline.get_pipeline_steps()[[x]]) %>% unlist
    }, error = function(e){
      stop("There is an error in your input steps. Please try again.")
    })
    
    cat(paste0("Here are the steps you selected to run:\n", 
               paste0(steps, collapse = "\n"), "\n"))
    run_mir_pipeline.user_input_with_quit(
      prompt="Hit enter or return to continue. ")
    return(input_steps)
}

#' @param prompt (str) of the prompt

run_mir_pipeline.user_input_with_quit <- function(prompt){
  cat("Enter q to exit")
  value <- readline(prompt=prompt)
  if(tolower(value) == "q") stop("You exited the script")
  return(value)
}

#' Prints the current mir_model_version's ST-GPR config 
#' for user to refer to when selecting model_index_ids to
#' run in ST-GPR
#' @param mir_model_version_id (int) the mir_model_version_id to run for
run_mir_pipeline.prompt_stgpr_config <- function(mir_model_version_id){
  cat("Displaying current ST-GPR config for choosing model_index_ids to run: \n")
  print(mir.load_stgprConfig(mir_model_version_id)[, c("model_index_id", "me_name", "description")])
  run_mir_pipeline.user_input_with_quit(prompt = "Hit enter or return to continue ... ")
}


# this will help write the specific model index config so we don't forget to do it ourselves
run_mir_pipeline.update_stgpr_config <- function(mir_model_version_id, model_index_start, model_index_end){
  stgpr_config <- paste0(get_path(key="stgpr_configs", process="mir_model"),
                         "/model_version_", mir_model_version_id, ".csv")

  for(cur_model_index in model_index_start:model_index_end){
    stgpr_model_entry <- fread(stgpr_config)[model_index_id == cur_model_index]
    if(nrow(stgpr_model_entry) == 0){
      cat(paste0("Skipping model_index_id ", cur_model_index, ". Doesn't exist in config file."))
      break
    }
    stgpr_config_ix_file <- paste0(get_path(key="stgpr_configs", process="mir_model"),
                                   "/model_version_", mir_model_version_id, "_ix_", cur_model_index, ".csv")
    fwrite(stgpr_model_entry, stgpr_config_ix_file)
    run_mir_pipeline.user_input_with_quit(prompt = paste0(
      "Check ", stgpr_config_ix_file, " to make sure your specific model entry is correct!"))
  }
  
}


run_mir_pipeline.get_pipeline_step_reminders <- function(step){
  if(step == "1") prompt <- "Please remember to update mir_cause_config and mir_model_entity before continuing!"
  else if(step == "2") prompt <- "Please run 01_MIR_nids.py before running step 2 of the MIR pipeline!"
  else if(step == "3") prompt <- paste0("Please remember to update the ST-GPR config for your models here:\n",
                                        paste0(get_path(key="stgpr_configs", process="mir_model"),
                                        "/model_version_<mir_model_version_id>.csv"))
  else return()
  run_mir_pipeline.user_input_with_quit(prompt=prompt)
}


#' @param step (str) numeric step (as str) of the mir pipeline step
#' @param step_args base mir pipeline arguments 
#' @return (list) updated list of mir pipeline arguments now
#' curated to each mir pipeline step
run_mir_pipeline.get_pipeline_step_params <- function(step, step_args){
  new_step_args <- copy(step_args)
  if(step == "1"){
    new_step_args[['age_type']] <- run_mir_pipeline.user_input_with_quit(
      prompt="Enter age_type associated with mir_model_version_id (Please enter all_ages or pediatric, or both): ") %>% as.character
    new_step_args[['calculate_caps']] <- run_mir_pipeline.user_input_with_quit(
      prompt="Enter whether to calculate upper and lower caps or not (Please enter TRUE or FALSE): ") %>% as.logical
  } else if(step == "2"){
    new_step_args[['age_type']] <- run_mir_pipeline.user_input_with_quit(
      prompt="Enter age_type associated with mir_model_version_id (Please enter all_ages, pediatric, both, or pooled): ") %>% as.character
    if (new_step_args[['age_type']]=="both"){
      new_step_args[['peds_mir_model_version_id']] <- run_mir_pipeline.user_input_with_quit(
        prompt="Enter mir_model_version_id associated with pediatric run you want to use: ") %>% as.numeric
      new_step_args[['all_ages_mir_model_version_id']] <- run_mir_pipeline.user_input_with_quit(
        prompt="Enter mir_model_version_id associated with all_ages run you want to use: ") %>% as.numeric
    } else {
      new_step_args[["peds_mir_model_version_id"]] <- NULL
      new_step_args[["all_ages_mir_model_version_id"]] <- NULL
    }
  } else if(step == "3"){
    new_step_args[["mir_process_type_id"]] <- NULL
    run_mir_pipeline.prompt_stgpr_config(mir_model_version_id = step_args[['mir_model_version_id']])
    new_step_args[['nDraws']] <- run_mir_pipeline.user_input_with_quit(
      prompt="Enter number of draws to run models on: ") %>% as.numeric
    new_step_args[['model_index_start']] <- run_mir_pipeline.user_input_with_quit(
      prompt="Enter model_index_start: ") %>% as.numeric
    new_step_args[['model_index_end']] <- run_mir_pipeline.user_input_with_quit(
      prompt="Enter model_index_end: ") %>% as.numeric
    run_mir_pipeline.update_stgpr_config(mir_model_version_id = step_args[['mir_model_version_id']],
                                         model_index_start = new_step_args[['model_index_start']], 
                                         model_index_end = new_step_args[['model_index_end']])
  } else if(step == "4"){
    new_step_args[["mir_process_type_id"]] <- NULL
    new_step_args[['mark_best']] <- run_mir_pipeline.user_input_with_quit(
      prompt="Enter whether to mark these ST-GPR models as best (Please enter TRUE or FALSE): ") %>% as.logical
  } else if(step == "5"){
    new_step_args[["mir_process_type_id"]] <- NULL
  } else if(step == "6") new_step_args[['is_resub']] <- run_mir_pipeline.user_input_with_quit(
    prompt="Enter whether to be in resubmission mode for compiling results: ") %>% as.logical
  return(new_step_args)
}


#' 
#' @param mir_model_version_id (int) the mir_model_version_id to run for


run_mir_pipeline.run <- function(mir_model_version_id, mir_process_type_id){
  mir_pipeline_steps <- run_mir_pipeline.get_pipeline_steps()
  run_mir_pipeline.display_pipeline_steps()
  input_steps <- run_mir_pipeline.get_user_selected_pipeline_steps()
  # run each step requested
  for(step in input_steps){
    run_mir_pipeline.get_pipeline_step_reminders(step)
    step_args <- list(mir_model_version_id = mir_model_version_id,
                      mir_process_type_id = mir_process_type_id)
    new_step_args <- run_mir_pipeline.get_pipeline_step_params(step, step_args)
    setwd(code_repo) # set working dir to repo each time after running a step
    do.call(get(mir_pipeline_steps[step] %>% unlist), new_step_args)
  }
}

if(!interactive()){
  run_mir_pipeline.prompt_gbd_parameters()
  mir_model_version_id <- as.numeric(commandArgs(trailingOnly=TRUE)[1])
  mir_process_type_id <- as.numeric(commandArgs(trailingOnly=TRUE)[2])
  run_mir_pipeline.run(mir_model_version_id, mir_process_type_id)
} else{
  run_mir_pipeline.prompt_gbd_parameters()
  mir_model_version_id <- run_mir_pipeline.user_input_with_quit(prompt="Enter mir_model_version_id: ") %>% as.numeric
  mir_process_type_id <- run_mir_pipeline.user_input_with_quit(prompt="Enter mir_process_type_id: ") %>% as.numeric
  run_mir_pipeline.run(mir_model_version_id, mir_process_type_id)
}

