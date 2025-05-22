####################################################################
## Name of script: 05_generateSurvivalDraws.R
## Description: Launcher for generateSurvivalDraws_worker.R
##              Creates relative survival draws for all locations
##              for each cancer
##
## Arguments: cnf_model_version_id
##
## Outputs: survival draws for each location and cancer
## Author(s): USERNAME
####################################################################

##########################
## Load packages         #
##########################
# launcher for running survival draws in parallel on the cluster
# this will launch one job for each cancer
library(here)  
library(tidyr)
library(argparse)
user <- Sys.info()[["user"]]

source("FILEPATH")
source(file.path("FILEPATH"))

##########################
## Set Parameters         #
##########################
get_cancer_list <- function(){
  # Returns list of str, causes we run in the survival pipeline
  cancerList <- c("neo_brain", "neo_breast", "neo_colorectal", "neo_esophageal", "neo_gallbladder",  
                  "neo_hodgkins", "neo_kidney", "neo_larynx", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic",
                  "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other", "neo_liver", "neo_lung",
                  "neo_lymphoma", "neo_melanoma", "neo_meso", "neo_mouth", "neo_myeloma", "neo_nasopharynx",
                  "neo_other_cancer", "neo_otherpharynx", "neo_pancreas", "neo_stomach", "neo_thyroid", "neo_prostate", 
                  "neo_testicular", "neo_cervical", "neo_ovarian", "neo_uterine",
                  'neo_bone','neo_eye_other','neo_eye_rb','neo_liver_hbl', 'neo_lymphoma_burkitt','neo_lymphoma_other','neo_neuro','neo_liver_hbl','neo_tissue_sarcoma')
  return(cancerList)
}
##########################
## Load functions        #
##########################

load_nf_locations <- function() { 
# Function that imports all nonfatal locations
# Returns: numeric vector, of nf location_ids
  gbd_locs <- get_location_metadata(location_set_id=35)
  estimation_locs <- gbd_locs[most_detailed==1, ]
  return(unique(estimation_locs[,location_id]))
}


clear_rel_survival <- function(causes){
  # Function that clears the relative survival output folders 
  # for select causes
  # Args: causes - vector, of strings of 
  #       our input causes
  
  # clear previous relative survival
  gbd_round <- get_gbd_parameter("current_gbd_name")$current_gbd_name
  surv_path <- paste0("FILEPATH")
  for(cancer in causes){
    print(paste0("Removing previous relative survival for ", cancer))
    #try(do.call(file.remove, list(list.files(paste0(surv_path, "/", cancer), full.names = TRUE))))
    print(paste0("Finished removing previous relative survival for ", cancer))
  }
}


launch_array_job <- function(causes, cnf_vers_id, is_resub, loc_ids = c()){
  #  Function that launches an arrya job with select job parameters
  # 
  #  Args: causes - vector, of strings of our input causes
  #       cnf_vers_id - int, our nonfatal model version id
  #       is_resub - boolean, if we are resubmitting jobs
  #       loc_ids - vector of ints, optional, if specified, runs
  #                 survival pipeline for select location_ids
    if(!is_resub){
      print("Not resubmission mode")
      clear_rel_survival(causes)
    }
    # select locations
    locationList <- load_nf_locations() 
    if(length(loc_ids) > 1) locationList <- loc_ids %>% copy
    else locationList <- load_nf_locations()
    
    for(cancer in causes){
        # create params file
        params <- data.table(cancer=cancer, cnf_version_id = cnf_vers_id, id = 1)
        loc_df <- data.table(id = 1, location_id = locationList)
        params <- merge(params, loc_df, by = "id", allow.cartesian = TRUE)
        params$id <- NULL
        param_file <- paste0("FILEPATH")
        fwrite(params, param_file)

        # ensure log path existence
        output_path <- paste0("FILEPATH")
        error_path <-  paste0("FILEPATH")
        for(log_path in c(output_path, error_path)){
            if(!dir.exists(log_path)){
                dir.create(log_path)
            }
        }
        
        print(paste0("Launching jobs for ", cancer))
        # launch array jobs
        launch_jobs(paste0("FILEPATH"),
                    script_arguments = param_file, memory_request = 5, 
                    time = "00:15:00", num_threads = 1,
                    output_path = output_path,
                    error_path = error_path,
                    job_header = paste0("surv_", cancer),
                    is_array_job = TRUE, njob = nrow(params))
    }
}

check_job_status <- function(causes){
  # Function checks jobs of current causes
  # Args: causes - vector, of strings of our input causes
  
    # check for job completion
    complete_causes <- c()
    while(length(setdiff(causes, complete_causes)) > 0){
        for(cancer in causes){
            if(!cancer %in% complete_causes){
                # check that jobs have finished
                locationList <- load_nf_locations() 
                locationList <- paste0(locationList, ".csv")
                cur_files <-list.files(
                            paste0("FILEPATH", 
                            cancer))
                jobs_left <- length(setdiff(locationList, cur_files))
                if(jobs_left == 0) complete_causes <- c(complete_causes, cancer)
                print(paste0(jobs_left, " jobs left for ", cancer))
                
                missing_locs <- paste0(gsub(".csv", "", setdiff(locationList, cur_files)) %>% as.numeric, collapse = " ")
                print(paste0("Missing locations: [", missing_locs, "]"))
            }
        }
        Sys.sleep(60)
    }
}

parse_args <- function(){
  # Function that parses Returns: data.table of input data with replaced missing values
  parser <- ArgumentParser()
  # specify our desired options 
  parser$add_argument("-c", "--cause", type="character",
                      default=get_cancer_list(), nargs='*',
                      help="List of cause(s) to run relative survival for")
  
  parser$add_argument("-resub", "--resubmission", type = "logical",
                      default=T, nargs='?',
                      help = "Activate resubmission mode (default=True). Set mode to False to delete previous outputs")
  
  parser$add_argument("-rid","--cnf_version_id", type="integer", nargs='?',
                      help="The cnf_model_version id number")
  
  parser$add_argument("-loc_id","--location_id", type="integer", nargs='*',
                      help="location_ids to run this for")
  
  return(parser$parse_args())
}

##########################
## RUN Launcher          #
##########################
args <- parse_args()
causes <- args$cause
cnf_vers_id <- args$cnf_version_id
is_resub <- args$resubmission
loc_ids <- args$location_id
launch_array_job(causes, cnf_vers_id, is_resub, loc_ids)
check_job_status(causes)
