#####################################INTRO#############################################
#' Author: 
#' 5/21/18
#' Purpose: Wait for epididymo prevalence draw files to save from 04 and then upload to epi database via save_results
#'          1) WAIT
#'          2) Run save_results
#'          3) STEP 2
#'
#'
#####################################INTRO#############################################


library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(rjson)

source_functions(save_results_epi = T, get_location_metadata = T, get_best_model_versions = T)



# Read in commandline args and setup ------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
me_id    <- args[1]
main_dir <- args[2]
root_dir <- args[3]
# me_id <- 20394

# ME ID of the final model we want to upload to,
# NOT the intermediate dismod model me id
upload_me_id <- if (me_id == 20394) {
  assign("acause", "std_chlamydia", envir = .GlobalEnv)
  1630 # EO due to chlamydia
} else if (me_id == 20393) {
  assign("acause", "std_gonnorhea", envir = .GlobalEnv)
  1636 # EO due to gonorrhea
}


# source job submission function (submits 04) and wait for function
source(paste0(root_dir, "submit_and_wait_functions.R"))

# set up locations for wait_for function
loc_data <- get_location_metadata(35)
locs     <- unique(loc_data[level >= 3, location_id]) 

# grab data on the model
model <- get_best_model_versions("modelable_entity", me_id)

# Wait for files from 04 to finish ----------------------------------------
# resubmits jobs if they fail

message(paste(Sys.time(), "Waiting for draw files to save from me_id", me_id))

wait_for(locs, files_in = paste0(main_dir, "EX"), pattern = me_id, 
         relaunch_code = paste0(root_dir, "04_save_epididymo_prevalence.R"), time = 60, repeat_limit = 20)



# Launch save_results_epi -------------------------------------------------
# files are currently saved as {me_id}_{location_id}.csv

description <- paste("Epididymo-orchitis prevalence due to", model$acause, "from DisMod model", model$model_version_id, "(", model$description, ")", "ran on", model$date_inserted)

message(paste(Sys.time(), "Saving results to me_id", upload_me_id))


# try saving results, creating a message for success/failure
tryCatch({
  save_results_epi(input_dir = paste0(main_dir, "EX"), input_file_pattern = paste0(me_id, "_{location_id}.csv"), measure_id = 5,
                   sex_id = 1, modelable_entity_id = upload_me_id, description = description,
                   mark_best = TRUE, birth_prevalence = FALSE)
  assign("message", paste0("Final epididymo-orchitis prevalence for ", acause, " uploaded successfully to me_id ", upload_me_id, 
                    " with descrition: ", description), .GlobalEnv)
}, error = function(e) {
  assign("message", paste0("Final epididymo-orchitis prevalence for ", acause, " upload FAILED. Error: ", e), .GlobalEnv)
})



# Make a message ----------------------------------------------------------
json <- rjson::fromJSON(file = paste0(h_root, "private_url.json"))

post_slack <- function(message, url) {
  system(paste0("curl -X POST -H \'Content-type: application/json' --data \'{\"text\": \"", message, "\"}\' ", url))
}

# post message
post_slack(message = message, url = json$pipeline_updates)














