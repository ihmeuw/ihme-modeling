#####################################INTRO#############################################
#' Author: 
#' 3/24/18
#' Purpose: Parallelized version of save_results to speed things up
#'          1) save results
#'
#' OUTPUTS: OUTPUTS
#'
#####################################INTRO#############################################

library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(rjson)
source_functions(save_results_cod = T)

# Read in country-specific data -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir     <- args[1]
cause_id    <- args[2]
description <- args[3]


# Save results ------------------------------------------------------------

save_results_cod(paste0(out_dir, "EX"), input_file_pattern = "{location_id}_{year_id}_{sex_id}.csv", 
                 cause_id = cause_id, description = description, mark_best = FALSE)



# try saving results, creating a message for success/failure
tryCatch({
  save_results_cod(paste0(out_dir, "results"), input_file_pattern = "{location_id}_{year_id}_{sex_id}.csv", 
                   cause_id = cause_id, description = description, mark_best = FALSE)
  assign("message", paste0("Congenital syphilis deaths for cause_id ", cause_id, " uploaded successfully ", 
                           " with descrition: ", description), .GlobalEnv)
}, error = function(e) {
  assign("message", paste0("Congenital syphilis deaths for cause_id ", cause_id, " upload FAILED. Error: ", e), .GlobalEnv)
})



# Make a message ----------------------------------------------------------
json <- rjson::fromJSON(file = paste0(h_root, "private_url.json"))

post_slack <- function(message, url) {
  system(paste0("curl -X POST -H \'Content-type: application/json' --data \'{\"text\": \"", message, "\"}\' ", url))
}

# post message
post_slack(message = message, url = json$pipeline_updates)

