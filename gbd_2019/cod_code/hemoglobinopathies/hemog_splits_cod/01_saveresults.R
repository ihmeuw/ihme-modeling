#########################################################
###### Purpose: save script for hemog-splits ############
####### in order to submit parallel so i can be away#####
#########################################################
#########################################################

rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "USERNAME"
}

arg <- commandArgs(trailingOnly = TRUE)
cause <- arg[1]

#############################################

source("FILEPATH")

#############################################

save_results_cod(cause_id = cause,
                 input_dir = paste0("FILEPATH",
                 input_file_pattern = "FILEPATH",
                 description = ("Post-Step 4, testing new processing, 12/20, rate"),
                 year_id = c(1980:2017),
                 metric_id = 3,
                 gbd_round_id = 6,
		 decomp_step = 'step4', mark_best = FALSE)
                     



