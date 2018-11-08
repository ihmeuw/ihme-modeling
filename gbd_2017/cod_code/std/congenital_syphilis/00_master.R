#'####################################`INTRO`##########################################
#' @author: 
#' 7/5/18 - the rewrite
#' @purpose: Master script for the congenital syphilis process. The only other input is 
#'           the early syphilis model that female seroprevalence is taken from. Pipeline steps are details below
#'           SETUP:
#'          @1 `01_test_and_treat`: 
#'          formats ST-GPR results from syphilis testing/treatment proportions into usable proportions by location
#'          @2 `02_global_cod_ratios`: 
#'          Calculate the global ratios of VR syphilis deaths for each age group relative 
#'          to neonatal VR deaths in order to expand calculated neonatal deaths into total deaths under age 10.
#'          @3 `03_anc_visit_proportions`: 
#'          Calculate the ANC visit proportions using ANC 1+, 2+, and 4+ covariates
#'          @4 `04_transmission_rate_by_stage`: 
#'          As early and late syphilis stages have a differing transmission rate to the fetus,
#'          our goal is to capture the difference in syphilis transmission between women with early and late syphilis. 
#'           BEGIN CALCULATIONS:
#'          @5 `05_unadjusted_livebirths`: 
#'          Calculate the number of live births at risk for syphilis, unadjusted by the increased
#'          risk of fetal loss in those with syphilis infections.  
#'          @6 `06_adjusted_livebirths`: 
#'          Adjust livebirths for to account for the increased risk of adverse pregnancy outcomes in syphilitic women
#'          @7 `07_congenital_syphilis_deaths`: 
#'          Calculate the total number of deaths from congenital syphilis (< 10 years) and save results
#'            SAVING:
#'          @8 Wait for all files from 5 - 7 to finish
#'          @9 Save results for both STDs excluding HIV (393) and syphilis (394)
#'
#' @outputs: congenital syphilis deaths under age 10 for each location across the globe and year since 1980.
#'           Draws are saved as FILEPATH.csv
#'
#'####################################`INTRO`##########################################


# Set up environment ------------------------------------------------------
library("ihme", lib.loc = "FILEPATH")
ihme::setup()

# all of the shared functions we need for 00 - 04
source_functions(get_best_model_versions = T, get_location_metadata = T, get_cod_data = T, 
                 get_covariate_estimates = T, get_population = T, get_ids = T)

library(magrittr)

source("cs_functions.R")


# put this in for dynamic run. Hard-coded for reproducibility
date       <- gsub("-", "_", Sys.Date())
#date       <- "2018_07_05"
h_base     <- "BASE_FILEPATH"
share_base <- paste0("FILEPATH/")
root_dir   <- paste0(h_root, h_base)
out_dir    <- paste0(share_base, "run_", date,"/")

model       <- get_best_model_versions("modelable_entity", 3948)
description <- paste0("ES model ", model$model_version_id, ": example description ", Sys.Date())


# Create directories, gives warning if already exists ---------------------

# dir.create(share_base)
dir.create(out_dir)
directories <- c("EX1", "EX2", "EX3", "EX4", "EX5", "EX6")
invisible(lapply(directories, function(inner_dir) { dir.create(paste0(out_dir, inner_dir)) }))


# Write run README --------------------------------------------------------------

# write a README for the run
file_conn <- file(paste0(out_dir, "README.md"))
writeLines(description, file_conn)
close(file_conn)


# Set up scripts model ----------------------------------------------------

# Logic switches on which scripts are run here.
# This is a named vector so you can easily see whats what
#' `RERUN RULES` listed for each file
run <- c("01_test_and_treat.R"             = 1, # RERUN: when you have new stg-pr results; will have to update run_ids
         "02_global_cod_ratios.R"          = 1, # RERUN: after each cod refresh
         "03_anc_visit_proportions.R"      = 1, # RERUN: when new ANC covariate version is available, talk to modeler
         "04_transmission_rate_by_stage.R" = 1) # RERUN: if meta-analyses are redone

settings <- data.frame(script = names(run), run = run, row.names = NULL)

# Run scripts -------------------------------------------------------------

source_script <- function(path = root_dir, script, out_dir) {
  run <- settings[script, "run"]
 
  if (run) {
    script_name <- paste0(path, script)
    message(paste(Sys.time(), "Running", script))
    source(script_name, local = TRUE)
  }
  
}

mapply(source_script, script = settings$script, out_dir = out_dir)


# Delete old draw files, diagnostics --------------------------------------

# remove old files
rm_all <- function(dir) {
  message(paste(Sys.time(), "Removing all files in ", dir))
  message(paste0("rm -f ", dir, "*"))
  system(paste0("rm -f ", dir, "*"))
}

written_dirs <- paste0(out_dir, c("EX1/", "EX2/", "EX3/", "EX4/"))
invisible(lapply(written_dirs, rm_all))


# Submit calculation pipeline jobs ----------------------------------------


location_data <- get_location_metadata(location_set_id = 35) 
locations     <- unique(location_data[level >= 3, location_id]) # locations only for nationals and subnationals

submit_for <- function(location_id, out_dir, root_dir) {
  jobname <- paste0("cs_", location_id)
  args <- list(out_dir, root_dir, location_id, root_dir)
  
  ihme::qsub(jobname, code = paste0(root_dir, code = "05_unadjusted_livebirths.R"), pass = args,
             slots = 5, submit = T, proj = "proj_custom_models",
             shell = "SINGULARITY_FILEPATH")
}


invisible(parallel::mclapply(sort(locations), submit_for, out_dir = out_dir, root_dir = root_dir))


# Wait for jobs to finish -------------------------------------------------

current_year <- get_ids("gbd_round")
current_year <- current_year[nrow(current_year), "gbd_round"][[1]] # dont have to update year

file_count <- (current_year - 1980 + 1) * 2 * length(locations)

wait_for(file_count, files_in = paste0(out_dir, "results"), time = 60, repeat_limit = 15)


# Launch save_results -----------------------------------------------------

message(paste(Sys.time(), "Launching save_results..."))

# launch 
launch_save <- function(cause_id, out_dir, description, root_dir) {
  jobname <- paste0("save_", cause_id)
  args <- list(out_dir, cause_id, description)
  
  ihme::qsub(jobname, code = paste0(root_dir, code = "08_save.R"), pass = args,
             slots = 25, submit = T, proj = "proj_custom_models",
             shell = "SINGULARITY_FILEPATH")
}

# 393: stds excluding HIV
# 394: syphilis
lapply(c(393, 394), launch_save, out_dir = out_dir, description = description, root_dir = root_dir)

message(paste(Sys.time(), "DONE"))


