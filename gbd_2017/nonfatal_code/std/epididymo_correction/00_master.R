#####################################INTRO#############################################
#' Author:
#' 5/4/18
#' Purpose: Master launch script for prevalence correction for epididymo-orchitis from chlamydia 
#'          and gonorrhea
#'          1) Set up directories
#'          2) Launch 01_severity_split_draws.R for both causes to get severity split draws 
#'          3) 02_epididymo_incidence.R is parallelized by location, grabbing male incidence draws
#'          4) 03_upload_to_epi_db whiles the current EO epi db, waits for all jobs from 02 to finish
#'             and the uploads the new data to epi viz
#'          5) Launch dismod model
#'          6) 04_save_epididymo_prevalence grabs draws from our fresh dismod model, parallelized by location
#'          7) 05_wait_and_save_results waits for all jobs from 04 to finish and then uploads them via save_results_epi
#'
#'
#####################################INTRO#############################################

library("ihme", lib.loc = "FILEPATH")
setup()

source_functions(get_location_metadata = T)



# Run switches ------------------------------------------------------------

run_01_sev_split_draws   <- FALSE 

run_02_chlamydia_eo_inc       <- FALSE
run_03_chlamydia_upload       <- FALSE
run_04_chlamydia_save_eo      <- TRUE
run_05_chlamydia_final_upload <- TRUE

run_02_gonorrhea_eo_inc       <- FALSE
run_03_gonorrhea_upload       <- FALSE
run_04_gonorrhea_save_eo      <- FALSE
run_05_gonorrhea_final_upload <- FALSE


# Create directories on /ihme --------------------------------------------

root_dir <- paste0("ROOT_DIR/")
base_dir <- paste0("BASE_DIR/")
main_dir <- paste0(base_dir, "epididymo_correction/")

dir.create(main_dir)

directories <- c("EX1", "EX2","EX3", "EX4")
lapply(directories, function(inner_dir) { dir.create(paste0(main_dir, inner_dir)) })


# Function and location setup ---------------------------------------------

loc_data <- get_location_metadata(35)
locs     <- unique(loc_data[level >= 3, location_id]) 

source(paste0(root_dir, "submit_and_wait_functions.R"))

# Source severity split draws script --------------------------------------

if (run_01_sev_split_draws) {
  source(paste0(root_dir, "01_severity_split_draws.R"))
}



# Delete old incidence files ----------------------------------------------

rm_all <- function(dir, run = TRUE) {
  if (run) {
    message(paste0(Sys.time(), " Removing all files in ", dir))
    message(paste0("rm -f ", dir, "*"))
    system(paste0("rm -f ", dir, "*"))
  }
}

# dirs need slashes at the end
written_dirs <- paste0(main_dir, c("EX1/", "EX2/"))
invisible(lapply(written_dirs, rm_all, run = run_02_chlamydia_eo_inc | run_02_gonorrhea_eo_inc))


# Launch 02_prevalence_correction for both causes -------------------------
# running in parallel for each location
# will run if run_epididymo_inc == TRUE

# creates me_ids to run based on logic switches
me_ids <- if (run_02_chlamydia_eo_inc && run_02_gonorrhea_eo_inc) {
  c(1629, 1635)
} else if (run_02_chlamydia_eo_inc) { 
  1629 
} else if (run_02_gonorrhea_eo_inc) { 
  1635
} else {
  NULL
}

# submit  02 for each me_id supplied
invisible(parallel::mclapply(sort(locs), submit_both, 
                             code_dir = paste0(root_dir, "02_epididymo_incidence.R"),
                             out_dir = main_dir,
                             me_ids = me_ids))



# Upload to epi db (waiting for jobs to finish ofc) -----------------------


chlamydia_args <- list(1629, main_dir, root_dir)
gonorrhea_args <- list(1635, main_dir, root_dir)


# launch chlamydia file manipulation/upload job first
ihme::qsub("chla_upload", code =  paste0(root_dir, "03_upload_to_epi_db.R"), 
           pass = chlamydia_args, slots = 10, submit = run_03_chlamydia_upload, proj = "proj_custom_models",
           shell = paste0(j_root, "SHELL_SCRIPT"))

# launch gonorrhea file manipulation/upload job second
ihme::qsub("gono_upload", code =  paste0(root_dir, "03_upload_to_epi_db.R"), 
           pass = gonorrhea_args, slots = 10, submit = run_03_gonorrhea_upload, proj = "proj_custom_models",
           shell = paste0(j_root, "SHELL_SCRIPT"))




# THEN HAVE TO RUN DISMOD MANUALLY ----------------------------------------

# ...

# Delete old prevalence draws ---------------------------------------------

# dirs need slashes at the end
written_dirs <- paste0(main_dir, "epididymo_prev_draws/")
invisible(lapply(written_dirs, rm_all, run = run_04_chlamydia_save_eo | run_04_gonorrhea_save_eo))


# Save epididymo prevalence draws -----------------------------------------
# In this step we take the prevalence draws from the
# dismod models for epididymo that we just launched and save

# creates me_ids to run based on logic switches
me_ids <- if (run_04_chlamydia_save_eo && run_04_gonorrhea_save_eo) {
  c(20394, 20393)
} else if (run_04_chlamydia_save_eo) { 
  20394 
} else if (run_04_gonorrhea_save_eo) { 
  20393
} else {
  NULL
}

# submit  04 for each me_id supplied
invisible(parallel::mclapply(sort(locs), submit_both, 
                             code_dir = paste0(root_dir, "04_save_epididymo_prevalence.R"),
                             out_dir = main_dir,
                             me_ids = me_ids))



# Wait for prevalence saving to finish and then upload --------------------
# via save_results_epi

chlamydia_eo_args <- list(20394, main_dir, root_dir)
gonorrhea_eo_args <- list(20393, main_dir, root_dir)

# launch chlamydia wait/save_results job first
ihme::qsub("chla_upload", code =  paste0(root_dir, "05_wait_and_save_results.R"), 
           pass = chlamydia_eo_args, slots = 25, submit = run_05_chlamydia_final_upload, proj = "proj_custom_models",
           shell = paste0(j_root, "SHELL_SCRIPT"))

# launch gonorrhea wait/save_results job second
ihme::qsub("gono_upload", code =  paste0(root_dir, "05_wait_and_save_results.R"), 
           pass = gonorrhea_eo_args, slots = 25, submit = run_05_gonorrhea_final_upload, proj = "proj_custom_models",
           shell = paste0(j_root, "SHELL_SCRIPT"))





