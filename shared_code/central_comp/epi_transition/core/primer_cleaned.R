##########################################################
## Main primer for all jobs
##########################################################
# Dependencies
library(data.table)
library(RMySQL)
library(DBI)
library(parallel)
library(plyr)
library(argparse)

# Load functions
source(paste0(currentDir(), "/helpers/data_grabber.R"))
source(paste0(currentDir(), "/helpers/db_fetcher.R"))

# Load relevant GBD shared functions
gbd_dir <- "FILEPATH"
source(paste0(gbd_dir, "/r/get_covariate_estimates.R"))
source(paste0(gbd_dir, "/r/get_demographics.R"))
source(paste0(gbd_dir, "/r/get_location_metadata.R"))
source(paste0(gbd_dir, "/r/get_draws.R"))
source(paste0(gbd_dir, "/r/get_outputs.R"))
source(paste0(gbd_dir, "/r/get_best_model_versions.R"))
source(paste0(gbd_dir, "/r/get_model_results.R"))
source(paste0(gbd_dir, "/r/get_envelope.R"))
source(paste0(gbd_dir, "/r/get_population.R"))
source(paste0(gbd_dir, "/r/get_rei_metadata.R"))
source(paste0(gbd_dir, "/r/get_cause_metadata.R"))
source(paste0(gbd_dir, "/r/get_life_table.R"))

# Get number of cores available
et.coreNum <- function(p = 1) {
  # Get slots
  if (interactive()) {
    slots <- readline(prompt="Slots in use: ")
  } else slots <- strtoi(Sys.getenv("NSLOTS"))
  slots <- as.numeric(slots)
  
  # Assign cores
  if (grepl("Intel", system("cat FILEPATH | grep 'name'| uniq", intern=TRUE))) {
    cores <- slots * 0.86
  } else {
    cores <- slots * 0.64
  }
  use_cores <- round(cores / p, 0)
  return(use_cores)
}

qsub <- function(jobname, shell, code, project = NULL, hold = NULL, args = NULL, slots = 1, mem = NULL) { 
  # Set up number of slots
  if (is.null(mem)) mem <- slots * 2
  if (slots > 1) {
    slot_string = paste0("-pe multi_slot ", slots, " -l mem_free=", mem, "g")
  }
  # Set up jobs to hold for 
  if (!is.null(hold)) { 
    hold_string <- paste0("-hold_jid \"", hold, "\"")
  }
  # Set up project under which jobs are to be submitted
  if (!is.null(project)) { 
    project_string <- paste0("-P proj_", project)
  }
  # Set up arguments to pass in 
  if (!is.null(args)) { 
    args_string <- paste(args, collapse = " ")
  }
  # Construct the command 
  
  if(grepl("python", shell)) {
      
      sub <- paste("qsub", 
                   if (slots>1) slot_string, 
                   if (!is.null(hold)) hold_string, 
                   if (!is.null(project)) project_string, 
                   "-N ", jobname, 
                   shell,
                   code,
                   if (!is.null(args)) args_string, 
                   sep=" ")
      
  } else {
      
      sub <- paste("qsub", 
                   if (slots>1) slot_string, 
                   if (!is.null(hold)) hold_string, 
                   if (!is.null(project)) project_string, 
                   "-N ", jobname, 
                   shell,
                   code,
                   "--slave",
                   "--no-restore",
                   paste0("--file=", code),
                   "--args",
                   if (!is.null(args)) args_string, 
                   sep=" ")
      
  }
  
  # Submit the command to the system
  if (Sys.info()[1] == "Linux") {
    system(sub) 
  } else {
    cat(paste("\n", sub, "\n\n "))
    flush.console()
  }
}


##########################################################