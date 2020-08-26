#####################################################################################################################################################################################
#####################################################################################################################################################################################
#' @Author: 
#' @DateUpdated: 11/20/2018
#' @Description: Runs the steps involved in cod/epi custom modeling submitted from 00_master.do file; [Do not make edits here] 
#' 
#' @param code_dir The path to the folder with your .do files and the excel file containing steps information. This excel file indicates how each step should be run, and dependencies 
#'                 amongst steps. 
#'                 NOTE: Unlike in GBD2013, we assume your .R files live in the same directory as your excel config file. The excel file name must end in "steps.xlsx"
#' @param out_dir  Define directory that will contain log files
#' @param tmp_dir  Define directory on clustertmp that holds intermediate draws, and ODE inputs/outputs
#' @param date     Date of the run in format YYYY_MM_DD: 2018_11_20. This is the string that will become the name of the parent folder.
#' @param steps    The string indicating which step you want to run. If step == "_all", all steps will be run in sequence.
#' @param in_dir   This directory stores the input files that are called by steps
#' 
#' @Implementation: This steps template is very similar to the GBD2013 template, but it doesn't make assumptions about where files should be stored.  Since functional_groups no longer 
#'                  exist, it is up to the modeler to decide where to save files. But inside the out_dir and tmp_dir, the remaining folder structure is the same as in 
#'                  GBD2013 (to help reduce code rewriting).
#'                  
#####################################################################################################################################################################################
#####################################################################################################################################################################################

model_custom <- function(code_dir, out_dir, tmp_dir, date, steps, in_dir, ds, loc) {
  # Load functions and packages
  library(zip)
  library(openxlsx)
  source(paste0(code_dir, "helper_functions/qsub_new.R"))
  source(paste0(code_dir, "helper_functions/argparser.R"))
  source(paste0(k, "current/r/get_demographics.R"))
  source(paste0(k, "current/r/get_location_metadata.R"))
  
  # generate location data and save it as an R object
  if (loc[1] == "all") {
    loc.meta <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)
    locations <- loc.meta[most_detailed == 1 & is_estimate == 1, unique(location_id)]
  } else {
    locations <- loc
  }
  saveRDS(locations, file.path(in_dir,"locations_temp.rds"))
  
  # generate demographics data
  demographics <- get_demographics(gbd_team="epi", gbd_round_id=6)
  # save it as an R object
  saveRDS(demographics, file.path(in_dir,"demographics_temp.rds"))
  
  # Load steps template
  step_sheet <- read.xlsx(file.path(code_dir,"meningitis_steps.xlsx"), sheet="steps")
  step_sheet <- step_sheet[!is.na(step_sheet$step), ]
  step_sheet$step <- as.character(step_sheet$step)
  step_sheet$name <- as.character(step_sheet$name)
  step_sheet$hold <- as.character(step_sheet$hold)
  
  # Get list of parent steps (i.e. numbered steps only)
  step_sheet$step_num <- gsub("[a-z]", "", step_sheet$step)
  step_num <- unique(step_sheet$step_num)
  
  # Get last step(s) in last_steps local
  s_num <- as.numeric(step_num)
  maxnum <- max(s_num)
  last_steps <- c("09")
  # Generate default hold lists for submitting parallel jobs
  nparent_steps <- length(s_num) 
  hold_on_steps <- new.env(hash=TRUE) # Hash table that for each step stores which previous steps to wait until they finish before running
  
  for (i in seq(2,nparent_steps)) {
    current_step <- step_num[[i]]
    previous_step <- step_num[[i-1]]
    # Add previous steps to the hold for the current parent step if step belongs to immediately anterior parent step AND it is one of the steps you are running
    for (substep in steps) {
      if (grepl(previous_step, substep) & any(grepl(previous_step, steps))) {
        if (is.null(hold_on_steps[[current_step]])) {
          hold_on_steps[[current_step]] <- c(substep)
        } else {
          hold_on_steps[[current_step]] <- c(hold_on_steps[[current_step]], substep) 
        }
      }
    }
  }
  
  for (s in steps) {
    name <- step_sheet$name[step_sheet$step == s]
    fullname <- paste0(s, "_", name)
    # Get parent step if step has multiple substeps (i.e. a,b,c)
    parent_step <- gsub("[a-z]", "", s)
      
    # Override default hold list if specified or use default hold list for parent step
    h <- step_sheet$hold[step_sheet$step == s]
    if (is.na(h)) {
      holds <- hold_on_steps[[parent_step]]
    } else {
      # Since we are pulling the hold list from the excel file here, we need to string split by spaces (i.e. \\s+)
      holds <- strsplit(h, "\\s+")[[1]]
    }
    
    # Create step directories
    for (dir in c(out_dir, tmp_dir)) {
      dir.create(file.path(dir, fullname),                           showWarnings = F)
      dir.create(file.path(dir, fullname, "01_inputs"),              showWarnings = F)
      dir.create(file.path(dir, fullname, "02_temp/01_code/checks"), showWarnings = F, recursive = T)
      dir.create(file.path(dir, fullname, "02_temp/02_logs"),        showWarnings = F, recursive = T)
      dir.create(file.path(dir, fullname, "02_temp/03_data"),        showWarnings = F, recursive = T)
      dir.create(file.path(dir, fullname, "02_temp/04_diagnostics"), showWarnings = F, recursive = T)
      dir.create(file.path(dir, fullname, "03_outputs/01_draws"),    showWarnings = F, recursive = T)
      dir.create(file.path(dir, fullname, "03_outputs/02_summary"),  showWarnings = F, recursive = T)
      dir.create(file.path(dir, fullname, "03_outputs/03_other"),    showWarnings = F, recursive = T)
    }
    
    # Delete finished.txt file for step if rerunning
    if (file.exists(file.path(out_dir, fullname, "finished.txt"))) { 
      file.remove(file.path(out_dir, fullname, "finished.txt")) 
    }
    
    # -------------------------------------------------------------------------- Submit job ----------------------------------------------------------------------------
    # Define qsub arguments
    my_job_name <- paste0("step_", s)
    my_project <- "proj_hiv"

    
    # Get memory needed for job (default 2G) and assign slots accordingly
    my_mem <- step_sheet$mem[step_sheet$step == s]
    if (my_mem == "") my_mem <- 2
    my_fthread <- ceiling(my_mem / 2)
    my_mem <- paste0(my_mem, "G")
    # my_time <- "24:00:00"
    # test
    my_time <- "2:00:00"
    
    if (!is.null(holds)) {
      # Format holds local into argument for submission to sge as wc_job_list (see man sge_types)
      # For example: "01a" "01b" -> "step_01a step_01b"
      my_holds <- paste0("step_", holds)
      my_holds <- paste(my_holds, collapse = ',')
    } else {
      my_holds <- NULL
    }
    
    shell_file <- paste0(code_dir, "r_shell.sh") # Should be updated to modeler's preferred R shell file
    parent_script <- file.path(code_dir, paste0(fullname, ".R"))
    
    # Define arguments for R script
    my_args <- argparser(
      root_j_dir = out_dir,
      root_tmp_dir = tmp_dir,
      date = date,
      step_num = s,
      step_name = name,
      hold_steps = my_holds,
      last_steps = last_steps,
      code_dir = code_dir,
      in_dir = in_dir,
      ds = ds
    )
    
    qsub_new(
      job_name = my_job_name,
      project = my_project,
      mem = my_mem,
      fthread = my_fthread,
      time = my_time,
      holds = my_holds,
      shell_file = shell_file,
      script = parent_script,
      args = my_args
    )
  }
  
}