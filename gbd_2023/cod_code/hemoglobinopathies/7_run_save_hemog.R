# Save cod results based on custom model type as defined in the global param map

source("cod_pipeline/src_functions/paths.R") 

# Set parameters ----------------------------------------------------------

if(!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  cause_id <- args[2]
  sex_id <- args[3]
} else{
  param_map_filepath <- PARAM_MAP_FP
  cause_id <- 614
  sex_id <- 1
}

param_map <- qs::qread(param_map_filepath)
param_map$cause_id <- cause_id
param_map$sex_id <- sex_id

# Convert files to .csv file format as needed -----------------------------

conversion_dir <- c() 
if (param_map$save_interp == TRUE) {
  conversion_dir <- c(conversion_dir, file.path(param_map$out_dir, "interp_files"))  
}
if (param_map$save_synth == TRUE) {
  conversion_dir <- c(conversion_dir, file.path(param_map$out_dir, "synth_files"))
}
for (path in conversion_dir) {
  cli::cli_progress_step(
    paste0("Converting QS files to CSV in ", 
           glue::glue("{path}/{cause}/{sex}...")),
    msg_done = "Completed file conversion from QS to CSV"
  )
  convert_qs_to_csv(
    source_dir = file.path(path, cause, sex),
    target_dir = file.path(path, cause, sex)
  )
  cli::cli_progress_done()
}

# Save synthesized results ------------------------------------------------

if (param_map$save_synth == TRUE){
  for (cause in param_map$cause_id) {
    for (sex in param_map$sex_id) {
      message(paste("Starting to save synthesized results for", cause, sex))
      # set cause- and sex-specific directory path and save description
      input_dir <- file.path(param_map$out_dir, "synth_files", cause, sex)
      if (cause == 614) {
        if (sex_id == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex_id == 2) {
          save_description <- "DESCRIPTION"
        }
      } else if (cause == 615) {
        if (sex_id == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex_id == 2) {
          save_description <- "DESCRIPTION"
        }
      } else if (cause == 616) {
        if (sex_id == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex_id == 2) {
          save_description <- "DESCRIPTION"
        }
      } else if (cause == 618) {
        if (sex_id == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex_id == 2) {
          save_description <- "DESCRIPTION"
        }
      } else {
        save_description <- "Unknown_Cause"
        message("Cause ID not recognized, save description set to Unknown_Cause")
      }
      
      # submit save jobs for each unique cause-sex combination
      ihme::save_results_cod(
        cause_id = cause,
        input_dir = input_dir,
        input_file_pattern = "synth_hemog_{location_id}.csv",
        description = save_description,
        year_id = param_map$year_ids,
        sex_id = sex,
        metric_id = 3,
        release_id = param_map$release_id,
        mark_best = param_map$mark_best_flag)
      
      message(paste("Saved synthesized results for", cause, sex))
    }
  }
} else {
  message(paste("Synthesized results not saved; to save synthesized results,
                update global parameter map and rerun saving script"))
}

# Save synthesized and scaled results -------------------------------------

if (param_map$save_synth_scaled == TRUE){
  for (cause in param_map$cause_id) {
    for (sex in param_map$sex_id) {
      message(paste("Starting to save synth-scaled results for", cause, sex))
      # set cause- and sex-specific directory path and save description
      input_dir <- file.path(param_map$out_dir, "synth_scaled", cause, sex)
      if (cause == 614) {
        if (sex == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex == 2) {
          save_description <- "DESCRIPTION"
        }
      } else if (cause == 615) {
        if (sex == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex == 2) {
          save_description <- "DESCRIPTION"
        }
      } else if (cause == 616) {
        if (sex == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex == 2) {
          save_description <- "DESCRIPTION"
        }
      } else if (cause == 618) {
        if (sex == 1) {
          save_description <- "DESCRIPTION"
        } else if (sex == 2) {
          save_description <- "DESCRIPTION"
        }
      } else {
        save_description <- "Unknown_Cause"
        message("Cause ID not recognized, save description set to Unknown_Cause")
      }
      
      # submit save jobs for each unique cause-sex combination
      ihme::save_results_cod(
        cause_id = cause,
        input_dir = input_dir,
        input_file_pattern = "synth_scaled_hemog_{location_id}.csv",
        description = save_description,
        year_id = param_map$year_ids,
        sex_id = sex,
        metric_id = 3,
        release_id = param_map$release_id,
        mark_best = param_map$mark_best_flag)
      
      message(paste("Saved synthesized-scaled results for", cause, sex))
    }
  }
} else {
  message(paste("Synthesized-scaled results not saved; to save 
                synthesized-scaled results, update global parameter map and 
                rerun saving script"))
}

message(paste("Finished saving all requested results"))