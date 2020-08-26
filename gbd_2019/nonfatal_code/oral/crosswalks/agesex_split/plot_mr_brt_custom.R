plot_mr_brt_custom <- function(model_object, continuous_vars = NULL, dose_vars = NULL, print_cmd = FALSE) {
  
  library("ihme", lib.loc = "FILEPATH")
  
  wd <- model_object[["working_dir"]]
  
  contvars <- paste(continuous_vars, collapse = " ")
  dosevars <- paste(dose_vars, collapse = " ")
  
  if (!file.exists(paste0("FILEPATH"))) {
    stop(paste0("No model outputs found at '", wd, "'"))
  }
  
  contvars_string <- ifelse(
    !is.null(continuous_vars), paste0("--continuous_variables ", contvars), "" )
  dosevars_string <- ifelse(
    !is.null(dose_vars), paste0("--dose_variable ", dosevars), "" )
  
  cmd <- paste(
    c("export PATH='FILEPATH'",
      "source /ihme/code/evidence_score/miniconda3/bin/activate mr_brt_env",
      # "cd /home/j/temp/rmbarber/MetaRegression", # dev
      paste(
        "python FILEPATH",
        "--mr_dir", wd, "--plot_note Bundle:", bundle, ", crosswalk:", cv, ", trim:", trim_percent,
        contvars_string,
        dosevars_string
      )
    ), collapse = " && "
  )
  
  print(cmd)
  cat("To generate plots, run the following command in a qlogin session:")
  cat("\n\n", cmd, "\n\n")
  cat("Outputs will be available in:", wd)
  
  path <- paste0(wd, "tmp.txt")
  readr::write_file(paste("/bin/bash -c", cmd), path)
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=2G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=00:05:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q all.q"
  shell_script <- path
  script <- path
  errors_flag <- paste0("FILEPATH")
  outputs_flag <- paste0("FILEPATH")
  job_text <- strsplit(wd, "/")
  job_name <- paste0("-N funnel_",job_text[[1]][7] )
  
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script)
  
  system(job)
  
}
