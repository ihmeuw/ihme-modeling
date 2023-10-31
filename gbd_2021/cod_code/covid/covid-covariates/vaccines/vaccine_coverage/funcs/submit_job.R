
.submit_job <- function(
  script_path, # full path to the plotting script e.g. file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "vaccination_lines_check.R")
  job_name = NULL, # Will be name of script if NULL.
  mem = "10G", # memory
  archiveTF = TRUE, # Expects T/F
  threads = "2", # threads 
  runtime = "40", # runtime, min
  Partition = "d.q", # partition 
  Account = "proj_covid", # account
  args_list = NULL, # optional list() of arguments, e.g. list("--arg1" = arg1, "--arg2" = arg2)
  use_vax_path_image = T, # Use the image saved to the environment from paths.R?
  r_image = NULL # Will use this image if not using the saved image and if not NULL. If not using saved image and NULL, will pass no image argument and use default.
) {
  
  source(paste0("FILEPATH", Sys.info()["user"], "FILEPATH/paths.R")) # paths

  if (is.null(job_name)) {
    tmp <- unlist(strsplit(script_path, "[/.]"))
    job_name <- toupper(tmp[length(tmp) - 1])
  }
  if (use_vax_path_image) {
    r_image = R_IMAGE_PATH
  }

  command <- paste0(
    "sbatch",
    " -J ", job_name,
    " --mem=", mem,
    ifelse(archiveTF, " -C archive", ""),
    " -c ", threads,
    " -t ", runtime,
    " -p ", Partition,
    " -A ", Account,
    " -e ", file.path(SLURM_ERROR_PATH, "%x.e%j"),
    " -o ", file.path(SLURM_OUTPUT_PATH, "%x.o%j"),
    " ", R_SHELL_PATH,
    ifelse(is.null(r_image), "", paste0(" -i ", r_image)),
    " -s ", script_path
  )

  for (arg_name in names(args_list)) { # append extra arguments
    command <- paste(command, arg_name, args_list[arg_name])
  }

  submission_return <- system(SYSTEM_COMMAND)
  message(paste("Cluster job submitted:", job_name, "; Submission return code:", submission_return))
  
  return(submission_return)
}
