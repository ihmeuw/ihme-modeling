## Submit job for line plot comparing versions/scenarios ##

.submit_plot_job <- function(
  plot_script_path, # full path to the plotting script e.g. file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "vaccination_lines_check.R")
  current_version, # current model version to plot e.g. 2021_08_10.01
  compare_version = NULL # if plot does comparison to a previous version provide here, otherwise leave NULL
) {
  source(paste0("FILEPATH", Sys.info()["user"], "FILEPATH/paths.R"))

  if (is.null(compare_version)) compare_version <- current_version

  tmp <- unlist(strsplit(plot_script_path, "[/.]"))
  job_name <- tmp[length(tmp) - 1]

  .submit_job(
    script_path = plot_script_path,
    job_name = job_name,
    mem = "10G",
    archiveTF = TRUE,
    threads = "2",
    runtime = "20",
    Partition = "d.q",
    Account = "proj_covid",
    args_list = list(
      "--current_version" = current_version,
      "--compare_version" = compare_version
    )
  )
}
