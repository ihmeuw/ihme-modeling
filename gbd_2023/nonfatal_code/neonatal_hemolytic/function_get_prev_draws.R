get_prev_draws <- function(name, params, path_params_global) {
  path_params <- paste0("params_", name, ".rds")
  readr::write_rds(params, file = path_params)
  nch::submit_job(
    script = "draws.R",
    script_args = c(path_params_global, path_params),
    job_name = paste0(name, "_draws"),
    memory = 40,
    ncpus = 8
  )
}
