# Setup -------------------------------------------------------------------
library("data.table")
source("cod_pipeline/0_set_global_params.R") 

# Set log path for the various job submissions ----------------------------
log_path <- fs::path(LOG_ROOT, Sys.getenv("USER"), "hemog_cod")

# 1. Pooling --------------------------------------------------------------
pooling_jid <- nch::submit_job(
  script = "cod_pipeline/1_run_pooling.R",
  script_args = param_map_filepath,
  job_name = "pooling_other_hemog_618",
  memory = 30,
  ncpus = 1,
  time = 30,
  error = file.path(log_path, "pooling/errors/%x-%A"),
  output = file.path(log_path, "pooling/output/%x-%A")
)
message("Pooling job id ", pooling_jid, " submitted")

# 2. Pull and interpolate DisMod results ----------------------------------
hold2 <- nch::submit_job(
  script = "cod_pipeline/2_run_interpolation.R",
  script_args = param_map_filepath,
  job_name = "interpolate_hemog", 
  memory = 30,
  ncpus = 1,
  time = 30,
  array = paste0("1-", length(global_param_map$location_id) + 2, "%50"),
  partition = "all.q",
  error = file.path(log_path, "interpolate/errors/%x-%a-%A"),
  output = file.path(log_path, "interpolate/output/%x-%a-%A"),
  dependency = pooling_jid
)
message("Interpolate job id ", hold2, " submitted")

if (global_param_map$save_interp == TRUE) {
  ready_to_save <- hold2
}

# 3. Get CoD draws for data rich locations --------------------------------

if (global_param_map$synthesize_results == TRUE) {
  hold3 <- nch::submit_job(
    script = "cod_pipeline/3_run_dr_draws.R",
    script_args = param_map_filepath,
    job_name = "dr_hemog",
    memory = 10,
    ncpus = 5,
    time = 30,
    partition = "long.q",
    error = file.path(log_path, "dr_draws/errors/%x-%a-%A"),
    output = file.path(log_path, "dr_draws/output/%x-%a-%A"),
    array = paste0("1-", length(global_param_map$dr_loc_id), "%20"),
    dependency = hold2
  )
  message("DR file output job id ", hold3, " submitted")
  
# 4. Synthesize interpolated and dr files ----------------------------------
  ready_to_save <- nch::submit_job(
    script = "cod_pipeline/4_run_synthesis.R",
    script_args = param_map_filepath,
    job_name = "synth_hemog",
    memory = 10,
    ncpus = 5,
    time = 30,
    partition = "long.q",
    error = file.path(log_path, "synthesis/errors/%x-%A"),
    output = file.path(log_path, "synthesis/errors/%x-%A"),
    dependency = hold3
  )
} else {
  hold3 <- hold2
  message("Skipping synthesis of DisMod and CoD results")
}


# 5 Get parent model draws results --------------------------------------------------------
if (global_param_map$scale_results == TRUE) {
  if (global_param_map$save_synth_scaled == TRUE) {
    hold4 <- nch::submit_job(
      script = "cod_pipeline/5_get_parent_model_draws.R",
      script_args = param_map_filepath,
      job_name = "hemog_get_parent_draws",
      memory = 10,
      ncpus = 2,
      time = 30,
      partition = "long.q",
      array = paste0("1-", length(global_param_map$location_id), "%25"),
      error = file.path(log_path, "scaling/errors/%x-%a-%A"),
      output = file.path(log_path, "scaling/output/%x-%a-%A"),
      dependency = hold3
    )
    message("Get parent model draws job id ", hold4, " submitted")
  }
} else {
  message("Not getting parent model draws for scaling")
}

# 6. Scale results --------------------------------------------------------
if (global_param_map$scale_results == TRUE) {
  if (global_param_map$save_synth_scaled == TRUE) {
    ready_to_save <- nch::submit_job(
      script = "cod_pipeline/6_run_scaling.R",
      script_args = param_map_filepath,
      job_name = "scaling_hemog",
      memory = 10,
      ncpus = 2,
      time = 30,
      partition = "long.q",
      array = paste0("1-", length(global_param_map$location_id), "%25"),
      error = file.path(log_path, "scaling/errors/%x-%a-%A"),
      output = file.path(log_path, "scaling/output/%x-%a-%A"),
      dependency = hold4
    )
    message("Scaling job id ", ready_to_save, " submitted")
  }
} else {
  message("Skipping output of scaled draws")
}

# 7. Save results --------------------------------------------------------
for (cause in global_param_map$cause_id) {
  for (sex in global_param_map$sex_id) {
    save_job <- nch::submit_job(
      script = "cod_pipeline/7_run_save_hemog.R",
      script_args = c(param_map_filepath, cause, sex),
      job_name = paste0("saving_hemog_", cause, "_", sex),
      memory = 95,
      ncpus = 10,
      time = 600,
      partition = "long.q",
      error = file.path(log_path, "saving/errors/%x-%A"),
      output = file.path(log_path, "saving/output/%x-%A"),
      dependency = ready_to_save,
      email = paste0(Sys.getenv('USER'), "@uw.edu")  
    )
    message("Saving job id ", save_job, 
            glue::glue(" submitted for cause {cause} sex {sex}"))
  }
}
    