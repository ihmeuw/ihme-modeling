
# set global variables ----------------------------------------------------

output_dir <- "FILEPATH"

# launch job for saving map data ------------------------------------------

map_job_id <- nch::submit_job(
  script = file.path(getwd(), "elevation/admin_locations/save_map_data.R"),
  script_args = output_dir,
  memory = 40,
  ncpus = 10,
  time = 15,
  archive = T,
  partition = "all.q",
  job_name = "save_elevation_maps"
)

# submit weighted elevation jobs ------------------------------------------

year_vec <- c(1980:2021)
array_string <- paste0("1-", length(year_vec), "%", length(year_vec))

admin2_job_id <- nch::submit_job(
  script = file.path(getwd(), "elevation/admin_locations/admin_elevation_calc_script.R"),
  script_args = output_dir,
  memory = 40,
  ncpus = 15,
  time = 60 * 70,
  array = array_string,
  archive = T,
  partition = "all.q",
  job_name = "admin2_elevation_calc",
  dependency = map_job_id
)

# submit job for aggregating admin 1 and admin 0 units --------------------

nch::submit_job(
  script = file.path(getwd(), "elevation/admin_locations/admin_0_1_calc_script.R"),
  script_args = output_dir,
  memory = 10,
  ncpus = 2,
  time = 2,
  archive = F,
  partition = "all.q",
  job_name = "aggregate_admin_1_0",
  dependency = admin2_job_id
)
