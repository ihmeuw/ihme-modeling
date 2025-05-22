
library(data.table)

# create launch script for mr-brt adjustments -----------------------------

config_file_path <- file.path(getwd(), "mrbrt/square_measure_type/config_map.csv")
config_map <- fread(config_file_path)

output_dir <- "FILEPATH"

new_run <- TRUE
job_throttle <- 50

if(new_run){
  array_string <- paste0("1-", nrow(config_map), "%", job_throttle)
}else{
  array_string <- paste0('121-128', "%", job_throttle)
}

nch::submit_job(
  script = file.path(getwd(), "mrbrt/square_measure_type/measure_square_mrbrt.R"),
  memory = 20,
  ncpus = 6,
  time = 60 * 5,
  array = array_string,
  script_args = c(config_file_path, output_dir),
  job_name = "square_measure_mrbrt"
)
