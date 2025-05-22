
# source libraries --------------------------------------------------------

library(data.table)

# create parameter map ----------------------------------------------------

severity_vec <- c('mild', 'moderate', 'severe', 'mod_sev', 'anemic')
alt_vec <- c('raw', 'adj', 'who', 'brinda')

param_map <- data.table()
for(sev in severity_vec){
  anemia_measure_vec <- c()
  for(alt in alt_vec){
    anemia_measure_vec <- append(
      anemia_measure_vec,
      paste('anemia', sev, alt, sep = "_")
    )
  }
  temp_map <- CJ(
    ref_var = anemia_measure_vec,
    alt_var = anemia_measure_vec
  )
  temp_map <- temp_map[ref_var != alt_var]
  param_map <- rbindlist(
    list(param_map, temp_map), use.names = TRUE, fill = TRUE
  )
}

param_map_file_path <- file.path(getwd(), "mrbrt/elevation_adjust/param_map.csv")
fwrite(
  x = param_map, 
  file = param_map_file_path
)

output_dir <- "FILEPATH"

# submit job --------------------------------------------------------------

array_string <- paste0("1-", nrow(param_map), "%", 30)

nch::submit_job(
  script = file.path(getwd(), "mrbrt/elevation_adjust/elevation_adjust_mrbrt.R"),
  script_args = c(param_map_file_path, output_dir),
  memory = 30,
  ncpus = 8,
  time = 60 * 2,
  archive = FALSE,
  array = array_string,
  partition = "all.q",
  job_name = "elevation_mr_brt_data"
)
