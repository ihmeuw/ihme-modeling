
# Prep environment --------------------------------------------------------

invisible(sapply(list.files("cod_pipeline/src_functions/", full.names = T), source))

# Set parameters ----------------------------------------------------------

if(!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))  # retrieve array task ID
} else{
  param_map_filepath <- PARAM_MAP_FP
  task_id <- 1L
}

# read in global param map
param_map <- qs::qread(param_map_filepath)
dr_locs <- param_map$dr_loc_id
loc <- dr_locs[task_id]

# read in version map
version_map <- readxl::read_excel(param_map$version_map_path)


for (i in seq_len(nrow(version_map))) {
  map_row <- version_map[i,]
  sex <- map_row$sex_id
  cause <- map_row$cause_id
  
  # create directories if needed
  path <- file.path(param_map$synth_check_path, cause, sex)
  fs::dir_create(path, recurse = TRUE)
  
  version_id <- version_map$dr_version_id[version_map$sex_id == sex &
                                            version_map$cause_id == cause]
  message(glue::glue("Getting DR synth draws: cause {cause} sex {sex} loc {loc}"))
  synth_df_rate <- get_synth_draws(all_years = param_map$year_ids, 
                                   loc_id = loc, 
                                   sex_id = sex, 
                                   cause = cause, 
                                   version_id = version_id, 
                                   release_id = param_map$release_id)
  message(glue::glue("Finished getting: cause {cause} sex {sex} loc {loc}"))
  
  # output result
  fp <- fs::dir_create(param_map$out_dir, "dr_files", cause, sex)
  qs::qsave(synth_df_rate, file.path(fp, glue::glue("/dr_hemog_{loc}.qs")))
  
  # write out loc check
  loc_dt <- data.table::data.table(location_id = loc)
  saveRDS(loc_dt, paste0(param_map$synth_check_path, "/", cause, "/", sex, "/loc_", loc, ".RDS"))
}
message("Finished outputting synth draws for data rich locations")