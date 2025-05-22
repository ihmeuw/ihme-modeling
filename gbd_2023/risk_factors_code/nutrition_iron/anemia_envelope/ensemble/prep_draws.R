
# load in cluster args ----------------------------------------------------

if(interactive()){
  task_id <- 1
  map_file_name <- file.path(getwd(), 'ensemble/params.rds')
}else{
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- command_args[1]
}

params <- readRDS(map_file_name)

param_map <- yaml::read_yaml(params$me_map_path)[[params$adj_type]]

draws_output_dir <- file.path(
  params$draws_path, param_map[[task_id]]$modelable_entity_id
)

if(!(dir.exists(draws_output_dir))){
  dir.create(draws_output_dir)
}

demographic_list <- ihme::get_demographics(
  gbd_team = 'epi',
  release_id = params$gbd_rel_id
)

dat <- ihme::get_draws(
  gbd_id_type = 'modelable_entity_id',
  source = 'stgpr',
  gbd_id = param_map[[task_id]]$modelable_entity_id,
  version_id = param_map[[task_id]]$model_version_id,
  age_group_id = params$age_group_id,
  location_id = demographic_list$location_id,
  sex_id = params$sex_id,
  year_id = params$year_id,
  downsample = if(params$num_draws < 1000) TRUE else FALSE,
  n_draws = if(params$num_draws < 1000) params$num_draws else NULL,
  num_workers = future::availableCores(),
  release_id = params$gbd_rel_id
)

for(loc in demographic_list$location_id) {
  fst::write.fst(
    x = dat[location_id == loc],
    path = file.path(
      draws_output_dir, 
      paste0('draws_loc_', loc, '.fst')
    )
  )
}
