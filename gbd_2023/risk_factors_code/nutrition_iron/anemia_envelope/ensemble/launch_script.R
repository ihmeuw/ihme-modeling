
# define constants --------------------------------------------------------

adj_type <- 'who'
me_map_file_name <- file.path(getwd(), 'ensemble/me_map.yaml')
me_map <- yaml::read_yaml(me_map_file_name)

gbd_rel_id <- 16

threshold_values <- list(0.004, 0.052, 0.149)
names(threshold_values) <- names(me_map[[adj_type]][2:4])

cluster_partiton <- 'long.q'

# load in demographics meta data ------------------------------------------

demographics <- ihme::get_demographics(
  gbd_team = 'epi', 
  release_id = gbd_rel_id
)

# create parameter map ----------------------------------------------------

params <- list(
  adj_type = adj_type, # current elevation adjustment type
  age_group_id = demographics$age_group_id, # ages to pull in 
  sex_id = demographics$sex_id, # sexes to pull in
  year_id = 1980:2024, # years to pull in
  xmin = 20, # minimum hemoglobin value
  xmax = 220, # maximum hemoglobin value
  min_sd = 0.5, # minimum SD value
  max_sd = 55, # maximum SD value
  threshold_weights = threshold_values, # weights of thresholds to be used 
  draws_path = 'FILEPATH', # location of draws files from ST-GPR
  me_map_path = me_map_file_name, # ME ID map
  pregnancy_population_file = 'FILEPATH/pregnancy_population/preg_pop.fst', # location of pregnancy population prop file
  num_draws = 1000, # number of draws to use
  me_id_list = me_map[[adj_type]], # ME IDs used in get draws
  gbd_rel_id = gbd_rel_id # gbd release ID
)
params$x <- seq(params$xmin, params$xmax, length.out = 1000) # hemoglobin distribution array

params_path <- file.path(getwd(), 'ensemble/params.rds')
saveRDS(params, params_path, compress = FALSE)

# create param map of all locations ---------------------------------------

loc_param_map <- data.frame(
  location_id = demographics$location_id
)
loc_id_path <- file.path(getwd(), 'ensemble/locs.csv')
write.csv(
  x = loc_param_map,
  file = loc_id_path,
  row.names = FALSE
)

# launch prep draws -------------------------------------------------------

array_string <- paste0("1-", length(me_map[[adj_type]]), '%2')

get_draws_job_id <- nch::submit_job(
  script = file.path(getwd(), "ensemble/prep_draws.R"),
  script_args = params_path,
  job_name = 'prep_draws_for_ens',
  memory = 100,
  ncpus = 30,
  time = 60 * 2,
  partition = cluster_partiton,
  array = array_string
)

# launch ensemble job -----------------------------------------------------

out_dir <- 'FILEPATH'

array_string <- paste0('1-', nrow(loc_param_map), '%', 100)

ensemble_job_id <- nch::submit_job(
  script = file.path(getwd(), 'ensemble/parallel_script.R'),
  script_args = c(loc_id_path, params_path, out_dir),
  job_name = 'updated_brinda_ensemble',
  memory = 20,
  time = 60 * 2,
  ncpus = 10,
  array = array_string,
  partition = cluster_partiton,
  dependency = if(exists('get_draws_job_id')) get_draws_job_id else NULL
) 

# get pregnancy data ------------------------------------------------------

pregnancy_pop_job_id <- nch::submit_job(
  script = file.path(getwd(), "ensemble/get_pregnant_population.R"),
  job_name = 'get_preg_data',
  script_args = params_path,
  memory = 4,
  ncpus = 2,
  time = 10,
  partition = cluster_partiton,
  dependency = if(exists('ensemble_job_id')) ensemble_job_id else NULL
)

# incorporate pregnancy data ----------------------------------------------

array_string <- paste0('1-', nrow(loc_param_map), '%100')

get_preg_anemia_job_id <- nch::submit_job(
  script = file.path(getwd(), "ensemble/get_pregnancy_anemia_prevalence.R"),
  job_name = 'get_preg_anemia',
  script_args = c(params_path, loc_id_path, out_dir),
  memory = 10,
  ncpus = 4,
  time = 30,
  array = array_string,
  dependency = if(exists('pregnancy_pop_job_id')) pregnancy_pop_job_id else NULL
)

