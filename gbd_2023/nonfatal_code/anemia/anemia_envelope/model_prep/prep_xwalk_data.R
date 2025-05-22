
# source libraries --------------------------------------------------------

library(data.table)

# load in all bundle data and perform the following: ----------------------

# 1) age/sex split
# 2) adjust for elevation by:
#    a) assign anemia category, raw prev, and elevation category for prevalence
#    b) apply the BRINDA adjustment for hemoglobin
# 3) square by elevation adjustment type

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'
cluster_parition <- 'long.q'

# age/sex split -----------------------------------------------------------

mem_per_job <- 50
cpu_per_job <- 25
num_parallel_jobs <- min(ceiling(1000 / mem_per_job), nrow(bundle_map))

array_string <- paste0("1-", nrow(bundle_map), "%", num_parallel_jobs)
#array_string <- '3,13'

age_sex_job_id <- nch::submit_job(
  script = file.path(getwd(), "model_prep/src_xwalk/parallel_age_sex_split.R"),
  memory = mem_per_job,
  ncpus = cpu_per_job,
  array = array_string,
  time = 60 * 5,
  partition = cluster_parition,
  job_name = 'bundle_age_sex_split'
)

# adjust for measure ------------------------------------------------------

mem_per_job <- 30
cpu_per_job <- 20
num_parallel_jobs <- min(ceiling(1000 / mem_per_job), nrow(bundle_map))

array_string <- paste0("1-", nrow(bundle_map), "%", num_parallel_jobs)

measure_adj_job_id <- nch::submit_job(
  script = file.path(getwd(), "model_prep/src_xwalk/parallel_apply_measure_adj.R"),
  memory = mem_per_job,
  ncpus = cpu_per_job,
  array = array_string,
  time = 60 * 5,
  partition = cluster_parition,
  job_name = 'apply_measure_adj',
  dependency = if(exists('age_sex_job_id')) age_sex_job_id else NULL
)

# adjust for elevation adj type --------------------------------------------

mem_per_job <- 30
cpu_per_job <- 20
num_parallel_jobs <- min(ceiling(1000 / mem_per_job), nrow(bundle_map))

array_string <- paste0("1-", nrow(bundle_map), "%", num_parallel_jobs)

elevation_adj_job_id <- nch::submit_job(
  script = file.path(getwd(), "model_prep/src_xwalk/parallel_apply_elevation_adj.R"),
  memory = mem_per_job,
  ncpus = cpu_per_job,
  array = array_string,
  time = 60 * 5,
  partition = cluster_parition,
  job_name = 'apply_elevation_adj',
  dependency = if(exists('measure_adj_job_id')) measure_adj_job_id else NULL
)

# aggregate duplicate data ------------------------------------------------

mem_per_job <- 30
cpu_per_job <- 20
num_parallel_jobs <- min(ceiling(1000 / mem_per_job), nrow(bundle_map))

array_string <- paste0("1-", nrow(bundle_map), "%", num_parallel_jobs)

aggregate_job_id <- nch::submit_job(
  script = file.path(getwd(), "model_prep/src_xwalk/aggregate_aslyn.R"),
  memory = mem_per_job,
  ncpus = cpu_per_job,
  array = array_string,
  time = 60 * 5,
  partition = cluster_parition,
  job_name = 'aggregate_duplicate_data',
  dependency = if(exists('elevation_adj_job_id')) elevation_adj_job_id else NULL
)

# update prevalence values ------------------------------------------------

measure_preg_df <- unique(bundle_map[,.(elevation_adj_type, cv_pregnant)])
measure_preg_file_path <- file.path(getwd(), 'model_prep/param_maps/measure_preg_df.csv')
fwrite(
  x = measure_preg_df,
  file = measure_preg_file_path
)

mem_per_job <- 20
cpu_per_job <- 6

array_string <- paste0("1-", nrow(measure_preg_df), "%", nrow(measure_preg_df))

update_prev_job_id <- nch::submit_job(
  script = file.path(getwd(), 'model_prep/src_xwalk/update_prevalence_values.R'),
  script_args = c(measure_preg_file_path, output_dir),
  memory = mem_per_job,
  ncpus = cpu_per_job,
  time = 5,
  array = array_string,
  partition = cluster_parition,
  job_name = 'update_prevalence_values',
  dependency = if(exists('aggregate_job_id')) aggregate_job_id else NULL
)

# outlier data ------------------------------------------------------------

mem_per_job <- 10
cpu_per_job <- 2

array_string <- paste0("1-", nrow(measure_preg_df), "%", nrow(measure_preg_df))

outlier_job_id <- nch::submit_job(
  script = file.path(getwd(), 'model_prep/src_xwalk/outlier_data.R'),
  script_args = c(measure_preg_file_path, output_dir),
  memory = mem_per_job,
  ncpus = cpu_per_job,
  time = 5,
  array = array_string,
  partition = cluster_parition,
  job_name = 'outlier_anemia_data',
  dependency = if(exists('update_prev_job_id')) update_prev_job_id else NULL
)

# upload xwalk versions ---------------------------------------------------

mem_per_job <- 30
cpu_per_job <- 10
num_parallel_jobs <- min(ceiling(1000 / mem_per_job), nrow(bundle_map))

array_string <- paste0("1-", nrow(bundle_map), "%", num_parallel_jobs)
bundle_id_vec <- bundle_map[(elevation_adj_type == 'who' & cv_pregnant == 0), id] 
array_string <- paste(which(bundle_map$id %in% bundle_id_vec), collapse = ',')

anemia_xwalk_upload_job_id <- nch::submit_job(
  script = file.path(getwd(), "model_prep/src_xwalk/upload_xwalk.R"),
  memory = mem_per_job,
  ncpus = cpu_per_job,
  array = array_string,
  time = 60,
  partition = cluster_parition,
  archive = TRUE,
  job_name = 'anemia_xwalk_upload',
  dependency = if(exists('outlier_job_id')) outlier_job_id else NULL
)

# get current xwalk IDs ---------------------------------------------------

source(file.path(getwd(), 'model_prep/src_xwalk/get_current_xwalk_ids.R'))
bundle_map <- get_current_xwalk_ids(bundle_map)
fwrite(
  x = bundle_map,
  file = file.path(getwd(), "model_prep/param_maps/bundle_map.csv")
)
