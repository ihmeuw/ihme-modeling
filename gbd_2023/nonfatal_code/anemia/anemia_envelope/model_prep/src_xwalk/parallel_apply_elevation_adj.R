# source libraries --------------------------------------------------------

library(data.table)

source(file.path(getwd(), "model_prep/src_xwalk/apply_elevation_adjustment.R"))

# load in data ------------------------------------------------------------

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'

anemia_map <- fread(file.path(getwd(), "model_prep/param_maps/anemia_map.csv"))

if(interactive()){
  r <- 15
}else{
  r <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
}

# apply elevation adjustments ---------------------------------------------

message(paste(r, bundle_map$id[r]))
measure_file_path <- file.path(
  output_dir,
  bundle_map$id[r],
  'square_measure_impute2.csv'
)
elevation_adj_measure_path <- file.path(
  output_dir,
  bundle_map$id[r],
  'elevation_adj_measure2.csv'
)
measure_adj_df <- fread(measure_file_path)
post_measure_df <- impute_missing_elevation_adj(
  input_df = measure_adj_df,
  bundle_map = bundle_map,
  anemia_map = anemia_map,
  bundle_dir = output_dir
)
fwrite(
  x = post_measure_df,
  file = elevation_adj_measure_path
)
