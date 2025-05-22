
# source libraries --------------------------------------------------------

library(data.table)

source(file.path(getwd(), "model_prep/src_xwalk/apply_measure_adjment.R"))

# load in data ------------------------------------------------------------

bundle_map <- fread(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))
output_dir <- 'FILEPATH'

anemia_map <- fread(file.path(getwd(), "model_prep/param_maps/anemia_map.csv"))

if(interactive()){
  r <- 7
}else{
  r <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
}

# apply measure adjustments -----------------------------------------------

message(paste(r, bundle_map$id[r]))
measure_file_path <- file.path(
  output_dir,
  bundle_map$id[r],
  'square_measure_impute2.csv'
)
age_sex_file_path <- file.path(
  output_dir,
  bundle_map$id[r],
  'age_sex_split_mrbrt.csv'
)
age_sex_split_df <- fread(age_sex_file_path)
post_measure_df <- impute_missing_measure(
  input_df = age_sex_split_df,
  bundle_map = bundle_map,
  anemia_map = anemia_map,
  bundle_dir = output_dir
)
fwrite(
  x = post_measure_df,
  file = measure_file_path
)
