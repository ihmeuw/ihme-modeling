
# source libraries --------------------------------------------------------

library(data.table)

# source all helper functions ---------------------------------------------

invisible(sapply(
  list.files(
    path = file.path(getwd(), 'model_prep/src_bundle/'),
    pattern = "*\\.R$",
    full.names = TRUE
  ),
  source
))

# global variables --------------------------------------------------------

release_id <- 16

# load in collapsed microdata ---------------------------------------------

anemia_collapsed_total <- lapply(
  list.files(
    path = 'FILEPATH/',
    full.names = TRUE
  ),
  fread
) |> rbindlist(use.names = TRUE, fill = TRUE)

# load in vmnis data ------------------------------------------------------

bv_id <- 45615
vmnis <- ihme::get_bundle_version(bundle_version_id = bv_id)
vmnis$vmnis <- 1

# bind all data together --------------------------------------------------

df <- rbindlist(
  list(anemia_collapsed_total, vmnis),
  use.names = TRUE,
  fill = TRUE
)

# add NIDs that are present in current data set ---------------------------

df <- add_missing_nids(input_df = df)

# format df to only contain necessary columns is prepped for upload -------

df <- format_stgpr_bundle(
  input_df = df, 
  release_id = release_id, 
  hot_fix = TRUE
)

# append on elevation data ------------------------------------------------

df <- append_elevation(
  input_df = df,
  gbd_rel_id = release_id
)

# square by measure and adjustment type -----------------------------------

df <- create_mod_sev_anemia_prev(input_df = df)

all_measure_df <- fread(file.path(getwd(), "model_prep/param_maps/all_adj_types.csv"))
measure_var_map <- yaml::read_yaml(file.path(getwd(), "model_prep/param_maps/missing_var_map.yaml"))
elevation_adj_map <- yaml::read_yaml(file.path(getwd(), "model_prep/param_maps/missing_elevation_map.yaml"))

df <- square_bundle_data_measure(
  input_df = df,
  measure_adj_df = all_measure_df,
  var_map = measure_var_map
)

df <- square_bundle_data_elevation_adj(
  input_df = df,
  measure_adj_df = all_measure_df,
  var_map = elevation_adj_map
)

df <- create_placeholders_for_imputed_rows(
  input_df = df
)

fst::write_fst(
  x = df,
  path = 'FILEPATH/all_bundle_data.fst'
)

# upload bundles ----------------------------------------------------------

bundle_map_file_name <- file.path(getwd(), "model_prep/param_maps/bundle_map.csv")
bundle_map <- fread(bundle_map_file_name)

array_string <- paste0('1-', nrow(bundle_map), '%', nrow(bundle_map))

nch::submit_job(
  script = file.path(getwd(), 'model_prep/src_bundle/bundle_upload/upload_bundle_data.R'),
  job_name = 'upload_anemia_bundles',
  memory = 30,
  ncpus = 6,
  time = 60,
  partition = 'all.q',
  archive = T,
  array = array_string
)

# get current bv ids ------------------------------------------------------

bundle_map <- get_current_bv_ids(input_bundle_map = bundle_map)
fwrite(
  x = bundle_map,
  file = bundle_map_file_name
)
