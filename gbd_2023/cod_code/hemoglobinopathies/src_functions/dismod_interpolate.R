# Interpolate Dismod results for hemog causes, save out flat files

dismod_interpolate <- function(input_map, loc_id, year_start, year_end, release_id) {
  source(paste0(CC_LIB_ROOT, "interpolate.R"))
  full_set <- data.table::data.table()
  dismod_mes <- input_map$modelable_entity_id[input_map$source == "dismod"]
  for (me in dismod_mes) {
    message("Interpolating ME ID: ", me)
    # Interpolate CSMR for each nonfatal hemoglobinopathy ME
    interp_df <- interpolate(
      gbd_id_type = 'modelable_entity_id',
      gbd_id = me,
      source = 'epi',
      measure_id = 15,
      location_id = loc_id,
      reporting_year_start = year_start,
      reporting_year_end = year_end,
      release_id = release_id,
      status = 'best',
      num_workers = parallelly::availableCores()
    )
    full_set <- rbind(full_set, interp_df)
  }
  
  if (!identical(sort(unique(full_set$modelable_entity_id)), sort(dismod_mes))) {
    stop('Your interpolated df did not get results for all MEs!')
  }
  
  merged_interp <- merge(full_set, input_map, by = c("modelable_entity_id")) |> 
    dplyr::select(-model_version_id,
                  -source,
                  -measure_id,
                  -metric_id,
                  -modelable_entity_id)
  
  agg_interp <- merged_interp |> 
    dplyr::group_by(age_group_id, sex_id, year_id, cause_id, location_id) |> 
    dplyr::summarise_all(sum)
  
  return(agg_interp)
}
