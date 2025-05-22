#### Getting data for synthesis


get_synth_draws <- function(all_years, loc_id, sex_id, cause, version_id, release_id){
  
  # pull draws for death counts then convert to rate (same process as with parent)
  synth_df <- ihme::get_draws('cause_id', gbd_id = cause, source = 'codem',
                        location_id = loc_id, version_id = version_id,
                        release_id = release_id)
  pop_df <- ihme::get_population(age_group_id = unique(synth_df$age_group_id), location_id = loc_id,
                           year_id = all_years, sex_id = unique(synth_df$sex_id), release_id = release_id) 
  
  
  synth_w_pop <- data.table::merge.data.table(synth_df, pop_df, by = c("age_group_id", 'sex_id', "year_id", "location_id"), suffixes = c(".synth", ".pop"))
  synth_w_pop <- synth_w_pop |> dplyr::select(-model_version_id,
                                              -measure_id,
                                              -metric_id)
  
  # get all draw columns
  draws_cols <- setdiff(colnames(synth_w_pop), nch::non_draw_cols(synth_w_pop))
  
  for (draw in draws_cols){
    synth_w_pop[[draw]] <- synth_w_pop[[draw]] / synth_w_pop$population.pop
    
  }
  
  return(synth_w_pop)
  
}