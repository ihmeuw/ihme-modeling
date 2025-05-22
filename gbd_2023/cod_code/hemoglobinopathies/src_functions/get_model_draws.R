####### Pull down model draws of parent hemog CoD model, convert to rate

get_model_draws <- function(all_years, loc_id, release_id) {
  source(paste0(CC_LIB_ROOT, "get_draws.R"))
  
  parent_df <- get_draws(
    gbd_id_type = 'cause_id',
    gbd_id = 613,
    location_id = loc_id,
    year_id = all_years,
    num_workers = parallelly::availableCores(),
    source = 'codem',
    release_id = release_id,
    status = "best"
  )
  
  source(paste0(CC_LIB_ROOT, "get_population.R"))
  pop_other <-
    get_population(
      age_group_id = unique(parent_df$age_group_id),
      location_id = loc_id,
      year_id = all_years,
      sex_id = c(1, 2),
      release_id = release_id
    )
  
  model_w_pop <- merge(
    parent_df,
    pop_other,
    by = c("age_group_id", 'sex_id', "year_id", "location_id")
  )
  
  model_w_pop <- data.frame(model_w_pop)
  
  df_only_data <- model_w_pop[, grepl("draw", colnames(model_w_pop))]
  
  model_data_rate <- df_only_data / model_w_pop$population.y
  
  model_data_rate <- cbind(
    model_w_pop[, c("age_group_id", "sex_id", "year_id", "cause_id", "location_id")],
    model_data_rate
  )
  
  return(model_data_rate)
}
