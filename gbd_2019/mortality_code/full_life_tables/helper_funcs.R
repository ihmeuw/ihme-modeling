# expand 5-year age groups to single-year age groups
expand_ages <- function(abridged_rates, single_abridged_age_map) {
  expanded_dataset <- CJ(sex_id = unique(abridged_rates[, sex_id]),
                       year_id = unique(abridged_rates[, year_id]),
                       age = unique(single_abridged_age_map[, age]),
                       draw = 0:999)
  expanded_dataset <- merge(expanded_dataset, single_abridged_age_map, by = "age", all.x = T)
  expanded_dataset <- merge(expanded_dataset, abridged_rates, by = c("year_id", "draw", "abridged_age", "sex_id"), all.x = T)
  
  return(expanded_dataset)
}

# merge population on to deaths, calculate rate
calculate_rates <- function(deaths, population) {
  rates <- merge(deaths, population, by = c("year_id", "location_id", "sex_id", "age_group_id"), all.x = T)
  rates[, death_rate := deaths / mean_population]
  rates[, c("deaths", "mean_population") := NULL]
  
  return(rates)
}

aggregate_deaths <- function(dataset, target_age_group_id, child_age_group_ids, id_vars) {
  # subset dataset to only contain the age group ids that are used to aggregate up to the target age group id
  child_ages <- dataset[age_group_id %in% child_age_group_ids]
  # select any age groups that are missing
  child_age_groups_missing <- child_age_group_ids[!(child_age_group_ids %in% unique(child_ages[, age_group_id]))]
  if (length(child_age_groups_missing) > 0) {
    stop(paste0("The following age groups id(s) required for aggregation up to age group id ", target_age_group_id," are missing: ", paste0(child_age_groups_missing, collapse = ", ")))
  }
  
  target_age <- child_ages[, lapply(.SD, sum), .SDcols = "deaths", by = id_vars]
  target_age[, age_group_id := target_age_group_id]
  return(target_age)
}

add_lt_ages <- function(death_rates) {
  age_ids_to_append <- lapply(list(33, 44, 45, 148), function(age_group) {
    new_shock_rates <- death_rates[age_group_id == 235, list(location_id, year_id, sex_id, draw, death_rate)]
    new_shock_rates[, age_group_id := age_group]
    return(new_shock_rates)
  })

  age_ids_to_append <- rbindlist(age_ids_to_append, use.names = T)
  death_rates <- rbindlist(list(age_ids_to_append, death_rates), use.names = T, fill = T)
  return(death_rates)
}
