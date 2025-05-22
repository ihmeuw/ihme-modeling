# Computes correction factors to scale household deaths to nationally-representative space

compute_scalars <- function(pop, population_estimate_version_id, gbd_year) {

  # Get GBD population estimates
  gbd_pop <- get_mort_outputs("population", "estimate",
                              run_id = population_estimate_version_id,
                              age_group_ids = 22,
                              gbd_year = gbd_year)
  setnames(gbd_pop, "mean", "population")
  gbd_pop <- gbd_pop[, list(age_group_id, year_id, sex_id, location_id, population, run_id)]

  # Old AP
  old_ap_pops <- gbd_pop[location_id %in% c(4841, 4871)]
  old_ap_pops <- old_ap_pops[, list(population = sum(population)), by = c("age_group_id", "year_id", "sex_id", "run_id")]
  old_ap_pops[, location_id := 44849]
  gbd_pop <- rbindlist(list(gbd_pop, old_ap_pops), use.names = T)

  # Merge ihme_loc_id variables onto GBD pop estimates
  locations <- get_locations(gbd_year = gbd_year, level = "all")[,c("location_id", "ihme_loc_id")]
  gbd_pop <- merge(gbd_pop, locations, by = "location_id", all.x = TRUE)
  gbd_pop[,sex_id:= as.factor(sex_id)]
  levels(gbd_pop$sex_id) <- c("male", "female", "both")
  setnames(gbd_pop, c("sex_id", "year_id"), c("sex", "year"))
  gbd_pop$sex <- as.character(gbd_pop$sex)

  # Get survey pop
  pop <- pop[!is.na(mean)]
  pop <- pop[year >= 1950]
  pop <- pop[!grepl("CENS|VR", source_type)]

  # Pull age group metadata and aggregate empirical pop
  age_map <- data.table(get_age_map(type = "all", gbd_year = gbd_year))
  id_vars <- c("ihme_loc_id", "source_type", "pop_source", "sex", "pop_nid", "underlying_pop_nid", "year")
  aggregated_pop <- agg_granular_age_data(pop, id_vars, age_map, agg_all_ages = T, agg_u5 = F, agg_15_60 = F)

  # Merge aggregated empirical pop with GBD population estimates to compute the scalars
  aggregated_pop <- merge(aggregated_pop, gbd_pop[, c("ihme_loc_id", "year", "sex", "population")], 
                                                    by = c("ihme_loc_id", "year", "sex"), all.x = TRUE)
  aggregated_pop[, correction_factor := population/mean]
  aggregated_pop <- aggregated_pop[!is.na(correction_factor)]

  return(aggregated_pop)
}