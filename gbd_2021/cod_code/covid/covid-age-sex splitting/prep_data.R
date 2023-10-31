prep_data <- function(deaths_rr_path, mort_age_pattern_path, sero_age_pattern_path, gbd_seir_outputs_dir, 
                prod_seir_outputs_dir, ifr_age_pattern_path, ihr_age_pattern_path,
                path_pop, path_locmeta, pop, age_table, icu_prop_path, gbd_hierarchy,
                covid_hierarchy, sub_locs, hosp_duration, icu_duration, infec_death_lag, fhs, apply_scalars) {
  
  # Mort RRs (age-specific)
  rr_in <- as.data.table(readRDS(deaths_rr_path))
  rr_u5_dt <- rr_in[age_group_start < 5]
  rr_5plus_dt <- rr_in[age_group_start >= 5]
  setnames(rr_5plus_dt, "age_group_start", "age_group_years_start")
  rr_5plus_dt <- merge(
    rr_5plus_dt, 
    age_table[, .(age_group_years_start, age_group_id)]
  )
  u5_ages <- age_table[age_group_years_start < 5]$age_group_id
  u5_rr <- rbindlist(lapply(u5_ages, function(a) {
    temp_dt <- copy(rr_u5_dt)
    temp_dt[, age_group_id := a]
    return(temp_dt)
  }))
  
  rr_dt <- rbind(
    u5_rr[, .(age_group_id, rr)],
    rr_5plus_dt[, .(age_group_id, rr)],
    use.names = T
  )

  deaths_rr <- rr_dt[order(age_group_id)]
  
  # Hosp RRs (age-specific)
  hosp_rr_in <- as.data.table(readRDS(hosp_rr_path))
  hosp_rr_u5_dt <- hosp_rr_in[age_group_start < 5]
  hosp_rr_5plus_dt <- hosp_rr_in[age_group_start >= 5]
  setnames(hosp_rr_5plus_dt, "age_group_start", "age_group_years_start")
  hosp_rr_5plus_dt <- merge(
    hosp_rr_5plus_dt, 
    age_table[, .(age_group_years_start, age_group_id)]
  )
  u5_ages <- age_table[age_group_years_start < 5]$age_group_id
  u5_hosp_rr <- rbindlist(lapply(u5_ages, function(a) {
    temp_dt <- copy(hosp_rr_u5_dt)
    temp_dt[, age_group_id := a]
    return(temp_dt)
  }))
  
  hosp_rr_dt <- rbind(
    u5_hosp_rr[, .(age_group_id, hosp_rr)],
    hosp_rr_5plus_dt[, .(age_group_id, hosp_rr)],
    use.names = T
  )

  hosps_rr <- hosp_rr_dt[order(age_group_id)]

  #infecs RRs
  infecs_rr <- 1
  
  # Seroprevalence, mortality, and population age pattern
  pop_mort_sero_props_dt <- prep_mort_sero_age_pattern(
      pop, mort_age_pattern_path, sero_age_pattern_path, age_table, gbd_hierarchy
    )
  
  # All-age daily infections and deaths, sourced from GBD and prod directories
  prod_gbd_locs <- setdiff(
    intersect(
      covid_hierarchy[most_detailed == 1]$location_id,
      gbd_hierarchy[most_detailed == 1]$location_id
    ), 
    sub_locs
  )
  
  # Data from subnational units of the following locations should be raked to the national value from the prod seir run
  # India urban/rural splits should be raked to India states from the prod seir run
  rake_parent_locs <- c(51, 62, 63, 72, 67, 90, 93, 4749, 142, 6, 11, 16, 179, 180, 196, 214, gbd_hierarchy[path_to_top_parent %like% '163' & level==4]$location_id)
  rake_locs <- gbd_hierarchy[parent_id %in% rake_parent_locs]$location_id
  

  daily_infecs <- synthesize_measure("infections", gbd_seir_outputs_dir, prod_seir_outputs_dir,
                                       rake_locs, prod_gbd_locs, sub_locs, fhs, apply_scalars, gbd_hierarchy)
    
  daily_deaths <- synthesize_measure("deaths", gbd_seir_outputs_dir, prod_seir_outputs_dir,
                                       rake_locs, prod_gbd_locs, sub_locs, fhs, apply_scalars, gbd_hierarchy)
    
  daily_hosps <- synthesize_measure('admissions', gbd_seir_outputs_dir, prod_seir_outputs_dir,
                                      rake_locs, prod_gbd_locs, sub_locs, fhs, apply_scalars, gbd_hierarchy)

  # Age-specific rates
  ifr_age <- prep_gbd_age_rates(ifr_age_pattern_path, age_table)
  ihr_age <- prep_gbd_age_rates(ihr_age_pattern_path, age_table)
  
  # ICU proportion
  icu_prop <- fread(icu_prop_path)

  # Duration
  durations <- list(hosp = hosp_duration, icu = icu_duration)
  
  # EM Scalars
  em_scalars <- fread(em_scalar_path)
  
  
  return(
    list(
      deaths_rr = deaths_rr,
      infecs_rr = infecs_rr,
      hosps_rr = hosps_rr,
      pop_mort_sero_props = pop_mort_sero_props_dt,
      daily_infecs = daily_infecs,
      daily_deaths = daily_deaths,
      daily_hosps = daily_hosps,
      ifr_age = ifr_age,
      ihr_age = ihr_age,
      icu_prop = icu_prop,
      durations = durations,
      infec_death_lag = infec_death_lag,
      em_scalars = em_scalars
    )
  )
}