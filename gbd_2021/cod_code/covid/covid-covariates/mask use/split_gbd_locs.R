
split_gbd_locations <- function(model,
                                hierarchy){
  
  source(file.path("/ihme/cc_resources/libraries/current/r/get_population.R"))
  split_columns <- c("mask_use")
  
  # Find missing GBD locations
  gbd_hier <- fread("FILEPATH/gbd_analysis_hierarchy.csv")
  
  missing_locs <- setdiff(gbd_hier[most_detailed == 1]$location_id, model$location_id)

  # Add problem subnats here
  add_parents <- hierarchy$location_id[hierarchy$location_name %in% c('South Africa', 'Indonesia')]
  add_gbd_locs <- gbd_hier$location_id[gbd_hier$parent_id %in% add_parents]
  
  missing_hier <- gbd_hier[location_id %in% c(missing_locs, add_gbd_locs)]
  
  admin1 <- missing_hier[level == 4]
  
  ## These seem to be India
  admin2 <- missing_hier[level == 5]
  admin2[location_id %in% c(44539, 44540), parent_id := 165]
  
  ## These seem to be UK
  admin3 <- missing_hier[level == 6]
  admin3$parent_id <- 95
  
  ## Okay, take the parent level, adjust for population size
  #gbd_pops <- get_population(gbd_round_id = 7, 
  #                           location_id = "all", 
  #                           year = 2020, 
  #                           decomp_step = "iterative",
  #                           age_group_id = 22, 
  #                           sex_id = 3)
  
  gbd_pops <- fread("FILEPATH/all_populations.csv")
  gbd_pops <- gbd_pops[age_group_id == 22 & sex_id == 3]

  child_locs <- rbind(admin1, admin2, admin3)
  pop <- merge(child_locs, gbd_pops, by = "location_id")
  pop <- merge(pop, gbd_pops, by.x = "parent_id", by.y = "location_id")
  pop[, pop_frac := population.x / population.y]
  
  setnames(pop, c("population.x"), c("population"))
  
  gbd_dt <- expand.grid(location_id = child_locs$location_id,
                        date = seq(min(model$date), max(model$date), by = "1 day"))
  
  gbd_dt <- data.table(gbd_dt)
  gbd_dt <- merge(gbd_dt, child_locs[,c("location_id","location_name","parent_id")], by = "location_id")
  
  out_gbd <- merge(gbd_dt, model[,c("location_id","date","observed",
                                    ..split_columns)], 
                   by.x = c("parent_id","date"), by.y = c("location_id","date"))
  
  out_gbd <- out_gbd[order(location_id, date)]
  out_gbd[, parent_id := NULL]
  
  return(out_gbd)
}


