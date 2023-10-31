#######################################
# Copy national level, assign to GBD
# locations not in covid hierarchy
#######################################
source(file.path("FILEPATH/get_population.R"))
  
make_all_gbd_locations <- function(dt){
  
  est <- copy(dt)
  # Find missing GBD locations
  gbd_hier <- fread("FILEPATH/gbd_analysis_hierarchy.csv")
  
  missing_locs <- setdiff(gbd_hier[most_detailed == 1]$location_id, est$location_id)
  
  missing_hier <- gbd_hier[location_id %in% missing_locs]
  
  admin1 <- missing_hier[level == 4]
  
  ## These seem to be India
  admin2 <- missing_hier[level == 5]
  admin2[location_id %in% c(44539, 44540), parent_id := 165]
  
  ## These seem to be UK
  admin3 <- missing_hier[level == 6]
  admin3$parent_id <- 95
  
  ## Okay, take the parent level, adjust for population size
  gbd_pops <- get_population(gbd_round_id = 7, location_id = missing_locs, year = 2020, decomp_step = "iterative",
                             age_group_id = 22, sex_id = 3)
  
  child_locs <- rbind(admin1, admin2, admin3)
  
  parents <- c(unique(child_locs[location_id %in% missing_locs]$parent_id))
  missing_children_dt <- rbindlist(lapply(parents, function(p) {
    parent_dt <- copy(est[location_id == p])
    all_children <- setdiff(child_locs[parent_id == p]$location_id, p)
    impute_locs <- intersect(missing_locs, all_children)
    rbindlist(lapply(impute_locs, function(i) {
      child_dt <- copy(parent_dt)
      child_dt[, location_id := i]
      child_dt[, location_name := gbd_hier[location_id == i]$location_name]
      child_dt[, population := NULL]
      child_dt[, pop := NA]
    }))
  }))
  
  setdiff(gbd_hier[most_detailed == 1, location_id],
          c(unique(est$location_id), unique(missing_children_dt$location_id)))
  
  missing_children_dt <- merge(missing_children_dt, gbd_pops[, c("location_id", "population")])
  return(missing_children_dt)
}

