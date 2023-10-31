##################################################
## Project: CVPDs
## Script purpose: Get flu cases
## Date: June 2022
## Author: USERNAME
##################################################

get_flu_rsv_cases <- function(year, age_meta, fatal_nonfatal){
if(fatal_nonfatal == "fatal"){  
  if(date %in% c("2022_06_07", "2022_06_13", "2022_06_13_B", "2022_06_14")){
    lri_draws <- fread("FILEPATH")[year_id == year]
  } else {
    #Fatal:
    lri_draws <- get_draws(gbd_id_type = "cause_id",
                           gbd_id = 322,
                           source = "codcorrect",
                           measure_id = 1,
                           metric_id = 1,
                           version_id = 253,
                           gbd_round_id = 7,
                           decomp_step = "step3",
                           location_id = unique(scalar_a1$location_id),
                           age_group_id = age_meta$age_group_id,
                           sex_id = c(1,2),
                           year_id = year,
                           num_workers = 10)
    
  }
} else if (fatal_nonfatal == "nonfatal"){
  #Non-fatal:
  lri_draws <- get_draws(gbd_id_type = "modelable_entity_id",
                         gbd_id = 1258,
                         source = "epi",
                         measure_id = 6,
                         metric_id = 3,
                         status = "best",
                         gbd_round_id = 7,
                         decomp_step = "iterative",
                         location_id = unique(scalar_a1$location_id),
                         age_group_id = age_meta$age_group_id,
                         sex_id = c(1,2),
                         year_id = year,
                         num_workers = 10)
}

  
  message("pulled LRI draws")
  # Convert rate to number space with population 
  pop <- get_population(age_group_id = age_meta$age_group_id, gbd_round_id = 7, decomp_step = "step3", sex_id = c(1,2), 
                        location_id = unique(hierarchy[most_detailed==1]$location_id), year_id = year)
  lri_draws <- merge(lri_draws, pop, by = c('location_id', 'sex_id', 'year_id', 'age_group_id'))
  drawnames <- paste0("draw_", 0:999)
  lri_draws[, (drawnames) := lapply(.SD, "*", population), .SDcols = drawnames]
  
  # Get etiology fractions
if(fatal_nonfatal == "fatal"){
  if(date == "2022_06_14"){
    etio_draws <- fread("FILEPATH")
  } else {
    etio_draws <- get_draws(gbd_id_type = c("rei_id", "rei_id", "cause_id"),
                            gbd_id = c(187,190,322),
                            metric_id = 2,
                            measure_id = 4,
                            source = "burdenator",
                            version_id = 221, 
                            gbd_round_id = 7,
                            decomp_step = "iterative",
                            location_id = unique(scalar_a1$location_id),
                            age_group_id = age_meta$age_group_id,
                            sex_id = c(1,2),
                            year_id = 2020, 
                            num_workers = 10)
  }
} else if(fatal_nonfatal=="nonfatal"){
  etio_draws <- get_draws(gbd_id_type = c("rei_id", "rei_id", "cause_id"),
                          gbd_id = c(187,190,322),
                          metric_id = 2,
                          measure_id = 3, 
                          source = "burdenator",
                          version_id = 221, 
                          gbd_round_id = 7,
                          decomp_step = "iterative",
                          location_id = unique(scalar_a1$location_id),
                          age_group_id = age_meta$age_group_id,
                          sex_id = c(1,2),
                          year_id = 2020, 
                          num_workers = 10)
}
  message("pulled etiology draws")
  
  # artificially set year to 2019 if needed
  if (year != 2020) etio_draws$year_id <- year
  cols.remove <- c("measure_id", "metric_id", "version_id")
  etio_draws[,(cols.remove) := NULL]
  # reshape from wide to long
  id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "rei_id")
  flu_dt <- melt(etio_draws[rei_id == 187], id.vars = id_vars, variable.name = "draw", 
                 value.name = "flu")
  rsv_dt <- melt(etio_draws[rei_id == 190], id.vars = id_vars, variable.name = "draw", 
                 value.name = "rsv")
  lri_dt <- merge(flu_dt, rsv_dt, by= c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "draw"))
  lri_dt$draw <- as.numeric(substr(lri_dt$draw, 6, nchar(as.character(lri_dt$draw))))
  
  # Get draw-wise flu count from etiology proportion and LRI draws
  if(fatal_nonfatal == "fatal"){
  #Fatal:
  lri_draws[,`:=`(version_id = NULL, cause_id = NULL, population = NULL, run_id = NULL)]
  } else if(fatal_nonfatal == "nonfatal"){
  #For non-fatal:
  lri_draws[,`:=`(model_version_id = NULL, modelable_entity_id = NULL, population = NULL, run_id = NULL)] 
  }
  id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", "metric_id")
  lri_long <- melt(lri_draws, id.vars = id_vars, variable.name = "draw", 
                   value.name = "lri_count")
  lri_long$draw <- as.numeric(substr(lri_long$draw, 6, nchar(as.character(lri_long$draw))))
  
  # if only have 100 burdenator draws, fix numbering here
  if(max(lri_dt$draw)<100){ # If we only have 100 burdenator draws
    # rescale annual_long draws from 0 to 99 repeated
    lri_long$draw_original <- lri_long$draw
    lri_long$draw <- lri_long$draw - 100*(floor(lri_long$draw/100))
  }
  
  # merge with the etiology fractions
  all_long <- merge(lri_long, lri_dt, by = c("location_id", "year_id", "draw", "sex_id", "age_group_id"), allow.cartesian = TRUE)
  # get mort counts from the percent
  all_long[, `:=` (flu_count = flu*lri_count, rsv_count = rsv*lri_count)]
 
  if(max(lri_dt$draw)<100){ # If we adjusted for 100 burdenator draws before
    # get the draw numbering back to normal
    all_long$draw <- all_long$draw_original
    all_long$draw_original <- NULL
  }
  return(all_long)
}
