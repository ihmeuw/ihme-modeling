
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)
library(parallel)

get_exposure_draws <- function(loc_id, temp_loc, age, sex){
 
  
  source("FILEPATH/get_draws.R"  )
  
  
    exposure <- mclapply(mes, function(x) {

                              file <- FILEPATH

                              data <- fread(file)
                              data <- data[year_id == 2010 & age_group_id == age & sex_id == sex]

                              data[, location_id := loc_id]
                              data[, modelable_entity_id := x]

                              return(data)

  }) %>% rbindlist(use.names = T, fill = T)
  
  exposure <- melt(exposure, id.vars = c("location_id", "age_group_id", "sex_id", "year_id", "modelable_entity_id", "measure_id"), variable = "draw", value.name = "exp")
  exposure[, draw := gsub(x = draw, pattern = "draw_", replacement = "")]
  
  return(exposure)
  
}


get_usa_ln_odds_draws <- function(loc_id, age, sex){
  
  odds <- fread(FILEPATH)
  
  num_draws = 1000
  
  newvars <- paste0("lnodds_",c(0:(num_draws-1)))
  
  odds <- odds[, list(location_id = 102, year_id = 2010, sex_id = sex, age_group_id = age, modelable_entity_id, ln_mortality_odds, std_error)]
  odds <- odds[!is.na(modelable_entity_id)]
  
  odds[, (newvars) := as.list(rnorm(num_draws,ln_mortality_odds,std_error)), by = modelable_entity_id ]
   
  odds <- melt(odds[, -c("ln_mortality_odds", "std_error")], id.vars = c("location_id", "year_id", "sex_id", "age_group_id", "modelable_entity_id"), variable = "draw", value = "lnodds")
  
  odds[, draw := gsub(x = draw, pattern = "lnodds_", replacement = "")]
  
  return(odds)
  
}


get_katz_ln_odds_draws <- function(r, age){
  
  region_rr <- katz_rrs[region == r & age_group_id == age, ]
  region_rr[, ln_mortality_odds := log(mean)][, ln_odds_lower := log(lower)][, ln_odds_upper := log(upper)]
  region_rr[, ln_mortality_odds_std_err := (ln_odds_upper - ln_odds_lower) / 3.92]
  
  num_draws = 1000
  
  newvars <- paste0("lnodds_",c(0:(num_draws-1)))

  region_rr[, (newvars) := as.list(rnorm(num_draws,ln_mortality_odds,ln_mortality_odds_std_err)), by = sga_category]
  
  region_rr <- melt(region_rr, id.vars = c("region", "age_group_id", "sga_category"), measure.vars = patterns("^lnodds_"), variable = "draw", value = "ln_odds")
  
  region_rr[, draw := gsub(x = draw, pattern = "lnodds_", replacement = "")]
   
  return(region_rr)
  
}

for(sex in c(1,2)){
  
  for(age in c(2,3)){


	version = "v10"

	ME_map <- fread(FILEPATH)
	mes <- ME_map[gbd_2017_me == 1, modelable_entity_id]

	katz_rrs <- fread(FILEPATH)

	usa_odds <- get_usa_ln_odds_draws(loc_id = 102, age, sex)

	usa_exp <- get_exposure_draws(loc_id = 102, temp_loc = 543, age, sex)

	usa_exp <- merge(usa_exp, ME_map[, list(modelable_entity_id, parameter, sga_category)], all.x = T)

	usa <- merge(usa_exp, usa_odds, by = intersect(names(usa_exp), names(usa_odds)), all = T)

	usa <- merge(usa, usa[, .(total = sum(exp)), by = list(location_id, year_id, age_group_id, sex_id, sga_category, draw)], by = c("location_id", "year_id", "age_group_id", "sex_id", "sga_category", "draw") )

	usa[, rescaled_exp := exp / total]

	usa_lnodds_by_sga <- usa[, .(usa_sga_cat_lnodds = sum(rescaled_exp * lnodds)), by = list(location_id, year_id, age_group_id, sex_id, sga_category, draw)]

	min_usa <- usa_lnodds_by_sga[, .(min = min(usa_sga_cat_lnodds)), by = list(location_id, year_id, age_group_id, sex_id, draw)]

	usa_lnodds_by_sga <- merge(usa_lnodds_by_sga, min_usa, by = intersect(names(usa_lnodds_by_sga), names(min_usa)))

	usa_lnodds_by_sga[, usa_sga_cat_lnodds := usa_sga_cat_lnodds - min]

	usa_lnodds_by_sga <- usa_lnodds_by_sga[, -c("min")]


	param_map = data.table(region = c("asia", "africa"), loc_id = c(4, 166), temp_loc = c(18, 189))

	for(i in 1:nrow(param_map)){
  
			  r = param_map[i, region]
			  loc_id = param_map[i, loc_id]
			  temp_loc = param_map[i, temp_loc]
			  
			  region_exp <- get_exposure_draws(loc_id = loc_id, temp_loc = temp_loc, age, sex)
			  
			  region_exp <- merge(region_exp, ME_map[, list(modelable_entity_id, parameter, sga_category)], all.x = T)
			  region_lnodds_by_sga <- get_katz_ln_odds_draws(r = r, age = age)
			  
			  region_lnodds_by_sga[, sex_id := sex][, age_group_id := age][, year_id := 2010]
			  
			  combined_lnodds_by_sga <- merge(region_lnodds_by_sga, usa_lnodds_by_sga[, -c("location_id")], by = c("year_id", "age_group_id", "sex_id", "draw", "sga_category"))
			  
			  combined_lnodds_by_sga[, scalar_factor := ln_odds / usa_sga_cat_lnodds]
			  
			  combined_lnodds_by_sga[is.na(scalar_factor), scalar_factor := 1]
			  
			  combined_lnodds_by_bin <- merge(usa, combined_lnodds_by_sga[, list(sga_category, sex_id, age_group_id, draw, scalar_factor)], by = intersect(names(usa), names(combined_lnodds_by_sga[, list(sga_category, sex_id, age_group_id, draw)])))
			  
			  combined_lnodds_by_bin[, rescaled_lnodds := lnodds * scalar_factor]
			  
			  region_pregpr <- combined_lnodds_by_bin[, .(y_obs = mean(rescaled_lnodds), y_std = sqrt(var(rescaled_lnodds))), by = list(location_id, year_id, sex_id, age_group_id, modelable_entity_id)]
			  
			  region_pregpr <- region_pregpr[, list(modelable_entity_id, y_obs, y_std)]
			  
			  region_pregpr <- merge(ME_map, region_pregpr, by = "modelable_entity_id")
			  
			  region_pregpr[, dim_x1 := ga]

			  region_pregpr[, dim_x2 := bw]
			  
			  write.csv(region_pregpr, FILEPATH, row.names = F, na = "")

  			}

		}
}