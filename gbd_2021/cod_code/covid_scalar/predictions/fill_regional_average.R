##################################################
## Project: CVPDs
## Script purpose: Fill missing countries with average of countries in region, draw wise
## Date: June 2022
## Author: USERNAME
##################################################

fill_regional_median <- function(dt, 
                                  countries, 
                                  draw_prefix = "draw_",
                                  new_draw_prefix = "new_draw_",
                                  num_draws = 1000,
                                  id_cols = c("region_id", "year_id")){
  # set drawnames
  drawnames <- paste0(draw_prefix, 0:(num_draws-1))
  new_drawnames <- paste0(new_draw_prefix, 0:(num_draws-1))
  # get locations
  missing_locs <- countries[!(countries %in% dt$location_id)]
  locs_to_add <- expand.grid(location_id = missing_locs,
                             year_id = unique(dt$year_id)) %>% as.data.table
  dt <- rbind(dt, locs_to_add, fill = TRUE)
  dt <- merge(dt,  hierarchy[, c("location_id", id_cols[id_cols %in% names(hierarchy)]), with = FALSE], by = "location_id")
  # fill with regional median. Median instead of mean because of rogue high draws that we don't want to drive up the mean
  dt[, (new_drawnames) := lapply(.SD, median, na.rm=TRUE), by=id_cols, .SDcols=drawnames]
  # mob_nat[is.na(mob_avg_month), mob_avg_month := reg_mob_avg_month]
  # set to the regional average if NA
  dt <- dt[, (drawnames) := lapply(0:(num_draws-1), function(x) {
    ifelse(is.na(get(paste0(draw_prefix, x))), get(paste0(new_draw_prefix, x)), get(paste0(draw_prefix, x)))})]
  # delete the regional average 
  dt[,(new_drawnames) := NULL]
  return(dt)
}

fill_regional_average <- function(dt, 
                                 countries, 
                                 draw_prefix = "draw_",
                                 new_draw_prefix = "new_draw_",
                                 num_draws = 1000,
                                 id_cols = c("region_id", "year_id")){
  # set drawnames
  drawnames <- paste0(draw_prefix, 0:(num_draws-1))
  new_drawnames <- paste0(new_draw_prefix, 0:(num_draws-1))
  # get locations
  missing_locs <- countries[!(countries %in% dt$location_id)]
  locs_to_add <- expand.grid(location_id = missing_locs,
                             year_id = unique(dt$year_id)) %>% as.data.table
  dt <- rbind(dt, locs_to_add, fill = TRUE)
  dt <- merge(dt, hierarchy[, c("location_id", id_cols[id_cols %in% names(hierarchy)]), with = FALSE], by = "location_id")
  # fill with regional median. Median instead of mean because of rogue high draws that we don't want to drive up the mean
  dt[, (new_drawnames) := lapply(.SD, mean, na.rm=TRUE), by=id_cols, .SDcols=drawnames]
  # mob_nat[is.na(mob_avg_month), mob_avg_month := reg_mob_avg_month]
  # set to the regional average if NA
  dt <- dt[, (drawnames) := lapply(0:(num_draws-1), function(x) {
    ifelse(is.na(get(paste0(draw_prefix, x))), get(paste0(new_draw_prefix, x)), get(paste0(draw_prefix, x)))})]
  # delete the regional average 
  dt[,(new_drawnames) := NULL]
  return(dt)
}