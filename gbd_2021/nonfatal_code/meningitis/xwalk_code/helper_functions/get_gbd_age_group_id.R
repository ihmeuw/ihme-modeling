# gets the age_group_id for age start/ age end in GBD land
get_gbd_age_group_id <- function(raw_dt, age_dt) {
  dt <- copy(raw_dt)
  dt <- merge(dt, age_dt[, .(age_group_id_start = age_group_id, gbd_age_start)], 
              by = c("gbd_age_start"))
  dt <- merge(dt, age_dt[, .(age_group_id_end = age_group_id, gbd_age_end)], 
              by = c("gbd_age_end"))
  return(dt)
}