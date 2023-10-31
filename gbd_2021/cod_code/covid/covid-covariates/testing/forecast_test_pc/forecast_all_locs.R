forecast_all_locs <- function(dt, var_name, ncores = 10, end_date = "2021-01-01") {
  dt_split <- split(dt, dt$location_id)
  # all_dt <- rbindlist(parallel::mclapply(dt_split, forecast_loc, var_name = var_name, mc.cores = ncores))
  all_dt <- rbindlist(lapply(dt_split, forecast_loc, var_name = var_name, end_date = end_date))
  return(all_dt)
}

forecast_loc <- function(loc_dt, var_name, end_date) {
  print(unique(loc_dt$location_name))
  loc_dt <- loc_dt[!is.na(get(var_name))]
  loc_dt[, day := as.integer(date) - as.integer(min(date))]
  max.day <- max(loc_dt$day)
  loc_dt <- loc_dt[day <= max.day]
  daily_vec <- loc_dt[[which(names(loc_dt) == var_name)]]
  diff_vec <- c(daily_vec[1], diff(daily_vec))
  arc <- max(mean(diff_vec), 0)
  max.val <- loc_dt[day == max.day, get(var_name)]
  extend_date <- as.Date(end_date) - max(loc_dt$date) # Make this date an argument
  loc_dt[, observed := T]
  loc_dt[, arc := arc]
  loc_dt <- rbind(
    loc_dt, 
    data.table(
      day = max(loc_dt$day) + 1:extend_date, 
      date = (max(loc_dt$date) + 1:extend_date),
      arc = arc
    ), 
    fill = T
  )
  loc_dt[day > max.day, (var_name) := max.val + (day - max.day) * arc]
  loc_dt[day > max.day, observed := F]
  loc_dt[, (paste0("fcast_", var_name)) := get(var_name)]
  loc_dt[, day := NULL]
  for(var in c("location_name", "location_id", "population")) {
    loc_dt[, (var) := unique(loc_dt[,var, with = F])[1]]
  }

  return(loc_dt)
}
