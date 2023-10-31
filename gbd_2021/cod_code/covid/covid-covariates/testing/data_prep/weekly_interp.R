#' Aggregate daily data to weekly bins and interpolate back to daily
#' @param testing_data data.table with testing data
weekly_interp <- function(testing_data) {
  
  # Create day number rows by location
  testing_data[, days := 1:.N, by = location_id]
  # Get max days by location
  testing_data[, max_days := max(days), by = location_id]
  # Get number of weeks back from the latest 
  testing_data[, weeks_back := floor((max_days - days) / 7), by = location_id]
  testing_data[, weeks := max(weeks_back) - weeks_back, by = location_id]
  
  # Calculate the weekly total, median day, and total days
  sum_dt <- testing_data[!is.na(daily_total_redistributed), 
    .(
      weekly_total = sum(daily_total_redistributed),
      day = median(days),
      total_days = .N
    ),
    by = .(location_name, location_id, weeks)
  ]
  
  
  sum_dt[, obs := .N, by = location_id]
  
  # For each location which has more than 1 observation
  # Approximate the daily deaths by linearly interpolating between the day 
  week_interp <- rbindlist(lapply(unique(sum_dt[obs > 1]$location_id), function(loc_id) {
    loc_dt <- sum_dt[location_id == loc_id]
    range <- 1:max(testing_data[location_id == loc_id & !is.na(daily_total_redistributed)]$days)
    daily_dt <- approx(loc_dt$day, loc_dt$weekly_total / loc_dt$total_days, xout = range, rule = 2)
    data.table(location_id = loc_id, days = daily_dt$x, total = daily_dt$y) # Change the divisor to be days in the week
  }))
  setnames(week_interp, "total", "week_interp")
  testing_data <- merge(testing_data, week_interp, by = c("location_id", "days"), all.x = T)
  testing_data[, daily_total := week_interp]

  return(testing_data[, .(location_id, location_name, date, raw_cumul, raw_daily, combined_cumul, filtered_cumul, daily_total_reported, daily_total_redistributed, daily_total, population)])
}
