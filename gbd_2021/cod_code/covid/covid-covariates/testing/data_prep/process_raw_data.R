
#' Process raw testing data
#' @param df data.table with raw testing data
#' @param dt_pop data.table with populations for each location in df
#' @param locs A vector of location_ids to check for in the data
#' @param first_case_date A data.table with date of first case for each location
#' @return A data.table with redistributed data and population appended

process_raw_data <- function(df, dt_pop, locs, first_case_date) {
  
  # create a full time series to account for missing days
  initial_test <- df[filtered_cumul != 0, .(first_test_date = min(date), last_date = max(date)), by = .(location_id, location_name)]
  loc_time_dt <- merge(initial_test, first_case_date, by = 'location_id')
  loc_time_dt[, start_date := min(first_test_date, first_case_date), by = 'location_id']
  
  time_series <- rbindlist(lapply(split(loc_time_dt, by = "location_id"), function(loc_dt) {
    
    #print(unique(loc_dt$location_id))
    loc_dt <- loc_dt[1,]
    data.table(location_id = loc_dt$location_id, 
               location_name = loc_dt$location_name,
               date = seq(loc_dt$start_date, loc_dt$last_date, by = "1 day"),
               first_test_date = loc_dt$first_test_date)
    
  }))
  
  df <- merge(df, time_series, by = c("location_id", "location_name", "date"), all = T)
  df <- merge(df, dt_pop[, .(location_id, population)], by = "location_id")
  df <- df[order(location_id, date)]
  
  # # drop any locations with exactly 1 data point as they cannot currently be filled
  count.by.loc <- df[!is.na(filtered_cumul), .(count = .N), by = "location_id"]
  count.by.loc <- count.by.loc[order(count)]
  single.record.location_ids <- count.by.loc[count == 1, location_id]
  
  if (length(single.record.location_ids) > 0) {
    warning(sprintf("The following locations have only 1 data point and are being dropped: %s", paste(single.record.location_ids, collapse = ", ")))
    df <- df[!location_id %in% count.by.loc[count == 1, location_id]]
  }
  
  df[, daily_total := shift_and_fill(filtered_cumul, redistribute = FALSE), by = "location_id"]
  df[, daily_positive := shift_and_fill(positive, redistribute = FALSE), by = "location_id"]
  
  # update locations where only new tests reported are positive
  df[, c("positive_reported", "total_reported", "daily_total_reported", "daily_positive_reported") :=
       .(positive, filtered_cumul, daily_total, daily_positive)]
  
  df[
    daily_total_reported == daily_positive_reported & daily_total_reported != 0 & date != first_test_date,
    c("filtered_cumul", "daily_total") := NA
  ]
  
  # Drop all zeroes because they are not allowed in INDIVIDUAL_NAME Smooth
  df[daily_total_reported == 0, c("filtered_cumul", "daily_total") := NA]
  
  # recalculate daily total to account for missing days/data dumps
  df[, daily_total_redistributed := shift_and_fill(filtered_cumul, redistribute = TRUE), by = "location_id"]
  #df[, daily_total_redistributed := shift_and_fill(filtered_cumul, redistribute = FALSE), by = "location_id"]
  
  # Manually drop Czechia
  df[location_id == 47 & date == "2020-05-25", daily_total_redistributed := NA]
  
  df[filtered_cumul < 0, filtered_cumul := NA]
  df[daily_total_redistributed < 0, daily_total_redistributed := NA]
  
  # Redistribute New Jersey
  bad_loc <- 553
  bad_date <- as.Date("2020-05-11")
  dates <- seq(bad_date - 13, bad_date, by = "1 day")
  bad_val <- df[location_id == bad_loc & date == bad_date]$daily_total_reported
  add_val <- bad_val / 14
  df[location_id == bad_loc & date %in% dates, daily_total_redistributed := daily_total_redistributed + add_val]
  prev_mean <- mean(df[location_id == bad_loc & date %in% dates & date != bad_date]$daily_total_reported)
  df[location_id == bad_loc & date == bad_date, daily_total_redistributed := daily_total_redistributed - bad_val + prev_mean]
  
  # Fill missing with interpolated values
  df[, daily_total_redistributed := na.approx(daily_total_redistributed, na.rm = F), by = location_id]
  df <- df[, .(location_id, location_name, date, raw_cumul, raw_daily, combined_cumul, filtered_cumul, daily_total_reported, daily_total_redistributed, population)]
  return(df[])
}
