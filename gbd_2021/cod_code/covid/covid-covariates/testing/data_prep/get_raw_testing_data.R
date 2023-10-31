#' Compile testing data
#' @param data_version Version to pull from model-inputs
#' @return A data.table with compiled testing data
get_raw_testing_data <- function(data_version = "latest") {
  
  testing_data_dir <- file.path("FILEPATH", data_version, "testing/")

  df <- fread(file.path(testing_data_dir, "all_locations_tests.csv"))
  
  setnames(df, c("location", "total_tests", "daily_tests"), c("location_name", "raw_cumul", "raw_daily"))
  df[, date := as.Date(date, format = "%d.%m.%Y")]

  # Drop data that are *definitely* not antibody/serology
  df <- subset(df, !(tests_units %like% "ntibody"))
  df <- subset(df, !(tests_units %like% "erology"))
  
  df <- df[!(is.na(raw_cumul) & is.na(raw_daily))]
  df <- df[order(location_id, date)]

  # Generate Washington locations
  wash_locs <- 3526:3564
  spo_df <- df[location_id == 3539]
  king_sno_df <- copy(df[location_id %in% c(3543, 3535)])
  king_sno_df <- king_sno_df[, lapply(.SD, sum, na.rm = T), by = "date", .SDcols = c("positive", "raw_cumul", "raw_daily")]
  king_sno_df[, location_id := 60886]
  king_sno_df[, location_name := "King and Snohomish Counties"]
  other_df <- copy(df[location_id %in% setdiff(wash_locs, c(3535, 3539, 3543))])
  other_df <- other_df[, lapply(.SD, sum, na.rm = T), by = "date", .SDcols = c("positive", "raw_cumul", "raw_daily")]
  other_df[, location_id := 60887]
  other_df[, location_name := "Washington except for King, Snohomish, and Spokane Counties"]
  wash_df <- rbindlist(list(spo_df, king_sno_df, other_df), fill = T)
  
  # There is a lag in reporting in Washington state that can cause what appear to be crashes in testing numbers
  # Masking the past 2 weeks of data here to prevent this
  mask_dates <- seq(as.Date(max(wash_df$date) - 15), as.Date(max(wash_df$date)), by=1)
  wash_df <- wash_df[-which(date %in% mask_dates),]
  
  df <- rbind(df[!(location_id %in% c(570, wash_locs))], wash_df)

  # Handle daily vs. cumulative
  df[, combined_cumul := raw_cumul]
  # Identify locations where cumulative is NA AND daily is not NA
  daily_locs <- unique(df[is.na(raw_cumul) & !is.na(raw_daily)]$location_id)
  for(loc in daily_locs) {
    # Are there any cumulative values not NA?
    any_total <- any(!is.na(df[location_id == loc]$raw_cumul))
    if(any_total) {
      # Beginning
      # Earliest non NA cumulative date
      first_total_date <- min(df[location_id == loc & !is.na(raw_cumul)]$date)
      # For dates earlier than first cumulative - calculate a "combined cumulative"
      # by cumulatively summing the dailies up
      df[location_id == loc & date < first_total_date & !is.na(raw_daily), combined_cumul := cumsum(raw_daily)]
      
      # End
      # Latest non NA cumulative date
      last_total_date <- max(df[location_id == loc & !is.na(raw_cumul)]$date)
      last_total_val <- df[location_id == loc & date == last_total_date]$raw_cumul
      # For dates after the last non NA cumulative date sum the dailies and add the last 
      # known cumulative
      df[location_id == loc & date > last_total_date & !is.na(raw_daily), combined_cumul := cumsum(raw_daily) + last_total_val]

      # Middle
      # Find the dates in the middle that have NA cumulative but non-NA daily
      # Calculate the cumulative by adding the daily for that day plus the previous 
      # days cumulative
      na_dates <- df[location_id == loc & date > first_total_date & date < last_total_date & is.na(raw_cumul)]$date
      for(d in na_dates) {
        prev_val <- df[location_id == loc & date == (d - 1)]$raw_cumul
        if(length(prev_val) == 1) {
          df[location_id == loc & date == d, combined_cumul := prev_val + raw_daily]
        }        
      }
    } else {
      # Total = cumulative of daily
      df[location_id == loc & !is.na(raw_daily), combined_cumul := cumsum(raw_daily)]
    }
  }
  
  return(df)
}
