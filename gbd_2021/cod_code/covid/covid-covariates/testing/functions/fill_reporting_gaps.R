#' Fill in a period of non-reporting with prior n-day average
#' 
#' Once you start filling in daily values, you must build the time series by day
#' from that point on.
#' 
#' Run after filter_data so you have filtered_cumul column - filtered_cumul is
#' copied from combined_cumul (which transforms daily to cumul space)
#'
#' @param Data_table [data.table] input raw data (all locations)
#' @param Location_id [integer] location_id for correction
#' @param Start_date [character] start date of zero-reporting period to fill
#' @param End_date [character] end date to fill
#' @param N_day_average [integer] carry an n_day average flat line forward; if NULL, linear interpolation in daily space
fill_reporting_gap <- function(Data_table, Location_id, Start_date, End_date, N_day_average = NULL) {
  
  # # scoping
  # Data_table <- copy(raw_testing_dt)
  # Location_id <- 35495
  # Start_date <- "2020-12-03"
  # End_date <- "2020-12-27"
  # N_day_average = 7
  # 
  # zero_idx <- which(x$filtered_daily == 0)
  # x[zero_idx]
  # # scoping
  
  # Validations
  if (!is.data.table(Data_table)) {
    stop("Data_table must be type data.table")
  } else if (!is.character(Start_date) & is.character(End_date)) {
    stop("All dates must be type character")
  }
  
  message(paste0("Filling gaps in ", 
                 unique(Data_table[location_id == Location_id]$location_name),
                 " : ", Location_id))
  
  # Setup
  library(zoo)
  x <- Data_table[location_id == Location_id]
  x <- x[order(date)]
  
  # ensure full and interpolated time series
  t <- c(min(x$date, na.rm = T), max(x$date, na.rm = T))
  x <- merge(x, data.table(date = as.Date(t[1]:t[2])), all.y = T)
  x[, filtered_cumul := zoo::na.approx(filtered_cumul)]
  
  # create daily space
  x[, filtered_daily := c(0, diff(filtered_cumul))]
  
  # isolate date range
  t <- as.Date(c(Start_date, End_date))
  idx <- which(x$date %in% t[1]:t[2])
  
  if(!is.null(N_day_average)){
    
  # calculate n-day average and fill zero space
  idx_mean <- which(x$date %in% (t[1]-N_day_average):(t[1]-1))
  n_day_avg <- mean(x[idx_mean]$filtered_daily)
  x[idx]$filtered_daily <- n_day_avg
  
  } else {
    
  # linear daily interpolation
  x[idx]$filtered_daily <- NA
  x$filtered_daily <- zoo::na.approx(x$filtered_daily)
  
  }
  
  # rebuild cumulative
  x$filtered_daily[1] <- x$filtered_cumul[1]
  x$filtered_cumul <- cumsum(x$filtered_daily)
  
  # dx plots
  # par(mfrow=c(1,3))
  # plot(x$date, x$raw_cumul - lag(x$raw_cumul))
  # plot(x$date, x$filtered_daily)
  # title(main = paste0("Gap fill\n",unique(x$location_name), " : ", unique(x$location_id)))
  # plot(x$date, x$filtered_cumul)
  # par(mfrow=c(1,1))
  
  # replace old data
  x[, filtered_daily := NULL]
  Data_table <- Data_table[!location_id == Location_id, ]
  Data_table <- rbind(Data_table, x)
  Data_table <- Data_table[order(location_id, date)]
  
  return(Data_table)
}
