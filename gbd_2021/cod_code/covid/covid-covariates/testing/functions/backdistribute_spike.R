#' Distribute positive data spike over entire time series.
#'
#' Spreads spike proportionally by daily values from day prior to spike to
#' beginning of time series.
#'
#' @param dt [data.table] Raw testing data.table. REQUIRE: data sorted by
#'   dt$location_id, dt$date. REQUIRE columns: c("location_id",
#'   "date","combined_cumul", "filtered_cumul")
#' @param loc_id [integer] IHME location ID
#' @param spike_date [character] date of positive data spike
#'
#' @return [data.table] Raw testing data.table with one spike in one location
#'   spread from day prior to spike to beginning of time series.
#' @export
#'
#' @examples
backdistribute_spike <- function(dt, loc_id, spike_date) {
  
  # scoping
  # spike_date <- "2021-12-03" # big spike in Mongolia
  # loc_id = 38L
  # scoping
  
  loc_name <- unique(dt[location_id==loc_id, location_name])
  
  message("Backdistributing spike on ", spike_date, " in ", loc_name)
  
  # locate spike and dates to back-distribute over
  spike_date <- as.Date(spike_date)
  spike_idx <- which(dt$date == spike_date & dt$location_id == loc_id)
  
  # use index as dates may not be complete/consecutive
  backdistribute_idx <- spike_idx - 1
  backdistribute_date <- spike_date - 1
  
  # get daily proportions divide spike value over
  backdistribute_denom <- dt[location_id == loc_id & date == backdistribute_date, combined_cumul]
  dt[location_id == loc_id & date <= backdistribute_date, prop_daily_over_cumul := filtered_daily / backdistribute_denom]
  spike_value <- dt[spike_idx, filtered_daily]
  
  # split the spike proportionally over all prior days
  dt[location_id == loc_id & date <= backdistribute_date, backdistribute_value := ..spike_value * prop_daily_over_cumul]
  
  # rebuild filtered_cumulative and filtered_daily
  dt[location_id == loc_id & date <= backdistribute_date, filtered_daily := filtered_daily + backdistribute_value]
  dt[location_id == loc_id & date <= backdistribute_date, filtered_cumul := cumsum(filtered_daily)]
  dt[ , filtered_daily := c(0, diff(filtered_cumul)), by = .(location_id)]
  
  #clean up
  drop_cols <- c(
    "prop_daily_over_cumul", 
    "backdistribute_value")
  dt <- dt[ , -..drop_cols]
  
  return(dt)
}