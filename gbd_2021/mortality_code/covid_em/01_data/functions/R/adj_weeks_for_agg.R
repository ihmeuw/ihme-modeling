
#' @title Manufacture Weeks for Aggregation
#' 
#' @description Aggregating a single year by week includes some days and drops some 
#' days on either end during most week formats. This is because those days are used 
#' to create full 7 day weeks. This function will prevent the gain or loss of days in a year 
#' so aggregation will roughly be only the days from that year between Jan 1 and Dec 31.
#' Right now this only works for the deaths rate excess file. It adjusts a measure
#' and person years to then aggregate to year.
#'
#' @param dt Dataset with weekly time units, measure, and person years
#'
#' @param id_cols Column names defining a groups of data for which the
#' adjustment should be applied.
#'
#' @param measure Name of variable (other than person_years) to be adjusted
#' 
#' @param year Single year to change weeks for aggregation 
#' 
#' @param location_week_types Dataset with ihme_loc_id and week type in 
#' ('isoweek', 'epiweek', 'ukweek')
#'
#' @note This is designed to add/subtract days to the first and last week of the year. 
#' The weeks are no longer comparable with other 7-day weeks and should immediately 
#' be aggregated.
#' 

adj_weeks_for_agg <- function(dt, id_cols, measure, year, location_week_types) {

  dt <- copy(dt)
  setnames(dt, measure, "measure")

  shift_by_cols <- union(id_cols, "time_unit")

  assertable::assert_colnames(
    dt,
    c(id_cols, "measure", "person_years", "time_unit", "year_start", "time_start")
  )

  # location week types
  locs_with_isoweek <- location_week_types[week_type == "isoweek", ihme_loc_id]
  locs_with_epiweek <- location_week_types[week_type == "epiweek", ihme_loc_id]
  locs_with_ukweek <- location_week_types[week_type == "ukweek", ihme_loc_id]
  
  # get first and last days of the year
  year_first_day <- as.Date(paste0(year,"-01-01"))
  year_last_day <- as.Date(paste0(year, "-12-31"))
  
  # find first day of week 1 of the year
  isoweek_first_day <- aweek::week2date(paste0(year,"-W01-1"), week_start = "Monday")
  epiweek_first_day <- aweek::week2date(paste0(year,"-W01-1"), week_start = "Sunday")
  ukweek_first_day <- aweek::week2date(paste0(year,"-W01-1"), week_start = "Saturday")
  
  # find last week of the year 
  years_w_53weeks <- c(2012, 2016, 2020)
  years_w_53weeks_uk <- c(2013, 2019)
  last_week <- ifelse(year %in% years_w_53weeks, 53, 52)
  last_week_uk <- ifelse(year %in% years_w_53weeks_uk, 53, 52)
 
  # find last day of last week of the year
  isoweek_last_day <- aweek::week2date(paste0(year,"-W",last_week,"-7"), week_start = "Monday")
  epiweek_last_day <- aweek::week2date(paste0(year,"-W",last_week,"-7"), week_start = "Sunday")
  ukweek_last_day <- aweek::week2date(paste0(year,"-W",last_week_uk,"-7"), week_start = "Saturday")
  
  # Subtract or add days to week 1 as needed for each week type #

  # create original person years column for comparison
  dt[, person_years_orig := person_years]
  
  if(isoweek_first_day > year_first_day && year > min(dt$year_start)){
    
    # find number of days from previous years final week to add to week 1
    day_difference <- as.numeric(difftime(isoweek_first_day, year_first_day, "days"))
    dt <- dt[order(location_id, year_start, time_unit, time_start)]
    dt[ihme_loc_id %in% locs_with_isoweek & time_unit == "week",
       ':='(measure_prev = shift(measure, type = "lag"),
            person_years_prev = shift(person_years, type = "lag")),
       by = shift_by_cols]
    dt[ihme_loc_id %in% locs_with_isoweek & time_unit == "week" & year_start == year & time_start == 1,
      ':='(measure = measure + ((day_difference/7) * measure_prev),
           person_years = person_years + ((day_difference/7) * person_years_prev))]
    dt[, ':='(measure_prev = NULL, person_years_prev = NULL)]
    
    assertable::assert_values(dt, "person_years", test = "gte", dt$person_years_orig)
    
  } else if(isoweek_first_day < year_first_day){
    
    # find number of days from previous year to remove from week 1
    day_difference <- as.numeric(difftime(year_first_day, isoweek_first_day, "days"))
    dt[ihme_loc_id %in% locs_with_isoweek & year_start == year & time_unit == "week" & time_start == 1, 
       ':='(measure = measure * ((7 - day_difference)/7),
            person_years = person_years * ((7 - day_difference)/7))]
    
    assertable::assert_values(dt, "person_years", test = "lte", dt$person_years_orig)
    
  }
  
  # reset person years original for changes 
  dt[, person_years_orig := person_years]
  
  if(epiweek_first_day > year_first_day && year > min(dt$year)){
    
    # find number of days from previous years final week to add to week 1
    day_difference <- as.numeric(difftime(epiweek_first_day, year_first_day, "days"))
    dt <- dt[order(location_id, year_start, time_unit, time_start)]
    dt[ihme_loc_id %in% locs_with_epiweek & time_unit == "week",
       ':='(measure_prev = shift(measure, type = "lag"),
            person_years_prev = shift(person_years, type = "lag")),
       by = shift_by_cols]
    dt[ihme_loc_id %in% locs_with_epiweek & time_unit == "week" & year_start == year & time_start == 1,
       ':='(measure = measure + ((day_difference/7) * measure_prev),
            person_years = person_years + ((day_difference/7) * person_years_prev))]
    dt[, ':='(measure_prev = NULL, person_years_prev = NULL)]
    
    assertable::assert_values(dt, "person_years", test = "gte", dt$person_years_orig)
    
  } else if(epiweek_first_day < year_first_day){
    
    # find number of days from previous year to remove from week 1
    day_difference <- as.numeric(difftime(year_first_day, epiweek_first_day, "days"))
    dt[ihme_loc_id %in% locs_with_epiweek & year_start == year & time_unit == "week" & time_start == 1, 
       ':='(measure = measure * ((7 - day_difference)/7),
            person_years = person_years * ((7 - day_difference)/7))]
    
    assertable::assert_values(dt, "person_years", test = "lte", dt$person_years_orig)
    
  }
  
  # reset person years original for changes 
  dt[, person_years_orig := person_years]
  
  if(ukweek_first_day > year_first_day && year > min(dt$year)){
    
    # find number of days from previous years final week to add to week 1
    day_difference <- as.numeric(difftime(ukweek_first_day, year_first_day, "days"))
    dt <- dt[order(location_id, year_start, time_unit, time_start)]
    dt[ihme_loc_id %in% locs_with_ukweek & time_unit == "week",
       ':='(measure_prev = shift(measure, type = "lag"),
            person_years_prev = shift(person_years, type = "lag")),
       by = shift_by_cols]
    dt[ihme_loc_id %in% locs_with_ukweek & time_unit == "week" & year_start == year & time_start == 1,
       ':='(measure = measure + ((day_difference/7) * measure_prev),
            person_years = person_years + ((day_difference/7) * person_years_prev))]
    dt[, ':='(measure_prev = NULL, person_years_prev = NULL)]
    
    assertable::assert_values(dt, "person_years", test = "gte", dt$person_years_orig)
    
  } else if(ukweek_first_day < year_first_day){
    
    # find number of days from previous year to remove from week 1
    day_difference <- as.numeric(difftime(year_first_day, ukweek_first_day, "days"))
    dt[ihme_loc_id %in% locs_with_ukweek & year_start == year & time_unit == "week" & time_start == 1, 
       ':='(measure = measure * ((7 - day_difference)/7),
            person_years = person_years * ((7 - day_difference)/7))]
    
    assertable::assert_values(dt, "person_years", test = "lte", dt$person_years_orig)
    
  }
  
  # reset person years original for changes 
  dt[, person_years_orig := person_years]
  
  # Subtract or add days to last week as needed for each week type #
  if(isoweek_last_day > year_last_day){
    
    # find number of days from to remove from final week
    day_difference <- as.numeric(difftime(isoweek_last_day, year_last_day, "days"))
    dt[ihme_loc_id %in% locs_with_isoweek & year_start == year & time_unit == "week" & time_start == last_week, 
       ':='(measure = measure * ((7 - day_difference)/7),
            person_years = person_years * ((7 - day_difference)/7))]
    
    assertable::assert_values(dt, "person_years", test = "lte", dt$person_years_orig)
    
  } else if(isoweek_last_day < year_last_day){
    
    # find number of days from next year to add to last week
    day_difference <- as.numeric(difftime(year_last_day, isoweek_last_day, "days"))
    dt <- dt[order(location_id, year_start, time_unit, time_start)]
    dt[ihme_loc_id %in% locs_with_isoweek & time_unit == "week",
       ':='(measure_next = shift(measure, type = "lead"),
            person_years_next = shift(person_years, type = "lead")),
       by = shift_by_cols]
    dt[ihme_loc_id %in% locs_with_isoweek & time_unit == "week" & year_start == year & time_start == last_week,
       ':='(measure = measure + ((day_difference/7) * measure_next),
            person_years = person_years + ((day_difference/7) * person_years_next))]
    dt[, ':='(measure_next = NULL, person_years_next = NULL)]
    
    assertable::assert_values(dt, "person_years", test = "gte", dt$person_years_orig)
    
  }
  
  # reset person years original for changes 
  dt[, person_years_orig := person_years]
  
  if(epiweek_last_day > year_last_day){
    
    # find number of days from to remove from final week
    day_difference <- as.numeric(difftime(epiweek_last_day, year_last_day, "days"))
    dt[ihme_loc_id %in% locs_with_epiweek & year_start == year & time_unit == "week" & time_start == last_week, 
       ':='(measure = measure * ((7 - day_difference)/7),
            person_years = person_years * ((7 - day_difference)/7))]
    
    assertable::assert_values(dt, "person_years", test = "lte", dt$person_years_orig)
    
  } else if(epiweek_last_day < year_last_day){
    
    # find number of days from next year to add to last week
    day_difference <- as.numeric(difftime(year_last_day, epiweek_last_day, "days"))
    dt <- dt[order(location_id, year_start, time_unit, time_start)]
    dt[ihme_loc_id %in% locs_with_epiweek & time_unit == "week",
       ':='(measure_next = shift(measure, type = "lead"),
            person_years_next = shift(person_years, type = "lead")),
       by = shift_by_cols]
    dt[ihme_loc_id %in% locs_with_epiweek & time_unit == "week" & year_start == year & time_start == last_week,
       ':='(measure = measure + ((day_difference/7) * measure_next),
            person_years = person_years + ((day_difference/7) * person_years_next))]
    dt[, ':='(measure_next = NULL, person_years_next = NULL)]
    
    assertable::assert_values(dt, "person_years", test = "gte", dt$person_years_orig)
    
  }
  
  # reset person years original for changes 
  dt[, person_years_orig := person_years]
  
  if(ukweek_last_day > year_last_day){
    
    # find number of days from to remove from final week
    day_difference <- as.numeric(difftime(ukweek_last_day, year_last_day, "days"))
    dt[ihme_loc_id %in% locs_with_ukweek & year_start == year & time_unit == "week" & time_start == last_week_uk, 
       ':='(measure = measure * ((7 - day_difference)/7),
            person_years = person_years * ((7 - day_difference)/7))]
    
    assertable::assert_values(dt, "person_years", test = "lte", dt$person_years_orig)
    
  } else if(ukweek_last_day < year_last_day){
    
    # find number of days from next year to add to last week
    day_difference <- as.numeric(difftime(year_last_day, ukweek_last_day, "days"))
    dt <- dt[order(location_id, year_start, time_unit, time_start)]
    dt[ihme_loc_id %in% locs_with_ukweek & time_unit == "week",
       ':='(measure_next = shift(measure, type = "lead"),
            person_years_next = shift(person_years, type = "lead")),
       by = shift_by_cols]
    dt[ihme_loc_id %in% locs_with_ukweek & time_unit == "week" & year_start == year & time_start == last_week_uk,
       ':='(measure = measure + ((day_difference/7) * measure_next),
            person_years = person_years + ((day_difference/7) * person_years_next))]
    dt[, ':='(measure_next = NULL, person_years_next = NULL)]
    
    assertable::assert_values(dt, "person_years", test = "gte", dt$person_years_orig)
    
  }
  
  # remove person years original 
  dt[, person_years_orig := NULL]
  
  # return
  setnames(dt, "measure", measure)
  return(dt)
  
}

