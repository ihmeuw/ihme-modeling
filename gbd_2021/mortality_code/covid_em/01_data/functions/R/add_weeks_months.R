#' @title Add Weeks and months
#'
#' @description Takes dataset with a date column and adds weekly and monthly
#' time start and time unit
#'
#' @param dt Dataset to add weekly and monthly time units. Must have columns:
#'    * "date": time in standard date format
#' @param gbd_year Year to pull location hierarchies from to add ihme_loc_id
#' where missing
#' @param data_code_dir Location of processing_covid_em repo to pull
#' `location_week_types.csv` from
#'
#' @details Type of week reported by country specific VR data
#'   * "iso" Week starts Monday & ends Sunday such that each week has 7 days
#'   * "epi" Week starts Sunday & ends Saturday such that each week has 7 days
#'   * "uk" Week starts Saturday & ends Friday such that each week has 7 days
#'   * "standard" lubridate week returns 7 day weeks starting on Jan 1. where week 53
#'   has 1 or 2 days depending on leap year. We are combining week 53 with week 52
#'   so that the last week 52 has 9 or 8 days. This is used where the week type is
#'   not specified.
#'
#' @example
#' dt <- data.table(
#'   date = as.Date("2021-01-01)
#' )
#' dt <- add_weeks_months(
#'   dt = dt,
#'   gbd_year = 2020 ,
#'   data_code_dir = ""
#' )


add_weeks_months <- function(dt,
                             gbd_year,
                             data_code_dir,
                             type = ""){

  # get location week types
  location_week_types <- fread(fs::path())
  locs_with_isoweek <- location_week_types[week_type == "isoweek", ihme_loc_id]
  locs_with_epiweek <- location_week_types[week_type == "epiweek", ihme_loc_id]
  locs_with_ukweek <- location_week_types[week_type == "ukweek", ihme_loc_id]

  # get location set for gbd year
  locs <- demInternal::get_locations(gbd_year = gbd_year)
  covid_loc_map <- demInternal::get_locations(gbd_year = gbd_year, location_set_name = "COVID-19 modeling")
  covid_only_locs <- covid_loc_map[!location_id %in% unique(locs$location_id)]
  locs <- rbind(locs, covid_only_locs)

  # replace or add ihme_loc_id
  dt <- copy(dt)
  has_ihme_loc <- "ihme_loc_id" %in% names(dt)
  if(!has_ihme_loc){
    dt <- merge(
      dt,
      locs[, c("location_id","ihme_loc_id")],
      by = "location_id",
      all.x = TRUE
    )
    dt[location_id == 99999, ihme_loc_id := "ITA_99999"]
    dt[location_id == 999, ihme_loc_id := "IND_999"]
  }

  # add week
  dt_w <- copy(dt)
  dt_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek), ':=' (time_start = lubridate::week(date),
                                                                                          year_start = lubridate::year(date))]
  dt_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek) & time_start == 53, time_start := 52]
  dt_w[ihme_loc_id %in% locs_with_isoweek, ':=' (time_start = lubridate::isoweek(date),
                                                 year_start = lubridate::isoyear(date))]
  dt_w[ihme_loc_id %in% locs_with_epiweek, ':=' (time_start = lubridate::epiweek(date),
                                                 year_start = lubridate::epiyear(date))]
  dt_w[ihme_loc_id %in% locs_with_ukweek, ':=' (week_year = aweek::date2week(date, time_start = "sat", floor_day = T))]
  dt_w[ihme_loc_id %in% locs_with_ukweek, ':=' (time_start = as.numeric(substr(week_year,7,8)),
                                                year_start = as.numeric(substr(week_year,1,4)))]
  
  if (type != "pop") {
    dt_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2020, ':=' (time_start = time_start + 1)]
    dt_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2019 & time_start == 53, ':=' (time_start = 1, year_start = 2020)]
  }
  dt_w[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]
  dt_w[, ':=' (week_year = NULL, time_unit = "week")]

  # add month
  dt_m <- copy(dt)
  dt_m[, ':=' (time_start = lubridate::month(date), year_start = lubridate::year(date), time_unit = "month")]
  dt_m[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]

  dt <- rbind(dt_m, dt_w)
  if(!has_ihme_loc){
    dt <- dt[, -c("ihme_loc_id")]
  }
  return(dt)

}