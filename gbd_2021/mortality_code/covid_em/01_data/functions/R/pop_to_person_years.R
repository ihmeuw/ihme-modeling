
#' @title Population to person-years
#'
#' @description Take "population" column representing mid-period population
#'   and compute "person-years".
#'
#' @param dt Dataset to compute person-years for. Must have columns:
#'   * "time_unit" : equals "month", "week", or "day"
#'   * "time_start" : equals value of time. ex: March has time unit "month"
#'       and time start "3".
#'   * "year_start" : used to determine leap year.
#'   * "population" : mid-period population
#' @param num_units Length of time period to multiply by n_days to get person
#'   years for data spanning multiple months or weeks. Should be 1 if data is
#'   weekly or monthly
#' @param week_type `default standard` Type of week reported by country specific VR data
#'   * "iso" Week starts Monday & ends Sunday such that each week has 7 days
#'   * "epi" Week starts Sunday & ends Saturday such that each week has 7 days
#'   * "uk" Week starts Saturday & ends Friday such that each week has 7 days
#'   * "standard" lubridate week returns 7 day weeks starting on Jan 1. where week 53
#'   has 1 or 2 days depending on leap year. We are combining week 53 with week 52
#'   so that the last week 52 has 9 or 8 days
#'
#' @details
#' person-years = mid-period-population * (n-days-in-period / n-days-in-year).
#'
#' @examples
#' dt <- data.table(
#'   time_unit = "month",
#'   time_start = c(1:3),
#'   year_start = 2020,
#'   population = c(1000, 1002, 1005)
#' )
#' dt <- pop_to_py(dt)


pop_to_py <- function(dt, num_units, week_type = "standard") {

  # check
  assertable::assert_colnames(
    dt, colnames = c("time_unit", "time_start", "year_start", "population"),
    only_colnames = F, quiet = T
  )
  if ("n_days" %in% names(dt)) {
    warning("`n_days` column present in `dt` will be dropped by `pop_to_py`.")
  }

  # compute width of time interval in days
  dt[time_unit == "month",
     n_days := lubridate::days_in_month(
       lubridate::ymd(paste0(year_start, "/", time_start, "/", 01))
     )
     ]
  if (week_type %in% c("iso","epi","uk")){
    dt[time_unit == "week",
       n_days := 7]
  } else if (week_type == "standard") {
    dt[time_unit == "week",
       n_days := ifelse(
         time_start <= 51, 7,
         ifelse(lubridate::leap_year(year_start), 9, 8)
       )
       ]
  } else {
    stop(paste0("Invalid week_type specified: ", week_type))
  }

  dt[time_unit == "day", n_days := 1]
  dt[, n_days := n_days * num_units]

  # calculate person-years
  dt[, person_years := (population * n_days) /
       (365 + (1 * lubridate::leap_year(year_start)))]

  # cleanup
  dt[, n_days := NULL]

  return(dt)

}

