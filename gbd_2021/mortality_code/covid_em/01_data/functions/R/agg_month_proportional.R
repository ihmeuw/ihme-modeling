#' @title Aggregate from week to month
#'
#' @description Convert weekly data to monthly data, assuming deaths are
#'   evenly distributed through the week for weeks that span multiple months.
#'
#' @param dt \[`data.table()`\]\cr
#'   Dataset with columns "time_start", "time_unit", "year_start", `id_cols`,
#'   and `measure`, where "time_unit" is always "week".
#' @param measure \[`character(1)`\]\cr
#'   Name of column with value to be aggregated.
#' @param id_cols \[`character()`\]\cr
#'   Columns that uniquely identify rows of `dt`. Must contain `time_start`,
#'   `time_unit`, and `year_start`.
#' @param na_rm \[`logical()`\]\cr default (TRUE)
#'   True or False whether NAs should be removed from aggregation
#'
#' @return \[`data.table()`\]\cr
#'   Dataset with columns "time_start", "time_unit", "year_start", `id_cols`,
#'   and `measure`, where "time_unit" is always "month".
#'
#' @details Assumes weeks that represent complete 7 day periods past January 1st.
#'
#' @examples
#' dt <- data.table(
#'   time_start = 1:5,
#'   time_unit = "week",
#'   year_start = 2019,
#'   deaths = c(2,4,6,8,10),
#'   group = 1
#' )
#' id_cols <- c("time_start", "time_unit", "year_start", "group")
#' dt <- agg_month_proportional(dt, "deaths", id_cols))

agg_month_proportional <- function(dt, measure, id_cols, na_rm = TRUE) {

  dt <- copy(dt)
  setnames(dt, measure, "measure")

  assertthat::assert_that(
    unique(dt$time_unit) == "week",
    msg = "dt must only have rows where time_unit = week"
  )

  # split into daily covid deaths (evenly by day in the week)
  week_merge_dt <- CJ(time_start = 1:52, day = 0:6)
  dt <- merge(dt, week_merge_dt, by = "time_start", allow.cartesian = T)

  # format date
  dt[, days_since_jan1 := (time_start - 1) * 7 + day]
  dt[, date := as.Date(days_since_jan1, origin = paste0(year_start, "-01-01"))]

  # divide "deaths" evenly across week based on number of days in the week
  # 53rd week may not have 7 days
  dt <- dt[lubridate::year(date) == year_start]
  dt[, ndays := .N, by = id_cols]
  dt[, measure := measure / ndays]
  dt[, ndays := NULL]

  # find month from date
  dt[, time_start := lubridate::month(date)]
  dt[, time_unit := "month"]

  # drop months which are incomplete
  dt[, ndays := .N, by = id_cols]
  dt <- dt[between(ndays, 28, 31)]
  dt[, ndays := NULL]

  # aggregate over month
  if(na_rm){
    dt <- dt[, list(measure = sum(measure, na.rm = T)), by = id_cols]
  } else {
    dt <- dt[, list(measure = sum(measure)), by = id_cols]
  }


  # return
  setnames(dt, "measure", measure)
  return(dt)
}
