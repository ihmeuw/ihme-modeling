
#' @title Expand annual population to weekly or monthly
#'
#' @description Uses interpolation and extrapolation to estimate weekly or
#'   monthly population from mid-year population.
#' @param dt \[`data.table()`\]\cr
#'   Dataset with columns "time_start", "time_unit", id_cols, and "population"
#' @param output_time_unit \[`character(1)`\]\cr
#'   Either "week" or "month"
#' @param id_cols \[`character()`\]\cr
#'   Columns that uniquely identify rows in `dt`.
#'
#' @return
#'   Dataset with the same columns as input `dt` but more rows, so that output
#'   has population for every week or month.

interpolate_pop <- function(dt, output_time_unit, id_cols) {

  dt <- copy(dt)

  if (output_time_unit == "month") {
    n_time_units <- 12
    dt[, `:=` (time_start = 6, time_unit = "month")]
  } else if (output_time_unit == "week") {
    n_time_units <- 53
    dt[is.na(time_start), `:=` (time_start = 26, time_unit = "week")]
  }

  # create 'time_id' which is continuous across years
  dt[, time_id := (year_start - estimation_year_start) * n_time_units + time_start]

  # how many 'time_id' values to extrapolate to?
  n_time_units_total <-
    (estimation_year_end - estimation_year_start + 1) * n_time_units

  # modified 'id_cols'
  new_id_cols <- c("time_id", setdiff(id_cols, c("time_start", "year_start")))

  # interpolate to get detailed time units
  dt[, population := log(population)]
  setkeyv(dt, new_id_cols)
  dt <- demUtils::interpolate(
    dt = dt,
    id_cols = new_id_cols,
    interpolate_col = "time_id",
    value_col = "population",
    interpolate_vals = 1:n_time_units_total,
    rule = 1 
  )
  dt[, population := exp(population)]
  dt <- dt[!is.na(population)]

  # extrapolate using rate of change to get population through end of 2020
  if (n_time_units_total > max(dt$time_id)) {
    dt <- demUtils::extrapolate(
      dt = dt,
      id_cols = new_id_cols,
      extrapolate_col = "time_id",
      value_col = "population",
      extrapolate_vals = min(dt$time_id):n_time_units_total,
      method = "rate_of_change",
      n_groups_fit = n_groups_fit
    )
  }

  # format back to year_start and time_start
  dt[, time_start := time_id %% n_time_units]
  dt[time_start == 0, time_start := n_time_units]
  dt[, year_start := (time_id - time_start) / n_time_units + estimation_year_start]
  dt[, time_id := NULL]

  return(dt)

}
