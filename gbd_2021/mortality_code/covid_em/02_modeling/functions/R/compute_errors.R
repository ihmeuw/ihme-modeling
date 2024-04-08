
#' @title Compute errors
#'
#' @description Compute relative, square, and regular errors.
#'
#' @param dt \[`data.table()`\]\cr
#'   Data with columns `observed_col` and `expected_col` as well as
#'   "year_start", "year_cutoff", "time_start", and "time_cutoff".
#' @param observed_col \[`character(1)`\]\cr
#'   Name of column in `dt` that contains observed values
#' @param expected_col \[`character(1)`\]\cr
#'   Name of column in `dt` that contains expected values
#' @param type \[`character(1)`\]\cr
#'   Either "in-sample" or "out-of-sample". Columns for year, time, and
#'   year and time cutoffs are used to determine how to subset `dt` before
#'   computing errors.
#'
compute_errors <- function(dt,
                           observed_col = "deaths_observed",
                           expected_col = "deaths_expected",
                           type = "in-sample") {

  # check
  assertable::assert_colnames(
    dt,
    c("year_start", "year_cutoff", "time_start", "time_cutoff",
      observed_col, expected_col),
    only_colnames = F, quiet = T
  )

  # subset
  dt <- copy(dt)
  if (type == "in-sample") {
    dt <- dt[(year_start < year_cutoff) |
               (year_start == year_cutoff & time_start < time_cutoff)]
  } else if (type == "out-of-sample") {
    dt <- dt[year_start == year_cutoff & time_start >= time_cutoff]
  } else {
    stop("`type` must be 'in-sample' or 'out-of-sample'")
  }

  # compute errors
  dt <- dt[, `:=`
    (error = (get(expected_col) - get(observed_col)),
     sq_error = (get(expected_col) - get(observed_col)) ^ 2,
     rel_error = (get(expected_col) - get(observed_col)) / get(observed_col))
  ]
  return(dt)

}
