#' Smooth targeted lifetable qx values by using lifetables in the same country/sex years surrounding it.
#'
#' @param empir_lt data.table with columns: ihme_loc_id, sex, year, source, age (numeric), qx, lx
#' @param smoothing_targets data.table with variables ihme_loc_id, sex
#' @param by_vars character vector of variables that would uniquely identify each observation if combined with the variable year.
#' @param moving_average_weights numeric vector of weights to use for moving average (if using). Must be symmetric, and will produce NA values for places where there are not enough values on either side
#'                               Example: c(1, 1.5, 6, 1.5, 1) uses a relative weight of 6 for the observation itself, 1.5 for lag/lead 1, 1 for lag/lead 2, and 0 for all other observations
#'
#' @return returns empir_lt with the same variables, but modified values for qx and lx based off of the combined, smoothed lifetables
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

smooth_lts <- function(empir_lt, by_vars, moving_average_weights) {
  
  empir_lt <- copy(empir_lt)
  
  # Order appropriately
  setorderv(empir_lt, c(by_vars, "year"))

  # Check to make sure rows are unique
  if(nrow(unique(empir_lt[, .SD, .SDcols = c(by_vars, "year")])) != nrow(empir_lt)) stop("by_vars and year variables do not uniquely identify observations")

  # Smooth qx
  empir_lt[, qx := moving_weighted_average(qx, moving_average_weights), by = by_vars]

  ## Regenerate lx
  setkeyv(empir_lt, c("year", by_vars))
  qx_to_lx(empir_lt, assert_na = F)

  ## Format and output
  return(empir_lt)
}
