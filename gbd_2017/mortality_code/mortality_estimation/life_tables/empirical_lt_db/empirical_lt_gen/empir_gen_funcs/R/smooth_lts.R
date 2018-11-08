#' Smooth targeted lifetable qx values by using lifetables in the same country/sex years surrounding it.
#'
#' @param empir_lt data.table with columns: ihme_loc_id, sex, year, source, age (numeric), qx, lx
#' @param smoothing_targets data.table with variables ihme_loc_id, sex
#' @param smoothing_option character, whether to smooth using wt_avg or half_half
#'                         Where wt_avg uses a vector of weights to determine a moving weighted average in a span of years before and after an observation
#'                         And half_half applies a weight of .5 to the actual observation and .5 combined across all other years within the by_vars (summing to 1)
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

smooth_lts <- function(empir_lt, smoothing_targets, by_vars, moving_average_weights) {
  # Order appropriately
  setorderv(empir_lt, c(by_vars, "year"))

  # Check to make sure rows are unique
  if(nrow(unique(empir_lt[, .SD, .SDcols = c(by_vars, "year")])) != nrow(empir_lt)) stop("by_vars and year variables do not uniquely identify observations")
  if(nrow(unique(smoothing_targets[, .SD, .SDcols = c('ihme_loc_id', 'sex')])) != nrow(smoothing_targets)) stop("ihme_loc_id and sex do not uniquely identify observations in smoothing targets")

  # Expand "both sexes" targets to sex-specific targets
  sex_crosswalk <- data.table(
    sex = c('male', 'female', 'both', 'both'),
    underlying_sex = c('male', 'female', 'male', 'female'))
  smoothing_targets <- merge(smoothing_targets, sex_crosswalk, by=('sex'), all.x = T, allow.cartesian=T)
  smoothing_targets[, sex := underlying_sex]
  smoothing_targets[, target_smooth := 1]
  smoothing_targets <- smoothing_targets[, .SD, .SDcols = c('ihme_loc_id', 'sex', 'target_smooth')]

  # Merge on targets to life tables
  empir_lt <- merge(empir_lt, smoothing_targets, by=c('ihme_loc_id', 'sex'), all.x = T)

  # Smooth qx
  empir_lt[target_smooth == 1,
           new_qx := moving_weighted_average(qx, moving_average_weights),
           by=by_vars]

  # Set new qx equal to qx
  empir_lt[target_smooth == 1, qx := new_qx]
  empir_lt[, new_qx := NULL]
  empir_lt[, target_smooth := NULL]

  lt_by_vars <- c(by_vars[by_vars != "age"], "year", "age")

  ## Regenerate lx
  setkeyv(empir_lt, lt_by_vars)
  qx_to_lx(empir_lt, assert_na = F)

  ## Format and output
  return(empir_lt)
}
