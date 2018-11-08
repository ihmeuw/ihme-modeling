#' Convert abridged (5-year age group) life tables to full (single-year-age) life tables using specified regression parameters or lx spline
#' 
#' Carry out 5-year to single-year conversion using regression parameters to adjust qx and px
#' 
#' @param abridged_lt data.table with variables age, all the id_vars (generally location_id, year_id, sex_id [required], draw [required]), age_length, qx, ax, px
#' @param regression_fits data.table with variables sex_id, age, intercept, slope
#' @param id_vars character, variables that, alongside age, uniquely identify observations
#' @param max_age integer, max age value that is being computed (default: 110)
#' @param lx_spline_start_age integer, starting age (in numbers, not age_group_id), to start using a lx spline rather than regression fits. 
#'                            Use 0 to use lx spline for all ages, or integer > 110 or Inf to use regression results for all
#' @param lx_spline_end_age integer, ending age (in numbers, not age_group_id), to stop using a lx spline rather than regression fits. 
#'                          Non-inclusive (e.g. a value of 100 means that spline will be used through age 99, and the regression fits will be used for 100+)
#' @param assert_qx logical, whether to break if qx is outside of plausible bounds (>= 0 and <= 1). Will only warn if F. Default: T

#' @export
#' @return data.table with columns id_vars, age, qx, ax
#' @import data.table
#' @import assertable
#'
#' @examples 
#' \dontrun{
#' abridged_lt <- data.table(age = c(0, 1, seq(5, 110, 5)), location_id = 1, year_id = 1950, sex_id = 1, qx = .2, ax = .5, px = .8)
#' abridged_lt[, age_length := 5]
#' abridged_lt[age == 0, age_length := 1]
#' abridged_lt[age == 1, age_length := 4]
#' regression_fits <- data.table(sex_id = 1, age = c(0:110), intercept = 0, slope = .5)
#' regression_fits[, slope := slope + .1 * age]
#' id_vars <- c("location_id", "year_id", "sex_id")
#' data <- gen_full_lt(abridged_lt, regression_fits, id_vars, assert_qx = F)
#' }

gen_full_lt <- function(abridged_lt, regression_fits, id_vars, max_age = 110, lx_spline_start_age = 15, lx_spline_end_age = 100, assert_qx = T) {
  print(paste0("Generating full life table with lx spline starting at age ", lx_spline_start_age))
  rescale_predicted_qx <- function(lt, id_vars, qx_varname, abridged_px_varname) {
    lt[, pred_px := 1 - get(qx_varname)]

    ## Rescale single-year qx values to ensure that px values are consistent 
    lt[, pred_px_abridged := prod(pred_px), by = c(id_vars, "abridged_age")]
    lt[, adjustment_factor := get(abridged_px_varname) / pred_px_abridged]
    lt[, adjustment_factor := adjustment_factor ^ (1/age_length)]
    lt[, pred_px_adjusted := pred_px * adjustment_factor]
    lt[, pred_qx_adjusted := 1 - pred_px_adjusted]
    lt[, c(qx_varname, "px", "pred_px", "pred_px_adjusted", "adjustment_factor") := NULL]
  }

  assertable::assert_colnames(abridged_lt, c(id_vars, "age"), only_colnames = F)

  abridged_lt <- copy(abridged_lt)
  first_last_ages <- c(0, max_age)
  first_last_dt <- abridged_lt[age %in% first_last_ages]

  setkeyv(abridged_lt, c(id_vars, "age"))
  qx_to_lx(abridged_lt, terminal_age = max_age)

  if(lx_spline_start_age < max_age) {
    ## Run the spline here -- start the spline at the starting age as well to avoid big issues 
    full_lt_spline <- abridged_lt[age >= (lx_spline_start_age-5), list(lx=spline(age, lx, method="hyman", xout = min(age):max_age)$y,
                                          age=(min(age):max_age),
                                          abridged_age = c(rep(age[age != 110], age_length[age != 110]), 110)),
                                          by=id_vars]

    setkeyv(full_lt_spline, c(id_vars, "age"))

    lx_to_qx_long(full_lt_spline, terminal_age = max_age)
    full_lt_spline[, c("lx") := NULL]
    full_lt_spline <- full_lt_spline[!age %in% first_last_ages]
  } else {
    full_lt_spline <- data.table(age = NA, qx = NA, ax = NA)
    full_lt_spline[, (id_vars) := NA]
  }
  # Expand dataset by the number of years in each age group
  setnames(abridged_lt, "age", "abridged_age")
  setkeyv(abridged_lt, c(id_vars, "abridged_age"))

  full_lt_draws <- abridged_lt[!abridged_age %in% first_last_ages, list(full_age = abridged_age:(abridged_age + age_length - 1),
                                      age_length, qx, px),
                                 by = c(id_vars, "abridged_age")]

  full_lt_draws <- merge(full_lt_draws, regression_fits, by.x = c("sex_id", "full_age"), by.y = c("sex_id", "age"))

  ## Calculate predicted single-year qx using regression parameters
  full_lt_draws[, reg_qx := exp((log(qx) * slope) + intercept)]

  full_lt_draws[, c("slope", "intercept") := NULL]
  setnames(full_lt_draws, "qx", "abridged_qx")

  rescale_predicted_qx(full_lt_draws, id_vars, qx_varname = "reg_qx", abridged_px_varname = "px")

  setnames(full_lt_draws, "full_age", "age")
  setnames(full_lt_draws, "pred_qx_adjusted", "qx")

  full_lt_draws <- rbindlist(list(full_lt_draws[age < lx_spline_start_age | age >= lx_spline_end_age, .SD, .SDcols = c(id_vars, "age", "qx")],
                               full_lt_spline[age >= lx_spline_start_age & age < lx_spline_end_age, .SD, .SDcols = c(id_vars, "age", "qx")]),
                          use.names = T)

  full_lt_draws[, ax := .5]
  full_lt_draws <- full_lt_draws[, .SD, .SDcols = c(id_vars, "age", "qx", "ax")]
  first_last_dt <- first_last_dt[, .SD, .SDcols = c(id_vars, "age", "qx", "ax")]

  full_lt_draws <- rbindlist(list(full_lt_draws[!age %in% first_last_ages], first_last_dt), use.names = T)
  setkeyv(full_lt_draws, c(id_vars, "age"))
  full_lt_draws[age == max_age, qx := 1]

  assertable::assert_values(full_lt_draws, "qx", "gte", 0, warn_only = !assert_qx, quiet = T)
  assertable::assert_values(full_lt_draws, "qx", "lte", 1, warn_only = !assert_qx, quiet = T)
  
  return(full_lt_draws)
}
