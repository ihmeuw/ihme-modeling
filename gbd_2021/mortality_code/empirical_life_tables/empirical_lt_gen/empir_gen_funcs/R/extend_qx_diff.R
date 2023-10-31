#' Extend qx up to age 105 using HMD regression parameters
#' logit(5q{x+5}) - logit(5qx) = B0 + B1*[logit(5qx) - logit(5q{x-5})] + location RE
#' Recursive prediction
#'
#' @param empir_lt data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, ax
#' @param hmd_qx_results data.table with variables sex, age, slope, intercept
#' @param by_vars character vector containing the variable names of all variables that uniquely identify the observations (except for age)
#'
#' @return returns empir_lt with the same variables, but modified qx extended to age 105
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

extend_qx_diff <- function(empir_lt, hmd_qx_results, by_vars) {

  empir_lt <- copy(empir_lt)

  ## remove any old age groups with qx > 1
  empir_lt[age >= 65 & qx >= 1, age_qx_over_1 := min(age), by = by_vars]
  empir_lt[is.na(age_qx_over_1), age_qx_over_1 := 999]
  empir_lt[, min_age_qx_over_1 := min(age_qx_over_1, na.rm=T), by = by_vars]
  empir_lt <- empir_lt[age < min_age_qx_over_1]

  ## Drop old age groups after qx starts to decrease, if this happens
  ## Make sure we keep 65 -- this is the lowest age we extend from
  change_neg <- get_min_qx_drop_age(empir_lt, by_vars, age_threshold = 65)
  if(nrow(change_neg) > 0){
    empir_lt <- merge(empir_lt, change_neg, by = by_vars, all.x=T)
    empir_lt <- empir_lt[is.na(min_age_qx_change_neg) | age < min_age_qx_change_neg]
  }

  ## Start should now be the last age that you have data for
  empir_lt[, start_age := max(age), by = by_vars]

  ## Drop all data where the max age in the series is less than 65 years -- need those years to do the rest of the extension
  empir_lt <- empir_lt[start_age >= 65]

  ## Expand grid to get all the old ages
  baseline_grid <- unique(empir_lt[, .SD, .SDcols = c(by_vars, "start_age")], by = by_vars)
  baseline <- list()

  new_ages <- c(0, 1, seq(5, 105, 5))
  for(new_age in new_ages) {
    list_item <- paste0(new_age)
    baseline[[list_item]] <- copy(baseline_grid)
    baseline[[list_item]][, age := new_age]
  }

  baseline_grid <- rbindlist(baseline)

  ## Merge everything together to get consistent age-series throughout
  empir_lt <- merge(baseline_grid, empir_lt, by = c(by_vars, "age", "start_age"), all.x = T)

  ## Generate logit qx and logit qx diff
  empir_lt[, logit_qx := logit(qx)]
  empir_lt <- empir_lt[order(age)]
  empir_lt[, diff_logit_qx_to := logit_qx - shift(logit_qx, type = "lag"), by = by_vars]

  ## Format HMD regression parameters long instead of wide
  hmd_qx_results <- copy(hmd_qx_results)
  empir_lt <- merge(empir_lt, hmd_qx_results, by = c("sex", "age"), all.x = T)

  ## Iterate over age to recursively predict logit qx for next age group
  ## diff_logit_qx_to is the qx-difference in logit space TO age aa from the previous age
  ## diff_logit_qx_from is the qx-difference in logit space FROM age aa to the next age
  setorderv(empir_lt, cols = c(by_vars, "age"))
  for(aa in seq(65, 100)){
    empir_lt[start_age == age & age == aa, pred_logit_qx := logit_qx]
    empir_lt[start_age <= aa & age == aa, diff_logit_qx_from := intercept + slope * diff_logit_qx_to]
    empir_lt[start_age <= aa, diff_logit_qx_to := shift(diff_logit_qx_from, type = "lag"), by = by_vars]
    empir_lt[start_age <= aa, pred_logit_qx := ifelse(!is.na(pred_logit_qx),
                                       pred_logit_qx,
                                       shift(pred_logit_qx, type = "lag") + diff_logit_qx_to),
                              by = by_vars]
  }
  empir_lt[age > start_age, logit_qx := pred_logit_qx]
  empir_lt[age > start_age, qx := invlogit(logit_qx)]

  setorderv(empir_lt, c(by_vars, "age"))
  
  ## Set qx to 0.99 if >= 1
  empir_lt[qx >= 1, qx := 0.99]

  ## subset variables
  empir_lt[, c("diff_logit_qx_to", "diff_logit_qx_from", "slope", "intercept", "pred_logit_qx", "start_age") := NULL]

  ## Regenerate lx
  setkeyv(empir_lt, c(by_vars, "age"))
  qx_to_lx(empir_lt, assert_na = F)

  ## Format and output
  return(empir_lt)
}

