#' Recalculate ax values to produce re-optimized qx and ax values
#'
#' For results where mx is over 1 or ax is below 0, recalculate qx directly from mx (rather than mx and ax).
#'  Then, recalculate ax based off of this regenerated qx value.
#'
#' @param lifetable data.table with variables: id_vars, mx, qx, ax
#' @param id_vars character vector of variables that uniquely identify observations in your dataset
#'
#' @return Modifies lifetable in-place, returning modified values of ax and qx for observations that violated qx < 1 or ax >= 0 restrictions
#' 
#' @export
#'
#' @import data.table
#' @import assertable

recalc_ax <- function(lifetable, id_vars) {

  ## Create age variable and add a qx modification indicator
  gen_age_length(lifetable)
  lifetable[(qx >= 1 & age != 110) | ax < 0, ax_recalc := 1]

  ## Generate qx based on mx alone
  lifetable[ax_recalc == 1, qx := mx_to_qx(mx, age_length)]

  ## Regenerate ax based on mx and qx
  lifetable[ax_recalc == 1, ax := mx_qx_to_ax(mx, qx, age_length)]

  ## Remove indicator
  lifetable[, c("age_length", "ax_recalc") := NULL]

  ## For age 110, reset qx = 1
  lifetable[age == 110, qx := 1]

  ## Run assertions
  assert_values(lifetable, "qx", "lte", test_val = 1, quiet=T)
  assert_values(lifetable, "ax", "gte", test_val = 0, quiet=T)

  ## Modifies in-place, no need to return values
}
