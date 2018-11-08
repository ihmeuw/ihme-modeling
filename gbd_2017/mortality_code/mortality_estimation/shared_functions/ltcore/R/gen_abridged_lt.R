#' Convert full (single-year-age) life tables to abridged (5-year age group) life tables using standard lifetable aggregation functions
#' 
#' Carry out single-year to five-year (abridged) lifetables
#' 
#' @param full_lt data.table with variables age, all the id_vars (generally location_id, year_id, sex_id, draw), qx, ax, dx
#' @param id_vars character, variables that, alongside age, uniquely identify observations
#' @param abridged_ages integer vector, ages to break the single-year ages into
#' @param assert_qx logical, whether to break if qx is outside of plausible bounds (>= 0 and <= 1). Will only warn if F. Default: T

#' @export
#' @return data.table with columns id_vars, age, qx and ax
#' @import data.table
#' @import assertable
#'
#' @examples 
#' \dontrun{
#' full_lt <- data.table(age = c(0:110), location_id = 1, year_id = 1950, sex_id = 1, qx = .2, ax = .5)
#' setkeyv(full_lt, c("location_id", "year_id", "sex_id", "age"))
#' qx_to_lx(full_lt)
#' lx_to_dx(full_lt)
#' id_vars <- c("location_id", "year_id", "sex_id")
#' data <- gen_abridged_lt(full_lt, id_vars, assert_qx = T)
#' }

gen_abridged_lt <- function(full_lt, id_vars, abridged_ages = c(0, 1, seq(5, 110, 5)), assert_qx = T) {
  full_lt <- copy(full_lt[, .SD, .SDcols = c(id_vars, "age", "qx", "ax", "dx")])

  # assign single year ages to abridged ages
  full_lt[, abridged_age := cut(age, breaks = c(abridged_ages, Inf), labels = abridged_ages,
                                     right = F)]
  full_lt[, abridged_age := as.integer(as.character(abridged_age))]

  # aggregate qx and ax
  full_lt[, px := 1 - qx]
  full_lt[, axdx_full_years := age - abridged_age]
  agg_lt_draws <- full_lt[, list(qx = (1 - prod(px)), ax = (sum((ax + axdx_full_years) * dx) / sum(dx))),
                            by = c(id_vars, "abridged_age")]
  setnames(agg_lt_draws, "abridged_age", "age")

  agg_lt_draws[age == max(abridged_ages), qx := 1]

  assertable::assert_values(agg_lt_draws, "qx", "gte", 0, warn_only = !assert_qx, quiet = T)
  assertable::assert_values(agg_lt_draws, "qx", "lte", 1, warn_only = !assert_qx, quiet = T)
  assertable::assert_values(agg_lt_draws, c("qx", "ax"), "not_na", warn_only = !assert_qx, quiet = T)

  return(agg_lt_draws)
}
