#' Generate average person-years lived by people in the age interval (nLx)
#'
#' Given a data.table with average years lived of those who died in the age interval (ax),
#' number who died in the age interval (dx), number at the beginning of the age interval (lx), 
#' and death rate (mx), calculate and generate nLx. 
#' nLx = age_group_length * lx_next_age + ax * dx
#' Essentially, nLx = years lived by survivors + years lived by those who died
#' nLx_terminal_age = lx / mx
#' For terminal ages, assume that average person years lived equals number at the start of the age group divided by the death rate (constant mortality assumption)
#'
#' @param dt data.table with lx, ax, dx, mx, age, and age_length variables. The data.table must be keyed by unique identifiers, with age as the final key variable.
#' @param terminal_age numeric, the terminal age group for the data. Default: 110.
#' @param assert_na logical, whether to check for NA values in the generated nLx variable.
#'
#' @return None. Modifies the given data.table in-place.
#' @export
#'
#' @examples
#' 
#' @import data.table
#' @import assertable

gen_nLx <- function(dt, terminal_age = 110, assert_na=T) {
  check_key(dt)
  id_vars_noage <- key(dt)[key(dt) != "age"]
  
  dt[, nLx := age_length * shift(lx, 1, type = "lead") + ax * dx, by = id_vars_noage]
  dt[age == terminal_age, nLx := lx/mx]
  if(assert_na == T) assertable::assert_values(dt[age != terminal_age], "nLx", "not_na", quiet=T)
}
