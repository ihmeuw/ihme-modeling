#' Convert a long dataset of person-years lived (lx) to probability of death (qx)
#' 
#' Given a dataset with an lx variable, convert to qx. 
#' qx_current_age = 1 - (lx_next_age / lx_current_age)
#' qx_terminal_age = lx_terminal_age
#' Modifies the dataset in-place.
#'
#' @param dt data.table keyed by variables uniquely identifying the dataset. 
#'          Must also include a variable called lx (non-NA for first age). 
#'          \strong{NOTE:} Variable age must be the last in the key specified.
#' @param terminal_age numeric representing the last age group in the dataset. Default: 110.
#' @param assert_na logical for whether to check for NA values in the qx variables.
#'
#' @return None. Modifies the given data.table in-place.
#' @export
#'
#' @examples
#' 
#' @import data.table
#' @import assertable

lx_to_qx_long <- function(dt, terminal_age = 110, assert_na = T) {
  check_key(dt)
  id_vars_noage <- key(dt)[key(dt) != "age"]
  
  dt[, qx := 1 - (shift(lx, 1, type = "lead")/lx), by = id_vars_noage]
  dt[age == terminal_age, qx := 1]
  if(assert_na == T) assertable::assert_values(dt, "dx", "not_na", quiet=T)
}
