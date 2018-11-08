#' Convert between person-years lived and the number dying in the age interval
#' 
#' Given a dataset with an lx variable, convert to dx. 
#' dx_current_age = lx_current_age - lx_next_age
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

lx_to_dx <- function(dt, terminal_age = 110, assert_na=T) {
	# dx = lx - lx_next
	# In terminal age group, dx = lx
	check_key(dt)
	id_vars_noage <- key(dt)[key(dt) != "age"]

	dt[, dx := lx - shift(lx, 1, type = "lead"), by = id_vars_noage]
	dt[age == terminal_age, dx := lx]
	if(assert_na == T) assertable::assert_values(dt, "dx", "not_na", quiet=T)
}
