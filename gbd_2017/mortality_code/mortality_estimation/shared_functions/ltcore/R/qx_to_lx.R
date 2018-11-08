#' Convert probability of dying (qx) to proportion of survivors in the beginning of an age group (lx)
#'
#' Given a data.table with a qx variable, calculate and generate lx.
#' Starting with lx = 1 at age 0, lx = lx_previous * (1-qx_previous)
#'
#' @param dt data.table with variables: qx (non-NA for first age) \strong{NOTE:} Variable age must be the last in the key specified.
#' @param terminal_age numeric representing the last age to be set. 
#' @param assert_na logical for whether to check for NA values in the generated lx variables.
#'
#' @return None. Modifies the given data.table in-place.
#' @export
#'
#' @examples
#' 
#' @import data.table
#' @import assertable

qx_to_lx <- function (dt, terminal_age = 110, assert_na=T) {
	check_key(dt)
	id_vars_noage <- key(dt)[key(dt) != "age"]

	dt[, lx := 1]

    # We append 1 at the beginning to make sure each row uses the previous row's qx value
	dt[, lx := lx[1] * cumprod(c(1,head(1-qx,-1))), by = id_vars_noage]

	# qx at age 110 = 1 -- doesn't seem to cause an issue, but use the equation below if it does throw issues
	# dt[age == terminal_age, lx := dt[age == (terminal_age - 5), lx] * (1 - dt[age == (terminal_age - 5), qx])]
	if(assert_na == T) assertable::assert_values(dt, "lx", "not_na", quiet=T)
}
