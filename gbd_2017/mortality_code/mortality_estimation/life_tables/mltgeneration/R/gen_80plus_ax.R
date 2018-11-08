#' Calculate ax for age groups Over 80
#' 
#' Calculate over-80 ax values based on a beta and constant based on qx and squared qx values
#'
#' @param dt data.table with variables: ihme_loc_id, sex, age, sim, qx
#' @param ax_params data.table with variables: sex, age, par_qx, par_sqx, par_con
#'
#' @return returns the given data.table, with over-80 ax (also modifies the given data.table in-place?)
#' @export
#'
#' @examples
#' 
#' @import data.table
#' @import assertable

gen_80plus_ax <- function(dt, ax_params) {
		dt <- merge(dt, ax_params, by = c("sex", "age"), all.x=T)
		dt[age >= 80, ax := par_qx * qx + par_sqx * (qx^2) + par_con]
		dt[, c("par_qx", "par_sqx", "par_con") := NULL]

	assert_values(dt[age >= 80 & age != 110], "ax", "not_na", quiet=T)
	return(dt)
}
