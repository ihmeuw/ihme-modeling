#' Generate mx and nLx for 110 age groups based on age 105 values
#'
#' Given a data.table with lx, mx, and the 105 age group, generate mx for the 110 age group.
#' Applies parameters to transform the log mx at age 105 to the age 110 mx value.
#'
#' @param dt data.table with variables: ihme_loc_id, sex, age, sim, mx, lx
#' @param mx_params data.table with variables: sex, parlnmx
#'                  Parameters to multiply by ln 5m105 to obtain mx at age 110 (terminal age group)
#' @param id_vars character vector of id variables (last one must be age)
#' @export
#'
#' @import data.table
#' @import assertable

gen_110_mx <- function(dt, mx_params, id_vars) {

	if(tail(id_vars, 1) != "age") stop("numeric variable age must be the last var specified in id_vars")
	dt <- merge(dt, mx_params, by = "sex")

	setkeyv(dt, id_vars)
	dt[age == 110, mx := exp(log(dt[age == 105, mx]) * parlnmx)]
	dt[, parlnmx := NULL]
	dt[age == 110, nLx := lx/mx]

	assert_values(dt[age==110], "nLx", "not_na", quiet=T)

	return(dt)
}
