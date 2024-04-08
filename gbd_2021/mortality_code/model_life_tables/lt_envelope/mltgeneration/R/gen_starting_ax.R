#' Generate Starting ax Values for Ages 10-100
#'
#' Generate starting ax (average years lived in age interval for those who die in the age interval) values
#' for ages 10-100 based on dx values in the age and surrounding ages.
#' This implements ax graduation (however, not-iterated) based on bracketing values of dx/
#' After this, 80+ ax gets modified by gen_80plus_ax, and under-10 gets calculated in recalc_u10_nlx_mx_ax
#'
#' @param dt data.table with variables: ihme_loc_id, sex, age, sim, dx
#' @param id_vars character vector of id variables that uniquely identify each observation (last one must be age)
#'
#' @return None. Modifies given data.table in-place
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

gen_starting_ax <- function(dt, id_vars) {
		if(tail(id_vars, 1) != "age") stop("numeric variable age must be the last var specified in id_vars")

	## Double check age subsetting here
		setorderv(dt, id_vars)
		dt[, new_ax := ((-5/24) * shift(dx, 1, type="lag")
						+ 2.5 * dx
						+ (5/24) * shift(dx, 1, type="lead"))
						/ dx]
		dt[age >= 10 & age <= 100, ax := new_ax]
		dt[, new_ax := NULL]

		assert_values(dt[age >= 10 & age <= 100], "ax", "not_na", quiet=T)

	# ax < 0 frequently occurs if qx gets reset to .99 because of qx > 1 issues with the standard iterated life table
		dt[ax < 0 & age >= 10 & age <= 105, ax := 2.5]
		dt[ax > 5 & age >= 10 & age <= 105, ax := 2.5]
}
