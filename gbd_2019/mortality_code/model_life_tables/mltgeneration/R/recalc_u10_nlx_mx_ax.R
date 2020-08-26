#' Calculate under-10 nlx, mx, and ax values
#'
#' Calculate under-10-specific nlx, mx, and ax values based on Human Life-Table Database k1 parameters
#' Original k1 parameters supposed to come from Coale, A.J. and Demeny, P. 1983. Regional Model Life Tables and Stable Populations. Second Edition. Academic Press, N.Y.-L. 
#'
#' @param dt data.table with variables: ihme_loc_id, sex, age, sim, age_length, qx, lx, dx
#' @param id_vars character vector of id variables (last one must be age)
#'
#' @return data.table with variables: ihme_loc_id, sex, age, sim, age_length, qx, lx, dx
#' @export
#' 
#' @import data.table
#' @import assertable

recalc_u10_nlx_mx_ax <- function(dt, id_vars) {
	## Prep datasets
		if(tail(id_vars, 1) != "age") stop("numeric variable age must be the last var specified in id_vars")
		key_ids <- id_vars[id_vars != "age"]
		setorderv(dt, id_vars)

		under_10 <- dt[age <= 5]

	## Generate k1 parameter
	## Human Life-Table Database -- Type 6. Recalculated abridged life tables (from Coale-Demeny 1983)
	## http://www.lifetable.de/methodology.pdf pg. 7
		under_1 <- dt[age == 0, .SD, .SDcols=c(key_ids, "qx")]
		under_1[sex == "male" & qx > .01, k1 := 1.352]
		under_1[sex == "male" & qx <=.01, k1 := 1.653 - 3.013 * qx]
		under_1[sex == "female" & qx > .01, k1 := 1.361]
		under_1[sex == "female" & qx <= .01, k1 := 1.524 - 1.627 * qx]
		under_1[, qx := NULL]

		assert_values(under_1, "k1", "not_na", quiet=T)

		under_10 <- merge(under_10, under_1, by = key_ids)

	## Recalculate nLx for 0-1, 1-5, 5-10, 10-15
	## Age 0 recalculated using Human Life-Table Database
	## http://www.lifetable.de/methodology.pdf pg. 6, equation 5
		lx_1_5 <- under_10[age == 1, lx]
		lx_5_10 <- under_10[age == 5, lx]
		lx_10_15 <- dt[age == 10, lx]
		if(length(lx_1_5) != length(lx_5_10) | length(lx_5_10) != length(lx_10_15)) stop("Lx lengths do not match")

	## Apply k1 weights -- merge lx_1_merged on so that subsetted results (e.g. '& qx > .1') don't get mixed-up lx values
		under_10[age == 0, lx_1_merged := lx_1_5]
		under_10[age == 0, nLx := (0.05 + 3 * qx) + (0.95 - 3 * qx) * lx_1_merged]
		under_10[age == 0 & qx > .1, nLx := .35 + .65 * lx_1_merged]
		under_10[age == 1, nLx := k1 * lx_1_5 + (4 - k1) * lx_5_10]
		under_10[age == 5, nLx := 2.5 * (lx_5_10 + lx_10_15)]
		under_10[, c("lx_1_merged", "k1") := NULL]

	## Generate mx
		under_10[, mx := dx/nLx]

	## Generate capped ax values 
		under_10[, ax := (qx + (age_length * mx * qx) - (age_length * mx)) / (mx * qx)]

	# ax can be negative if qx is very low compared to mx, Recenter all values that occur this way
		under_10[(ax <= 0 | ax >= 1) & age == 0 & sex == "male", ax := .2]
		under_10[(ax <= 0 | ax >= 1) & age == 0 & sex == "female", ax := .15]
		under_10[(ax <= 0 | ax >= 4) & age == 1 & sex == "male", ax := 1.35]
		under_10[(ax <= 0 | ax >= 4) & age == 1 & sex == "female", ax := 1.36]
		under_10[(ax <= 0 | ax >= 5) & age == 5 & sex == "male", ax := 2.5]
		under_10[(ax <= 0 | ax >= 5) & age == 5 & sex == "female", ax := 2.5]

	assert_values(under_10, c("lx", "qx", "nLx", "mx", "ax"), "not_na", quiet=T)

	dt <- rbindlist(list(dt[age >= 10], under_10), use.names=T)
	return(dt)
}
