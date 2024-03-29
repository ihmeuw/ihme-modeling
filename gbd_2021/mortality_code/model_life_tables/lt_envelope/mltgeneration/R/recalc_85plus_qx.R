#' Recalculate over-80 qx values based on HMD VR relationships with age 80 qx
#'
#' Apply high-quality HMD VR relationships between logit 5q80 and age-specific over-80 granular qx values to re-calculated over-80 granular qx
#' GBD2015 MortCoD Appendix: Part 3.2
#' Gompertz old age updates are only applied to places that don't already have those ages (e.g. aren't matched with full lifetable output)
#'
#' @param dt data.table with variables: ihme_loc_id, sex, age, sim, qx
#' @param betas_qx_85 data.table with variables: sex, age, beta_age, beta_logitqx80, constant
#'                    Betas from logit model using selected high-quality HMD VR to convert from logit 5q80 to age-specific over-80 granular qx values
#'                    Note: age = seq(80,100,5)
#' @param id_vars character vector of id variables (last one must be age)
#'
#' @return data.table with variables: ihme_loc_id, sex, age, sim, qx
#'         Contains recalculated qx values
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

recalc_85plus_qx <- function(dt, betas_qx_85, id_vars) {
	
		if(tail(id_vars, 1) != "age") stop("numeric variable age must be the last var specified in id_vars")
		id_vars_noage <- id_vars[id_vars != "age"]

	## Merge age-85 + qx values, regression betas, and age-80 logit qx
		over_80 <- dt[age >= 80]
		over_80[, l_qx := logit(qx)]
		over_80 <- merge(over_80, betas_qx_85, by=c("sex", "age"), all.x=T)
		assert_values(over_80[age <= 100], c("beta_age", "beta_logitqx80", "constant"), "not_na")

	## Generate predicted differences from the age and the previous age, then apply then to each age individually
		setkeyv(over_80, id_vars)
		over_80[age == 80, predicted_diff := constant + beta_logitqx80 * over_80[age == 80, l_qx] + beta_age]	

		for(a in seq(85, 105, 5)) {
			prev_age <- a - 5
			over_80[age == a, predicted_diff := constant + beta_logitqx80 * over_80[age == 80, l_qx] + beta_age]	
			over_80[age == a, qx := invlogit(over_80[age == prev_age, l_qx] + over_80[age == prev_age, predicted_diff])]
			over_80[age == a, l_qx := logit(qx)]
		}

		over_80[age == 110, qx := 1] # Terminal age group qx = 1

		over_80 <- over_80[, .SD, .SDcols = c(id_vars, "qx")]

		setnames(over_80, "qx", "regressed_qx")

		dt <- merge(dt, over_80, by = id_vars, all.x=T)
		dt[is.na(qx) & age > 80, qx := regressed_qx]
		dt[, regressed_qx := NULL]

		# assert_values(dt[age >= 85], "qx", "not_na", quiet=T)

		# Cap qx at .99 -- this can be a downstream result of standard LT iteration results (see BWA)
		# Either the iteration func outputs NA (because no LTs have it)
		# Or the qx replacement has qx > 1 in the reference categories or qx value itself, leading the logit/invlogit to output NaN
		# The over-1 qx also being a result of the scalars chosen through iteration
		dt[qx > 1 | is.na(qx), qx := .99] 

	return(dt)
}
