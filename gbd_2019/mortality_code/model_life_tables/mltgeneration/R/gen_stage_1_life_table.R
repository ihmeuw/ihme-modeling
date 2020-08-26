#' Generate initial life tables from qx values of the first-stage iteration and regression parameters (HIV-free if non-ZAF, with-HIV if ZAF)
#'
#' From post-iteration qx values, generate a full lifetable.
#' Recalculate over-85 qx based on HMD parameters and age 80 qx estimates.
#' Generate age 110 mx estimates based on age 105 mx estimates and regression parameters
#' Recalculate under-10 nLx, mx, and ax based on Coale-Demeny child values
#'
#' @param iter_qx data.table with variables: sim, ihme_loc_id, year, sex, age (numeric), qx
#'                Post-stage-1 qx values
#' @param mx_110 data.table with variables: sex, parlnmx
#'               Parameter to multiply by ln 5m105 to obtain mx at age 110 (terminal age group)
#' @param betas_qx_85 data.table with variables: sex, age, beta_age, beta_logitqx80, constant
#'                    Betas from logit model using selected high-quality HMD VR to convert from logit 5q80 to age-specific over-80 granular qx values.
#' @param ax_params data.table with variables: sex, age, par_qx, par_sqx, par_con
#'                  Constant and betas to generate ax values based on qx and qx^2 values. Unknown provenance
#' @param ax_scale scale factors for ax post recalc from qx, based on adjustments needed to scale to observed 95+ mx, only applies to 95+
#'
#' @return data.table with variables: sex, age, qx, mx, ax, etc. Full lifetable with adjusted LT values from iterated qx
#' @export
#'
#' @import data.table
#' @import assertable

gen_stage_1_life_table <- function(iter_qx, mx_110, betas_qx_85, ax_params, ax_scale) {

  iter_qx[age %in% c(95, 100, 105) & qx >= 1, qx := 0.999]

	## Prep dataset
		id_vars <- c("sim", "ihme_loc_id", "year", "sex", "age")
		id_vars_noage <- id_vars[id_vars != "age"]

		assert_colnames(iter_qx, c("sim", "ihme_loc_id", "year", "sex", "age", "qx"))
		assert_colnames(mx_110, c("sex", "parlnmx"))
		assert_colnames(betas_qx_85, c("sex", "age", "beta_age", "beta_logitqx80", "constant"))
		assert_colnames(ax_params, c("sex", "age", "par_qx", "par_sqx", "par_con"))

	## Create lx and dx values
		setkeyv(iter_qx, id_vars)

		print(iter_qx[1:10])
		qx_to_lx(iter_qx)
		print(iter_qx[1:10])
		lx_to_dx(iter_qx)

	## Initialize ax values
		gen_starting_ax(iter_qx, id_vars = id_vars)
		iter_qx <- gen_80plus_ax(iter_qx, ax_params, ax_scale)
		setkeyv(iter_qx, id_vars)

		gen_age_length(iter_qx)

	## Ages 10-105: Adjust mx, nLx, ax, mx, and nLx
		dt_10_plus <- iter_qx[age >= 10]
		dt_10_plus[, mx := qx_ax_to_mx(qx, ax, age_length)]
		gen_nLx(dt_10_plus, assert_na = F)

	# Recalibrate ax if it leads to out-of-bounds mx values
		dt_80_plus <- dt_10_plus[age >= 80]
		dt_80_plus[age <= 100 & (mx >= 1 | mx <= 0), ax := 1]

		dt_80_plus[, mx := qx_ax_to_mx(qx, ax, age_length)]
		gen_nLx(dt_80_plus, assert_na = F)

	# Generate age 110 estimates
		dt_80_plus <- gen_110_mx(dt_80_plus, mx_110, id_vars)

		dt_10_plus <- rbindlist(list(dt_10_plus[age < 80], dt_80_plus), use.names=T)
		iter_qx <- rbindlist(list(iter_qx[age <= 5], dt_10_plus), use.names=T, fill=T)

	## Ages 0-10: Using k1 values and lx values, recalculate nLx and then rollw that
	## calculation through the rest of life-table parameters
		iter_qx <- recalc_u10_nlx_mx_ax(iter_qx, id_vars = id_vars)
		assert_values(iter_qx[age != 110], c("mx", "ax", "dx", "lx", "nLx"), "not_na", quiet=T)

	## Generate Tx, ex, and regenerate terminal-age ax
		setkeyv(iter_qx, id_vars)
		gen_Tx(iter_qx, id_vars)
		gen_ex(iter_qx)
		iter_qx[age == 110, ax := ex]

		assert_values(iter_qx, c("Tx", "ex", "ax"), "not_na", quiet=T)

	## Reorder data, lowest-age-first
		setkeyv(iter_qx, id_vars)

	return(iter_qx)
}
