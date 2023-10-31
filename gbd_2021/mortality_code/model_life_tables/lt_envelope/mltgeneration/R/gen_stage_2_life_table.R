#' Generate a Full Lifetable from stage-2 results, given mx, ax, and qx
#'
#' From post-iteration stage-2 results, with qx, mx, and ax values, generate a full lifetable
#'
#' @param iter_mx_qx_ax data.table with variables: sim, ihme_loc_id, year, sex, age (numeric), mx, qx
#' @param id_vars character vector of ID variables that uniquely identify observations in the dataset. Age must be the last variable specified.
#'
#' @return data.table with variables: sim, ihme_loc_id, sex, year, age, mx, ax, qx, lx, dx, nLx, age_length, Tx, ex
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

gen_stage_2_life_table <- function(iter_mx_qx_ax, id_vars) {
	stage_2_lt <- copy(iter_mx_qx_ax)

	## Prep dataset
		setkeyv(stage_2_lt, id_vars)
		assert_values(stage_2_lt, c("qx", "mx"), "not_na", quiet=T)

	## Generate lx and dx
		qx_to_lx(stage_2_lt)
		lx_to_dx(stage_2_lt)

	## Generate nLx based off of calculated lx and dx values, along with age_length and merged-in ax values
		gen_age_length(stage_2_lt)
		gen_nLx(stage_2_lt)

	## Enforce nLx = lx/mx at the terminal age group
		stage_2_lt[age == 110, nLx := lx/mx]

	## Generate Tx values
		gen_Tx(stage_2_lt, id_vars)

	## Generate ex values
		gen_ex(stage_2_lt)

	return(stage_2_lt)
}
