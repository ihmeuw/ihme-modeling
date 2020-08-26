#' Calculate ax for age groups Over 80
#' 
#' Calculate over-80 ax values based on a beta and constant based on qx and squared qx values
#' Currently no writeup of where these beta parameters come from.
#'
#' @param dt data.table with variables: ihme_loc_id, sex, age, sim, qx
#' @param ax_params data.table with variables: sex, age, par_qx, par_sqx, par_con
#' @param ax_scale scale factors for ax post recalc from qx, based on adjustments needed to scale to observed 95+ mx, only applies to 95+
#'
#' @export
#' 
#' @import data.table
#' @import assertable

gen_80plus_ax <- function(dt, ax_params, ax_scale) {
  
		dt <- merge(dt, ax_params, by = c("sex", "age"), all.x=T)
		dt[age >= 80, ax := par_qx * qx + par_sqx * (qx^2) + par_con]
		dt[, c("par_qx", "par_sqx", "par_con") := NULL]
		
		ax_scale <- ax_scale[source_type == "VR"]
		ax_scale <- ax_scale[, c("ihme_loc_id", "year", "sex", "ax_scale_factor", "terminal_age_start")]
		dt <- merge(dt, ax_scale, by = c("ihme_loc_id", "year", "sex"), all.x = T)
		dt[age >= terminal_age_start & !is.na(ax_scale_factor), ax := ax * ax_scale_factor]
		dt <- dt[, c("ax_scale_factor", "terminal_age_start") := NULL]

	  assert_values(dt[age >= 80 & age != 110], "ax", "not_na", quiet=T)
	  return(dt)
}
