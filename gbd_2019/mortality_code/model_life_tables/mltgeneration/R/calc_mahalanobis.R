#' Calculate Mahalanobis Function
#'
#' Calculate mahalanobis distance from empirical life tables to observed lifetable results, based on logit 45q15 and 5q0 values.
#' The Mahalanobis distance is a multi-dimensional approximation of the distance between a point and a distribution, given multiple variables to compare
#' Used to sub-select the most closest empirical life tables to match with observed results, across multiple variables (in this case, logit 5q0 and logit 45q15).
#' 
#' @param draw_num numeric, row number of the lt_draws data.table to use
#' @param lt_draws data.table with variables: ihme_loc_id, year, logit_calc_45q15, logit_calc_5q0
#'                Observations of the HIV-free results from the 5q0 and 45q15 processes (location/age/sex/year/draw-specific)
#' @param empir_lt data.table with variables: ihme_loc_id, year, sex, age, parent_ihme, super_region_name, region_name, empir_logit_45q15, empir_logit_5q0
#'
#' @return data.table with ihme_loc_id (of empir LT), year (of empir LT), est_ihme_loc_id, est_year, est_sim, n_match, and distance.
#' @export
#'
#' @examples
#' 
#' @import data.table

calc_mahalanobis <- function(draw_num, lt_draws, empir_lt) {
	lt_draw <- lt_draws[draw_num]
	if(nrow(lt_draw) != 1) stop("Can only calculate mahalanobis on one observation at a time")

	sel_ihme <- lt_draw$ihme_loc_id
	sel_year <- lt_draw$year
	sel_sex <- lt_draw$sex
	sel_sim <- lt_draw$sim
	sel_match <- lt_draw$n_match

	empir_lt <- empir_lt[sex == sel_sex]

	## Take variance, then feed into mahalanobis function
	## Use the lt_draw as the "center", or reference observation, on which the distance will be taken for each row
	empir_var <- var(empir_lt[, list(empir_logit_45q15, empir_logit_5q0)])
	distance <- mahalanobis(x = as.matrix(empir_lt[, list(empir_logit_45q15, empir_logit_5q0)]), 
							center= as.matrix(lt_draw[, list(logit_calc_45q15, logit_calc_5q0)]), 
							cov=empir_var)
	if(length(distance[is.na(distance)]) > 0) stop("Mahalanobis calculation failed -- NA values produced. Check input data for NA issues")

	lt_results <- cbind(empir_lt, distance)
	lt_results[, est_ihme_loc_id := sel_ihme]
	lt_results[, est_year := sel_year]
	lt_results[, est_sim := sel_sim]
	lt_results[, n_match := sel_match]

	return(lt_results)
}
