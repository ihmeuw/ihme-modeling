#' Create With-HIV Envelope Results
#'
#' Create with-HIV envelope results based on with-HIV lifetables, under-5 envelope results, and population.
#' Deaths = mx * population, but substitute in under-5 envelope for under-5 ages. Also, modify 95+ deaths for non-HMD countries.
#'
#' @param whiv_lt data.table with variables: sim, ihme_loc_id, year, sex, age (numeric), mx, lx, Tx
#' @param population data.table with variables: sim, ihme_loc_id, year, sex, age (numeric), population
#' @param u5_envelope data.table with variables: sim, ihme_loc_id, year, sex, age (character, enn/lnn/pnn/1/2/3/4), deaths
#' @param mx_params data.table with variables: sex, parmx
#' @param hmd_indic logical (T/F) -- is this country a HMD country? If not, run calc_95_no_hmd_mx
#'
#' @return data.table with variables: sim, ihme_loc_id, sex, year age, deaths
#' @export
#'
#' @import data.table
#' @import assertable

gen_whiv_env <- function(whiv_lt, population, u5_envelope, mx_params, hmd_indic, env_ids) {
	
	## Subset with-HIV lifetable to ages 95 and below, and regenerate mx in terminal age group based on lx and Tx values
		id_vars <- c("sim", "ihme_loc_id", "year", "sex", "age")
		whiv_lt <- whiv_lt[age <= 95, .SD, .SDcols = c(id_vars, "mx", "lx", "Tx")]
		whiv_lt[age == 95, mx := lx/Tx]

	## For non-HMD countries, adjust 95+ based on 90-94 relationships
		if(hmd_indic == F) {
			whiv_lt <- calc_95_no_hmd_mx(env_mx = whiv_lt, mx_params = mx_params, id_vars = id_vars)
		}

	## Merge with-HIV lifetables and population to generate deaths based on mx and population
		pop_ids <- id_vars[id_vars != "sim"]
		orig_rows <- nrow(whiv_lt)
		whiv_env <- merge(whiv_lt, population[, .SD, .SDcols = c(pop_ids, "population")], by = pop_ids)
		if(orig_rows != nrow(whiv_lt)) stop("with-HIV lifetable and Population values did not merge correctly")

		whiv_env[, deaths := mx * population]
		whiv_env[, c("mx", "lx", "Tx", "population") := NULL]

	## Add on the under-5 envelope results
		u5_envelope <- agg_u5_env(u5_envelope)

		orig_rows <- nrow(whiv_env[age <= 1])
		new_rows <- nrow(u5_envelope)

		whiv_env <- whiv_env[!age %in% c(0,1)]
		whiv_env[, age := as.character(age)]
		whiv_env <- rbindlist(list(whiv_env, u5_envelope), use.names=T)

	## Make some assertions
		assert_values(whiv_env[year >= 1954], "deaths", "not_na", quiet=T)
		assert_ids(whiv_env, env_ids)

	return(whiv_env)
}
