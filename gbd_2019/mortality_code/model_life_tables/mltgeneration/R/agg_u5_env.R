#' Aggregrate Under-5 Age Groups in the Under-5 Envelope
#'
#' Aggregate the under-5 envelope from granular age groups (e.g. enn/lnn/pnn) to 0-1 and 1-4 age groups
#'
#' @param u5_env data.table with variables sim, ihme_loc_id, year, sex, age, and deaths
#'
#' @return data.table with variables sim, ihme_loc_id, year, sex, age, and deaths
#'         Includes 0-1 and 1-4 age groups.
#' @export
#'
#' @examples
#'
#' @import data.table
#' @import assertable

agg_u5_env <- function(u5_env) {
	id_vars <- c("sim", "ihme_loc_id", "year", "sex", "age")
	u5_env <- copy(u5_env)
	## Collapse from:
	## 		NN age groups to 0-1 age group
	## 		1-2, 2-3, 3-4, and 4-5 to 1-4 age group
	u5_env[age %in% c("2", "3", "4"), age := "1"]
	u5_env <- u5_env[, list(deaths = sum(deaths)), by = id_vars]

	assert_values(u5_env[year >= 1954], "deaths", "not_na", quiet=T)

	return(u5_env)
}
