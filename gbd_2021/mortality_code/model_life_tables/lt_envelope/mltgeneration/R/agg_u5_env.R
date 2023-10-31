#' Aggregrate Under-5 Age Groups in the Under-5 Envelope
#'
#' Aggregate the under-5 envelope from granular age groups (1, 2, 3, 4) to gbd age groups (1, 2-4)
#'
#' @param u5_env data.table with variables sim, ihme_loc_id, year, sex, age, and deaths
#' @param gbd_year
#'
#' @return data.table with variables sim, ihme_loc_id, year, sex, age, and deaths
#' @export
#'
#' @examples
#' 
#' @import data.table
#' @import assertable

agg_u5_env <- function(u5_env, gbd_year) {
  
	id_vars <- c("sim", "ihme_loc_id", "year", "sex", "age")
	u5_env <- copy(u5_env)
	
	# split differently based on gbd year.
	# TODO: modify to be based on a "age_breaks" argument not "gbd_year" to be more flexible.
	if(gbd_year >= 2020){
	  u5_env[age %in% c("3", "4"), age := "2"]  # 1 and 2-4
	} else {
	  u5_env[age %in% c("2", "3", "4"), age := "1"]  # 1-4
	}

	u5_env <- u5_env[, list(deaths = sum(deaths)), by = id_vars]

	assert_values(u5_env[year >= 1954], "deaths", "not_na", quiet=T) # Temporary: Values under 1954 may be NA due to aging

	return(u5_env)
}
