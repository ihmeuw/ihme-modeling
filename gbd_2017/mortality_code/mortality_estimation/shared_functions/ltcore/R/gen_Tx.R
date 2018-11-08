#' Generate person-years lived above age x (Tx) based on average person-years lived by people in the age interval
#'
#' Given a data.table with an nLx variable, calculate and generate Tx.
#' Starting with Tx = nLx at the terminal age group, take the cumulative sum of nLx 
#'
#' @param dt data.table with an nLx variable.
#' @param id_vars character vector of variables that uniquely identify all observations.\strong{NOTE:} Variable age must be the last variable specified.
#' @param assert_na logical for whether to check for NA values in the newly generated Tx variable.
#'
#' @return None. Modifies the given data.table in-place.
#' 
#' @export
#'
#' @examples
#' test <- data.table(ihme_loc_id = "AFG", age = c(0, 1, seq(5, 110, 5)), nLx = .2)
#' test[age == 10, nLx := 10]
#' test[age == 110, nLx := 110]
#' gen_Tx(test, id_vars = c("ihme_loc_id", "age"))
#' 
#' @import data.table
#' @import assertable

gen_Tx <- function(dt, id_vars, assert_na=T) {
	if(tail(id_vars,1) != "age") stop("The last item in id_vars must be age")
	id_vars_noage <- id_vars[id_vars != "age"]

	# Set descending order to begin the cumulative sum at the oldest age group
	setorderv(dt, id_vars, order = -1)
	dt[, Tx := cumsum(nLx), by = id_vars_noage]
	if(assert_na == T) assertable::assert_values(dt, "Tx", "not_na", quiet=T)
}

