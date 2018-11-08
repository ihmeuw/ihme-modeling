#' Generating life expectancy (ex) at age x based on person-years lived above the age group (Tx) 
#' and proportion of people alive at the start of age group (lx)
#'
#' Given a data.table with person-years lived above the age group (Tx) and proportion of people alive at the start of the age group (lx), 
#' ex = Tx / lx
#'
#' @param dt data.table with Tx and lx variables.
#' @param assert_na logical for whether to check for NA values in the newly generated ex variable.
#'
#' @return None. Modifies the given data.table in-place.
#' @export
#'
#' @examples
#' 
#' @import data.table
#' @import assertable

gen_ex <- function(dt, assert_na=T) {
	dt[, ex := Tx/lx]
	if(assert_na == T) assertable::assert_values(dt, "ex", "not_na", quiet=T)
}
