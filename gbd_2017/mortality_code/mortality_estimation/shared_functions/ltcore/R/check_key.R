#' Check that data.tables contain the correct keys for life table calculations
#' 
#' Checks that all keys in a given data.table are set appropriately for LT calculations.
#' Requires that \enumerate{
#'   \item Keys are set in the data.table
#'   \item 'age' is the last variable in the key list
#' }
#'
#' \strong{NOTE:} Does not check that the dataset is unique across the keys -- 
#' that is an implicit assumption that should be checked by the modeler outside of this function. 
#' Uniqueness checked in this function due to runtime considerations.
#'
#' @param dt data.table
#'
#' @return None. Throws an error when keys are not set appropriately.
#' @export
#'
#' @examples
#' 
#' @import data.table

check_key <- function(dt) {
  if(is.null(key(dt))) stop("dt must be keyed by id_vars and age")
	if(tail(key(dt), 1) != "age") stop("numeric variable age must be the last var specified in the key of dt")
}
