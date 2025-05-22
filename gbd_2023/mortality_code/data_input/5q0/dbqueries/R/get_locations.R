get_locations <- function(location_set_id = NULL, location_set_verison_id = NULL, gbd_round_id = 5) {
  # Where clause
  if (!is.null(location_set_verison_id)) {
    where_clause <- paste0("QUERY")
  } else if (!is.null(location_set_id)) {
    where_clause <- paste0("QUERY")
  } else {
    stop("Must provide covariate_id or covariate_name_short")
  }
  
  # Create the query
  q <- paste("QUERY", where_clause)
  
  # Run query
  return(dbtools::query(q, conn_def = "DATABASE"))
}