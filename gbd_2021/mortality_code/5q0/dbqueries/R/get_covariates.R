get_best_covariate_version <- function(covariate_id = NULL, covariate_name_short = NULL, 
                                       gbd_round_id = 5) {
  # Where clause
  if (!is.null(covariate_id)) {
    where_clause <- paste0("c.covariate_id = ", covariate_id)
  } else if (!is.null(covariate_name_short)) {
    where_clause <- paste0("c.covariate_name_short = '", covariate_name_short, "'")
  } else {
    stop("Must provide covariate_id or covariate_name_short")
  }
  
  
  # Create the query
  q <- paste("QUERY", where_clause, 
             "AND mv.is_best = 1 AND mv.gbd_round_id = ",
             gbd_round_id)
  
  # Run query
  return(dbtools::query(q, conn_def = "mortality"))
}

get_covariate_estimate <- function(covariate_name_short = NULL, covariate_id = NULL,
                                   location_id = -1, year_id = -1, sex_id = -1,
                                   age_group_id = -1, gbd_round_id = 5,
                                   model_version_id = NULL) {
  # Get the model version
  if (is.null(model_version_id)) {
    # Get the model version id using the get_best_covariate_version function
    if (!is.null(covariate_id)) {
      cov_data <- dbqueries::get_best_covariate_version(covariate_id = covariate_id, 
                                                        gbd_round_id = gbd_round_id)
    } else if (!is.null(covariate_name_short)) {
      cov_data <- dbqueries::get_best_covariate_version(covariate_name_short = covariate_name_short, 
                                                        gbd_round_id = gbd_round_id)
    } else {
      stop("Must provide covariate_id, covariate_name_short, or model_version_id")
    }
    model_version <- cov_data$model_version_id
  } else {
    model_version <- model_version_id
  }
  where_clause <- paste0("model_version.model_version_id = ", model_version)
  
  # Craft the query
  q <- paste0("QUERY",
              where_clause)
  
  # Check output
  output <- dbtools::query(q, conn_def = "mortality")
  assertthat::assert_that(length(unique(output$model_version_id)) == 1)

  return(output)
}
