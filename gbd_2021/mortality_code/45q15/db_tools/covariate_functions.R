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
  q <- paste("SELECT
             c.covariate_name_short,
             dv.covariate_id,
             mv.model_version_id,
             mv.best_start,
             mv.date_inserted,
             mv.description
             FROM
             covariate.model_version mv
             JOIN
             covariate.data_version dv USING (data_version_id)
             JOIN
             shared.covariate c USING (covariate_id)
             WHERE", where_clause,
             "AND mv.is_best = 1 AND mv.gbd_round_id = ",
             gbd_round_id)

  # Run query
  return(query(q, conn_def = "modeling-mortality-db"))
}

get_covariate_estimate <- function(covariate_name_short = NULL, covariate_id = NULL,
                                   location_id = -1, year_id = -1, sex_id = -1,
                                   age_group_id = -1, gbd_round_id = 5,
                                   model_version_id = NULL) {
  # Get the model version
  if (is.null(model_version_id)) {
    # Get the model version id using the get_best_covariate_version function
    if (!is.null(covariate_id)) {
      cov_data <- get_best_covariate_version(covariate_id = covariate_id,
                                                        gbd_round_id = gbd_round_id)
    } else if (!is.null(covariate_name_short)) {
      cov_data <- get_best_covariate_version(covariate_name_short = covariate_name_short,
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
  q <- paste0("SELECT
              model.model_version_id,
              covariate.covariate_id,
              covariate.covariate_name_short,
              model.location_id,
              location.location_name,
              model.year_id,
              model.age_group_id,
              age_group.age_group_name,
              model.sex_id,
              model.mean_value,
              model.lower_value,
              model.upper_value
              FROM
              covariate.model
              JOIN
              covariate.model_version
              ON model.model_version_id=model_version.model_version_id
              JOIN
              covariate.data_version
              ON model_version.data_version_id=data_version.data_version_id
              JOIN
              shared.covariate
              ON data_version.covariate_id=covariate.covariate_id
              JOIN
              shared.location
              ON model.location_id=location.location_id
              JOIN
              shared.age_group
              ON model.age_group_id=age_group.age_group_id
              WHERE
              covariate.last_updated_action!='delete' AND ",
              where_clause)

  # Check output
  output <- query(q, conn_def = "modeling-mortality-db")
  assertthat::assert_that(length(unique(output$model_version_id)) == 1)

  return(output)
}
