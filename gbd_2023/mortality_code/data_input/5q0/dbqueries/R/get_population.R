get_process_version_data <- function(process_id, status = "best", gbd_round_id = 5) {
  # Generate the where clause
  if (status == "best") {
    status_id <- 5
  } else {
    status_id <- 3
  }
  where_clause <- c(paste0("process_id = ", process_id))
  where_clause <- c(where_clause, paste0("status_id = ", status_id))
  where_clause <- c(where_clause, paste0("gbd_round_id = ", gbd_round_id))

  # Create the query
  q <- paste("QUERY",
             paste(where_clause, collapse = " AND "), ";")

  # Run query
  return(dbtools::query(q, conn_def = "mortality"))
}

get_process_version_id <- function(process_id, status = "best", gbd_round_id = 5) {
  process_version_data <- dbqueries::get_process_version_data(process_id, status = status,
                                                              gbd_round_id = gbd_round_id)
  return(process_version_data$proc_version_id)
}

get_run_id <- function(process_id, status = "best", gbd_round_id = 5) {
  process_version_data <- dbqueries::get_process_version_data(process_id, status = status,
                                                              gbd_round_id = gbd_round_id)
  return(process_version_data$run_id)
}

get_child_run_id <- function(parent_process_version_id, child_process_id = NULL) {
  #
}

get_population <- function(status = "best", gbd_round_id = 5, location_id = -1, year_id = -1, sex_id = -1, age_group_id = -1, location_set_id = NULL) {
  # Get the population run_id
  process_id <- 17
  if (status == "best") {
    stop("get_population with status = best has not yet been implemented")
  } else if (status == "recent") {
    pop_run_id <- dbqueries::get_run_id(process_id, status = "best", gbd_round_id = gbd_round_id)
  } else {
    stop("get_population status must be 'best' or 'recent'")
  }

  # F
  if (!is.null(location_set_id)) {
    location_data = dbqueries::get_locations(location_set_id = location_set_id, gbd_round_id = gbd_round_id)
    location_id = unique(location_data$location_id)
  }

  # Construct the where clause
  where_clause <- c(paste("run_id = ", pop_run_id))
  if (location_id != -1 && !is.null(location_id)) {
    parameter <- paste("location_id IN (", paste(location_id, collapse = ", "), ")")
    where_clause <- c(where_clause, parameter)
  }
  if (year_id != -1 && !is.null(year_id)) {
    parameter <- paste("year_id IN (", paste(year_id, collapse = ", "), ")")
    where_clause <- c(where_clause, parameter)
  }
  if (sex_id != -1 && !is.null(sex_id)) {
    parameter <- paste("sex_id IN (", paste(sex_id, collapse = ", "), ")")
    where_clause <- c(where_clause, parameter)
  }
  if (age_group_id != -1 && !is.null(age_group_id)) {
    parameter <- paste("age_group_id IN (", paste(age_group_id, collapse = ", "), ")")
    where_clause <- c(where_clause, parameter)
  }

  # Get the population estimates
  q <- paste("QUERY")
  pop_data <- dbtools::query(q, conn_def = "DATABASE")
  pop_data <- pop_data[, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'population')]
  return(pop_data)
}
