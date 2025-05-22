make_new_aggregates <- function(key, new_location_id, child_location_ids, data, population_data) {
  if (key == 'maternal_education') {
    version_col <- 'maternal_edu_model_version_id'
    value_col <- 'maternal_edu'
    sex_id <- 2
    age_group_id <- 24
  } else if (key == 'ldi') {
    version_col <- 'ldi_model_version_id'
    value_col <- 'ldi'
    sex_id <- 3
    age_group_id <- 22
  } else if (key == 'hiv') {
    version_col <- 'hiv_model_version_id'
    value_col <- 'hiv'
    sex_id <- 3
    age_group_id <- 1
  }
  # Get the version number
  setnames(data, old = version_col, new = "version_id")
  version_id <- data$version_id[1]

  # Rename value column
  setnames(data, old = value_col, new = "value")

  # Generate sex and age columns
  data$sex_id <- sex_id
  data$age_group_id <- age_group_id

  # Merge covariate data and population
  new_loc_data <- merge(data, population_data, by = c('location_id', 'year_id', 'sex_id', 'age_group_id'))

  new_loc_data <- new_loc_data[new_loc_data$location_id %in% child_location_ids, ]

  # Convert from rate to number space
  new_loc_data[, value := value * pop]

  # Sum Telangana and Andhra Pradesh covariate numbers and population
  new_loc_data$location_id <- new_location_id
  new_loc_data <- new_loc_data[, .(value = sum(value), pop = sum(pop)), by = c('location_id', 'year_id', 'sex_id', 'age_group_id')]

  # Convert back to rate space
  new_loc_data[, value := value / pop]

  # Set the version number for the new data
  new_loc_data$version_id <- version_id

  # Combine
  data <- rbind(data, new_loc_data, use.names=T, fill = T)

  # Rename value and version columns back
  setnames(data, old = "value", new = value_col)
  setnames(data, old = "version_id", new = version_col)

  # Return data
  data <- data[, c('location_id', 'year_id', version_col, value_col), with = FALSE]
  return(data)
}
