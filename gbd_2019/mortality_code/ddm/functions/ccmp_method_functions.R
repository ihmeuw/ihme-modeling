library(mortcore, lib=FILEPATH)


scaled_logit <- function(x, upper_bound, lower_bound) {
  return (boot::logit((x - lower_bound) / (upper_bound - lower_bound)))
}

inv_scaled_logit <- function(x, upper_bound, lower_bound) {
  return ((boot::inv.logit(x) * (upper_bound - lower_bound)) + lower_bound)
}

# average adjacent years so that we have rates between mid-years rather than over an entire year
average_adjacent_year_rates <- function(data, id_vars) {
  data[, next_year_value := shift(mean, type = "lead"), by = setdiff(id_vars, "year_id")]
  data <- data[!is.na(next_year_value)] # drop the last year of data since we don't have the next year
  data <- data[, list(mean = ((mean + next_year_value) / 2)), by = id_vars]
  return(data)
}

format_as_matrix <- function(data, value_var, ages, prep_type="") {
  data <- data.table::dcast(data, age_group_years_start ~ year_id, value.var=value_var) # make year variable wide
  data <- data[, names(data)[-1], with=F] # get rid of the age_group_years_start in the first column
  data <- as.matrix(data)
  rownames(data) <- ages
  return(data)
}

format_as_matrix_by_sex <- function(data, value_var, ages, prep_type="") {

  data <- lapply(1:2, function(s) {
    format_as_matrix(data[sex_id == s], value_var, ages, prep_type)
  })
  names(data) <- c("male", "female")
  return(data)
}

format_value_matrix <- function(data, use_age_int, use_terminal_age, id_vars, year_start, baseline = F) {

  # determine how many single or five year age groups are included in each row
  data[, age_groups := n / use_age_int]
  data[age_group_years_end == age_group_table_terminal_age, age_groups := ((use_terminal_age - age_group_years_start) / use_age_int) + 1]   # terminal age group need to add additional age group

  # use the location or region specific standard deviation
  data[, sd := census_default_sd]

  # calculate scalar based on the beginning age of each data point
  data[, age_middle_floor := floor(age_group_years_start + (n / 2))]
  data[age_group_years_end == age_group_table_terminal_age, age_middle_floor := (use_terminal_age - age_group_years_start) / 2]
  data <- merge(data, sd_age_scalar, by = c("location_id", "year_id", "sex_id", "age_middle_floor"), all.x = T)

  # calculate scalar for the number of age groups included in a data point so that larger age groups get more weight
  data[, sd_scalar_age_groups := (1 / age_groups)]

  # get ready to apply all the sd scalars
  data[, sd_scaled := sd]

  # inflate the baseline standard deviation
  data[, inflate_baseline_sd_scalar := inflate_baseline_sd_scalar]
  data[year_id == year_start, sd_scaled := sd_scaled * inflate_baseline_sd_scalar]

  # apply scalar
  data[, sd_scaled := sd_scaled * sd_scalar_age_groups]
  data[, sd_scaled := sd_scaled * sd_age_scalar]

  setkeyv(data, c("location_id", "year_id", "sex_id", "age_group_years_start"))
  matrices <- lapply(1:2, function(s) {
    sex_specific_data <- data[sex_id == s, list(mean, sd_scaled, year_id, age_group_years_start, sd, sd_scalar_age_groups, sd_age_scalar)]
    sex_specific_data <- as.matrix(sex_specific_data)
    return(sex_specific_data)
  })
  names(matrices) <- c("male", "female")
  return(matrices)
}

format_row_matrix <- function(data, use_age_int, use_terminal_age, id_vars, year_start) {
  # determine columns of full TMB projection matrix to pull from
  data[, year_column := year_id - year_start]

  # determine rows of full TMB projection matrix that should be included in the aggregate census age groups
  data[, age_start_row := age_group_years_start / use_age_int] # inclusive
  data[, age_end_row_exclusive := age_start_row + (n / use_age_int)] # exclusive
  data[age_group_years_end == age_group_table_terminal_age, age_end_row_exclusive := (use_terminal_age / use_age_int) + 1] # terminal age group

  setkeyv(data, c("location_id", "year_id", "sex_id", "age_group_years_start"))
  matrices <- lapply(1:2, function(s) {
    sex_specific_data <- data[sex_id == s, list(year_column, age_start_row, age_end_row_exclusive)]
    sex_specific_data <- as.matrix(sex_specific_data)
    return(sex_specific_data)
  })
  names(matrices) <- c("male", "female")

  return(matrices)
}

agg_age_data <- function(data, id_vars, value_vars = "mean", age_grouping_var = NULL) {

  # determine sets of age groups used in dataset
  data_age_groups <- data[, list(census_age_group_years_start = paste(sort(unique(age_group_years_start)), collapse = ",")), by = c(id_vars, age_grouping_var)]
  unique_age_groupings <- unique(data_age_groups[, c("census_age_group_years_start", age_grouping_var), with = F])

  # determine common set of most granular age starts present in data
  if (is.null(age_grouping_var)) {
    common_age_groups <- lapply(unique_age_groupings$census_age_group_years_start, function (x) as.numeric(strsplit(x, split = ",")[[1]]))
    common_age_groups <- data.table(age_group_years_start = Reduce(intersect, common_age_groups))
  }

  print(paste0("Using 'agg_results' to aggregate to ", nrow(unique_age_groupings), " different combinations of age groupings"))

  agg_data <- lapply(1:nrow(unique_age_groupings), function(i) {
    print(paste0("Grouping: ", i))

    data_age_grouping_string <- unique_age_groupings[i, census_age_group_years_start]
    data_age_grouping <- as.numeric(strsplit(data_age_grouping_string, split = ",")[[1]])

    # determine target ages to aggregate to
    if (is.null(age_grouping_var)) {
      same_age_grouping_sources <- data_age_groups[census_age_group_years_start == data_age_grouping_string]
      target_age_start <- common_age_groups$age_group_years_start
    } else {
      if (age_grouping_var == "aggregate_ages") {
        target_age_start <- unique_age_groupings[i, aggregate_ages]
        same_age_grouping_sources <- data_age_groups[census_age_group_years_start == data_age_grouping_string & aggregate_ages == target_age_start]
        target_age_start <- eval(parse(text = target_age_start))

      } else if (age_grouping_var == "collapse_to_age") {
        collapse_to_age_val <- unique_age_groupings[i, collapse_to_age]
        same_age_grouping_sources <- data_age_groups[census_age_group_years_start == data_age_grouping_string & collapse_to_age == collapse_to_age_val]
        target_age_start <- data_age_grouping[data_age_grouping <= collapse_to_age_val]

      } else if (age_grouping_var == "max_age_group_width") {
        age_int <- unique_age_groupings[i, max_age_group_width]
        same_age_grouping_sources <- data_age_groups[census_age_group_years_start == data_age_grouping_string & max_age_group_width == age_int]

        # determine the set of age groups to collapse to. To specified age interval and up to the maximum age in the age grouping
        if (age_int == 0) {
          target_age_start <- 0
        } else {
          target_age_start <- seq(0, max(data_age_grouping), age_int)
        }

      } else {
        stop("not set up to aggregate by age this way")
      }
    }

    # subset to the input data for this grouping
    same_age_grouping_sources[, include := T]
    same_age_grouping_data <- copy(data)
    same_age_grouping_data <- merge(same_age_grouping_data, same_age_grouping_sources, by = c(id_vars, age_grouping_var), all = T)
    same_age_grouping_data <- same_age_grouping_data[(include)]
    same_age_grouping_data[, c("include", "census_age_group_years_start", "age_group_years_start", "age_group_years_end", age_grouping_var) := NULL]

    # determine target age groups to aggregate to
    target_age_start <- intersect(data_age_grouping, target_age_start)
    target_age_groups <- data.table(age_group_years_start = sort(target_age_start))
    target_age_groups <- age_start_to_age_group_id(target_age_groups)

    same_age_grouping_data <- agg_results(same_age_grouping_data, id_vars = c(id_vars, "sex_id", "age_group_id"),
                                          value_vars = value_vars, age_aggs = target_age_groups$age_group_id,
                                          agg_hierarchy = F, id_assertion = F)
    same_age_grouping_data <- same_age_grouping_data[age_group_id %in% target_age_groups$age_group_id]
    return(same_age_grouping_data)
  })
  agg_data <- rbindlist(agg_data)
  return(agg_data)
}