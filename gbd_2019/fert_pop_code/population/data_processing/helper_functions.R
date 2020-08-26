
get_location_id <- function(ihme_loc, location_hierarchy) {
  loc_id <- location_hierarchy[ihme_loc_id %in% ihme_loc, location_id]
  return(loc_id)
}
get_subnational_location_ids <- function(ihme_loc, location_hierarchy) {
  loc_id <- get_location_id(ihme_loc, location_hierarchy)
  subnational_location_ids <- location_hierarchy[grepl(paste0(",", loc_id, ","), path_to_top_parent), location_id]
  return(subnational_location_ids)
}

agg_age_data <- function(data, id_vars, age_grouping_var = NULL) {
  library(mortdb, lib.loc = "FILEPATH/r-pkg")
  library(mortcore, lib.loc = "FILEPATH/r-pkg")

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
    # must take intersection of age groups and each censuses age groups in order to avoid
    # rounding down age groups (like from 7 to 5) to include ages they shouldn't
    target_age_start <- intersect(data_age_grouping, target_age_start)
    target_age_groups <- data.table(age_group_years_start = sort(target_age_start))
    target_age_groups <- mortcore::age_start_to_age_group_id(target_age_groups)

    # actually aggregate to the correct age groups, no id_assertion since doesn't make sense with id_vars
    same_age_grouping_data <- mortcore::agg_results(same_age_grouping_data, id_vars = c(id_vars, "sex_id", "age_group_id"),
                                                    value_vars = "mean", age_aggs = target_age_groups$age_group_id,
                                                    agg_hierarchy = F, id_assertion = F)
    same_age_grouping_data <- same_age_grouping_data[age_group_id %in% target_age_groups$age_group_id]

    # check that all target age groups exist for each of the sources
    check_target_ages <- same_age_grouping_data[, list(target_age_groups_present = all.equal(sort(unique(age_group_id)), sort(target_age_groups$age_group_id))), by = c(id_vars, "sex_id")]
    if (!all(check_target_ages$target_age_groups_present)) {
      print(check_target_ages[!(target_age_groups_present)])
      stop("Not all the intended target age groups are present in the dataset")
    }

    return(same_age_grouping_data)
  })
  agg_data <- rbindlist(agg_data)
  return(agg_data)
}
