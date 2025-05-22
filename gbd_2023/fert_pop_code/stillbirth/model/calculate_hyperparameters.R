##############################################################################################################
## Documentation:
##
##   - All source types are assigned a weight of 1
##   - Data split vr v. non-vr
##   - Data density table: ihme_loc_id, data_density, literature, gov_report, survey, census
##   - Going through each location in births data table:
##       - Initially assign data density as 0
##       - For non-vr data:
##           - For each unique nid:
##               - data density = data density + # of non vr sources (since all sources has a weight of 1)
##           - For rows without an nid:
##               - data density = data density + length of rows (unique by data name)
##       - For vr data:
##           - data density = data density + vr weight (1) * # location-years
##   - Locations assigned to one of five data density categories (0-9, 10-19, 20-29, 30-49, 50+)
##   - Zeta (space weights), lambda (time weights), and scale are assigned based on data density category
##   - Values differ depending if you want 5q0 parameters or fertility density parameters
##   - Set amp2x = 1
##
##############################################################################################################

source(paste0(shared_functions_dir, "get_covariate_estimates.R"))

calculate_hyperparameters <- function(data, use_5q0_density_parameters = F, use_fertility_density_parameters = F, gbd_year, release_id, test_lambda = NA) {

  births <- get_covariate_estimates(covariate_id = 60, release_id = release_id)
  loc_map <- mortdb::get_locations(level = "all", gbd_year = gbd_year)
  loc_map <- subset(loc_map, select = c("location_id", "ihme_loc_id"))
  births <- merge(births, loc_map, by = "location_id", all.x = T)
  births <- subset(births, select = c("year_id", "mean_value", "ihme_loc_id"))
  setnames(births, "mean_value", "num_births")
  births[, num_births := num_births * 1000]

  data_vr <- data[data_type == "vr",]
  data_non_vr <- data[data_type != "vr",]

  vr_weight <- 1
  survey_weight <- 1
  census_weight <- 1
  literature_weight <- 1
  gov_report_weight <- 1

  determine_non_vr_source_weight <- function(data_type) {
    if (data_type == "survey") {
      weight <- survey_weight
    }
    if (data_type == "census") {
      weight <- census_weight
    }
    if (data_type == "literature") {
      weight <- literature_weight
    }
    if (data_type == "gov_report") {
      weight <- gov_report_weight
    }
    return(weight)
  }

  data_density_table <- data.frame(ihme_loc_id = unique(births$ihme_loc_id),
                                   data_density = NA,
                                   vr = 0,
                                   survey = 0,
                                   census = 0,
                                   gov_report = 0,
                                   literature = 0)

  ### Fill in data density column ###

  for (loc in unique(births$ihme_loc_id)) {
    cat(paste0(loc, "\n")); flush.console()
    data_density <- 0
    # non_vr data
    if (loc %in% data_non_vr$ihme_loc_id) {
      # nid is present
      for (nid in unique(data_non_vr$nid[!is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc])) {
        data_density <- data_density + determine_non_vr_source_weight(unique(data_non_vr$data_type[data_non_vr$nid == nid & !is.na(data_non_vr$nid)]))
      }
      # nid is NA
      data_density <- data_density + length(unique(data_non_vr$data_type[is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc]))
    }
    # vr data
    if (loc %in% data_vr$ihme_loc_id) {
      for (yr in sort(unique(data_vr$year_id[data_vr$ihme_loc_id == loc]))) {
        if (any(data_vr$data_type[data_vr$ihme_loc_id == loc & data_vr$year_id == yr] == "vr")) {
          data_density <- data_density + vr_weight * length(data_vr$ihme_loc_id[data_vr$ihme_loc_id == loc &
                                                                                  data_vr$year_id == yr &
                                                                                  data_vr$data_type == "vr"])
        }
      }
    }

    data_density_table$data_density[data_density_table$ihme_loc_id == loc] <- data_density
  }

  ### Fill in columns of counts by data type ###

  # vr
  for (loc in unique(births$ihme_loc_id)) {
    cat(paste0(loc, "\n")); flush.console()
    vr <- 0
    # vr data
    if (loc %in% data_vr$ihme_loc_id) {
      for (yr in sort(unique(data_vr$year_id[data_vr$ihme_loc_id == loc]))) {
        if (any(data_vr$data_type[data_vr$ihme_loc_id == loc & data_vr$year_id == yr] == "vr")) {
          vr <- vr + vr_weight * length(data_vr$ihme_loc_id[data_vr$ihme_loc_id == loc &
                                                              data_vr$year_id == yr &
                                                              data_vr$data_type == "vr"])
        }
      }
    }

    data_density_table$vr[data_density_table$ihme_loc_id == loc] <- vr
  }

  # survey
  for (loc in unique(births$ihme_loc_id)) {
    cat(paste0(loc, "\n")); flush.console()
    survey <- 0
    # non_vr data
    if (loc %in% data_non_vr$ihme_loc_id) {
      # nid is present
      for (nid in unique(data_non_vr$nid[!is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc & data_non_vr$data_type == "survey"])) {
        survey <- survey + determine_non_vr_source_weight(unique(data_non_vr$data_type[data_non_vr$nid == nid & !is.na(data_non_vr$nid)]))
      }
      # nid is NA
      survey <- survey + length(unique(data_non_vr$data_type[is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc]))
    }

    data_density_table$survey[data_density_table$ihme_loc_id == loc] <- survey
  }

  # census
  for (loc in unique(births$ihme_loc_id)) {
    cat(paste0(loc, "\n")); flush.console()
    census <- 0
    # non_vr data
    if (loc %in% data_non_vr$ihme_loc_id) {
      # nid is present
      for (nid in unique(data_non_vr$nid[!is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc & data_non_vr$data_type == "census"])) {
        census <- census + determine_non_vr_source_weight(unique(data_non_vr$data_type[data_non_vr$nid == nid & !is.na(data_non_vr$nid)]))
      }
      # nid is NA
      census <- census + length(unique(data_non_vr$data_type[is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc]))
    }

    data_density_table$census[data_density_table$ihme_loc_id == loc] <- census
  }

  # gov report
  for (loc in unique(births$ihme_loc_id)) {
    cat(paste0(loc, "\n")); flush.console()
    gov_report <- 0
    # non_vr data
    if (loc %in% data_non_vr$ihme_loc_id) {
      # nid is present
      for (nid in unique(data_non_vr$nid[!is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc & data_non_vr$data_type == "gov_report"])) {
        gov_report <- gov_report + determine_non_vr_source_weight(unique(data_non_vr$data_type[data_non_vr$nid == nid & !is.na(data_non_vr$nid)]))
      }
      # nid is NA
      gov_report <- gov_report + length(unique(data_non_vr$data_type[is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc]))
    }

    data_density_table$gov_report[data_density_table$ihme_loc_id == loc] <- gov_report
  }

  # literature
  for (loc in unique(births$ihme_loc_id)) {
    cat(paste0(loc, "\n")); flush.console()
    literature <- 0
    # non_vr data
    if (loc %in% data_non_vr$ihme_loc_id) {
      # nid is present
      for (nid in unique(data_non_vr$nid[!is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc & data_non_vr$data_type == "literature"])) {
        literature <- literature + determine_non_vr_source_weight(unique(data_non_vr$data_type[data_non_vr$nid == nid & !is.na(data_non_vr$nid)]))
      }
      # nid is NA
      literature <- literature + length(unique(data_non_vr$data_type[is.na(data_non_vr$nid) & data_non_vr$ihme_loc_id == loc]))
    }

    data_density_table$literature[data_density_table$ihme_loc_id == loc] <- literature
  }

  ### Add location info ###

  data_density_table <- as.data.table(merge(data_density_table, loc_map, by = "ihme_loc_id", all.x = T))

  data_density_table[data_density == 0, data_density_category := "0"]
  data_density_table[data_density >= 1 & data_density < 5, data_density_category := "1_to_4"]
  data_density_table[data_density >= 5 & data_density < 10, data_density_category := "5_to_9"]
  data_density_table[data_density >= 10 & data_density < 20, data_density_category := "10_to_19"]
  data_density_table[data_density >= 20, data_density_category := "20_plus"]

  if (use_5q0_density_parameters) {

    data_density_table[data_density_category == "0_to_10", zeta := 0.7]
    data_density_table[data_density_category == "10_to_20", zeta := 0.7]
    data_density_table[data_density_category == "20_to_30", zeta := 0.8]
    data_density_table[data_density_category == "30_to_50", zeta := 0.9]
    data_density_table[data_density_category == "50_plus", zeta := 0.99]

    data_density_table[data_density_category == "0_to_10", lambda := 0.7]
    data_density_table[data_density_category == "10_to_20", lambda := 0.5]
    data_density_table[data_density_category == "20_to_30", lambda := 0.4]
    data_density_table[data_density_category == "30_to_50", lambda := 0.3]
    data_density_table[data_density_category == "50_plus", lambda := 0.2]

    data_density_table[data_density_category == "0_to_10", scale := 15]
    data_density_table[data_density_category == "10_to_20", scale := 15]
    data_density_table[data_density_category == "20_to_30", scale := 15]
    data_density_table[data_density_category == "30_to_50", scale := 10]
    data_density_table[data_density_category == "50_plus", scale := 5]

  }

  if (use_fertility_density_parameters) {

    # these correspond to gbd 2017 hyper-parameter selection

    data_density_table[data_density_category == "0", zeta := 0.6]
    data_density_table[data_density_category == "1_to_4", zeta := 0.7]
    data_density_table[data_density_category == "5_to_9", zeta := 0.8]
    data_density_table[data_density_category == "10_to_19", zeta := 0.9]
    data_density_table[data_density_category == "20_plus", zeta := 0.99]

    data_density_table[data_density_category == "0", lambda := 1]
    data_density_table[data_density_category == "1_to_4", lambda := 0.8]
    data_density_table[data_density_category == "5_to_9", lambda := 0.6]
    data_density_table[data_density_category == "10_to_19", lambda := 0.4]
    data_density_table[data_density_category == "20_plus", lambda := 0.2]

    if (!is.na(test_lambda)) data_density_table[, lambda := test_lambda]

    data_density_table[data_density_category == "0", scale := 15]
    data_density_table[data_density_category == "1_to_4", scale := 15]
    data_density_table[data_density_category == "5_to_9", scale := 15]
    data_density_table[data_density_category == "10_to_19", scale := 10]
    data_density_table[data_density_category == "20_plus", scale := 5]

  }

  data_density_table[, best := 1]

  data_density_table[, amp2x := 1]

  readr::write_csv(data_density_table, paste0(estimate_dir, version_estimate, "/FILEPATH/hyperparameters.csv"))

}
