################################################################################
# Description: Aggregate subnational data to higher level locations.
# - aggregate gbd subnational locations to higher level subnationals, national and historical locations
# - in specified national locations use aggregated data points
################################################################################

library(data.table)
library(readr)
library(mortdb, lib.loc = "FILEPATH/r-pkg")
library(mortcore, lib.loc = "FILEPATH/r-pkg")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/data_processing/census_processing/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_processing_vid", type = "character",
                    help = "The version number for this run of population data processing, used to read in settings file")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_processing_vid <- "99999"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_processing_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "helper_functions.R"))

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))
subnational_mappings <- fread(paste0(output_dir, "/inputs/subnational_mappings.csv"))


# Aggregate data ----------------------------------------------------------

census_data <- fread(paste0(output_dir, "/outputs/03_split_historical_subnationals.csv"))
census_data[, aggregate := F]

parent_subnational_loc_ids <- location_hierarchy[level == 4, unique(parent_id)]
parent_subnational_ihme_locs <- location_hierarchy[location_id %in% parent_subnational_loc_ids, ihme_loc_id]
subnational_mappings <- subnational_mappings[national_ihme_loc_id %in% parent_subnational_ihme_locs]

subnational_loc_ids <- c(location_hierarchy[grepl(paste0(",", paste0(parent_subnational_loc_ids, collapse = ",|,"), ","), path_to_top_parent), location_id],
                         subnational_mappings[, parent_location_id], subnational_mappings[, child_location_id])
subnational_loc_ids <- subnational_loc_ids[!subnational_loc_ids %in% parent_subnational_loc_ids]
subnational_loc_ids <- unique(subnational_loc_ids)
subnational_data <- census_data[(location_id %in% subnational_loc_ids | location_id %in% parent_subnational_loc_ids) & outlier_type == "not outliered"]
subnational_data[, split := F]

census_data <- census_data[!((location_id %in% subnational_loc_ids | location_id %in% parent_subnational_loc_ids) & outlier_type == "not outliered")]

final_subnational_data <- lapply(parent_subnational_ihme_locs, function(sub_ihme_loc) {

  print(sub_ihme_loc)

  sub_loc_id <- location_hierarchy[ihme_loc_id == sub_ihme_loc, location_id]
  sub_loc_ids <- c(get_subnational_location_ids(sub_ihme_loc, location_hierarchy),
                   subnational_mappings[national_ihme_loc_id == sub_ihme_loc, parent_location_id],
                   subnational_mappings[national_ihme_loc_id == sub_ihme_loc, child_location_id])
  sub_loc_ids <- sub_loc_ids[!sub_loc_ids %in% sub_loc_id]
  sub_loc_ids <- unique(sub_loc_ids)

  # load old subnational unit mappings if they exist
  if (sub_ihme_loc %in% unique(subnational_mappings$national_ihme_loc_id)) {
    old_subnational_unit_mappings <- subnational_mappings[national_ihme_loc_id == sub_ihme_loc]
    old_subnational_unit_mappings <- old_subnational_unit_mappings[, list(location_id = child_location_id,
                                                                          parent_location_id,
                                                                          level)]
    sub_loc_ids <- unique(c(sub_loc_ids, old_subnational_unit_mappings[, parent_location_id],
                            old_subnational_unit_mappings[, location_id]))
  } else {
    old_subnational_unit_mappings <- NULL
  }

  # subset to data for one subnational unit
  data <- subnational_data[location_id %in% c(sub_loc_id, sub_loc_ids)]

  # add on the normal location hierarchy
  gbd_map <- location_hierarchy[location_id %in% sub_loc_ids | location_id == sub_loc_id]
  gbd_map <- gbd_map[, list(location_id, parent_location_id = parent_id, level)]
  old_subnational_unit_mappings <- unique(rbind(gbd_map, old_subnational_unit_mappings))

  census_years <- unique(data$year_id)

  year_specific_results <- lapply(census_years, function(census_year) {
    print(census_year)

    year_data <- data[year_id == census_year]
    year_data <- merge(year_data, unique(old_subnational_unit_mappings[, list(location_id, level)]),
                       by = c("location_id"), all.x = T)

    agg_levels <- sort(unique(location_hierarchy[level > 3, level]), decreasing = T)
    agg_levels <- agg_levels[agg_levels <= max(year_data$level)]
    if (sub_ihme_loc %in% c("CHN", "GBR")) agg_levels <- agg_levels[agg_levels > 4]

    for (agg_level in agg_levels) {
      print(agg_level)

      agg_data <- year_data[level == agg_level & outlier_type == "not outliered"]
      agg_data[, level := NULL]

      # aggregate to common set of most granular age groups
      if (nrow(agg_data) > 0) {
        agg_data <- merge(agg_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
        agg_data[, c("split", "aggregate") := NULL]
        agg_data <- agg_age_data(agg_data, id_vars = census_id_vars)
      }

      # map each data point to its parent locations, if multiple parent locations,
      # create multiple rows for data point with different parent locations
      # can have multiple parent locations when mapping to national location and to historical location for example
      agg_data <- merge(agg_data, old_subnational_unit_mappings, by = c("location_id"), all.x = T, allow.cartesian = T)

      # actually aggregate the data up a level
      agg_data <- agg_data[, list(mean = sum(mean), split = F, aggregate = T),
                           by = c("parent_location_id", setdiff(census_id_vars, "location_id"), "sex_id", "age_group_id")]
      setnames(agg_data, "parent_location_id", "location_id")
      agg_data <- merge(agg_data, unique(old_subnational_unit_mappings[, list(location_id, level)]),
                        by = c("location_id"), all.x = T)

      # if data already exists for the aggregate then mark as duplicated
      agg_data_exists_already <- unique(year_data[location_id %in% unique(agg_data$location_id),
                                                  list(location_id, year_id, aggregate_exists_already = T)])
      agg_data <- merge(agg_data, agg_data_exists_already, by = c("location_id", "year_id"), all.x = T)
      agg_data[(aggregate_exists_already), outlier_type := "duplicated"]
      agg_data[, aggregate_exists_already := NULL]

      # year_data <- rbind(year_data, old_subnational_aggregates, agg_data, use.names = T)
      year_data <- rbind(year_data, agg_data, use.names = T)
    }
    return(year_data)
  })
  year_specific_results <- rbindlist(year_specific_results)
  return(year_specific_results)
})
final_subnational_data <- rbindlist(final_subnational_data, use.names = T)


# 2015 UKR census data point was only conducted in Crimea so not a national census
final_subnational_data <- final_subnational_data[!(location_id %in% get_location_id("UKR", location_hierarchy) & year_id == 2014)]

# use aggregated registry data rather than census data
final_subnational_data[aggregate == F & location_id %in% get_location_id("SWE", location_hierarchy) & year_id >= 1968, outlier_type := "duplicated"]
final_subnational_data[aggregate == T & location_id %in% get_location_id("SWE", location_hierarchy) & year_id >= 1968, outlier_type := "not outliered"]

# use more detailed aggregate subnational data for USA
final_subnational_data[aggregate == F & location_id %in% get_location_id("USA", location_hierarchy) & between(year_id, 1970, 1990), outlier_type := "duplicated"]
final_subnational_data[aggregate == T & location_id %in% get_location_id("USA", location_hierarchy) & between(year_id, 1970, 1990), outlier_type := "not outliered"]

# use more detailed aggregate subnational data for the RUS
final_subnational_data[aggregate == F & location_id %in% get_location_id("RUS", location_hierarchy) & !year_id %in% c(2002, 2010), outlier_type := "duplicated"]
final_subnational_data[aggregate == T & location_id %in% get_location_id("RUS", location_hierarchy) & !year_id %in% c(2002, 2010), outlier_type := "not outliered"]

# use aggregate subnational data for ETH, slightly less age misreporting in young adults in 1984 census
final_subnational_data[aggregate == F & location_id %in% get_location_id("ETH", location_hierarchy), outlier_type := "duplicated"]
final_subnational_data[aggregate == T & location_id %in% get_location_id("ETH", location_hierarchy), outlier_type := "not outliered"]

# use more detailed aggregate subnational data for IDN
final_subnational_data[aggregate == F & location_id %in% get_location_id("IDN", location_hierarchy) & year_id > 1971, outlier_type := "duplicated"]
final_subnational_data[aggregate == T & location_id %in% get_location_id("IDN", location_hierarchy) & year_id > 1971, outlier_type := "not outliered"]

# use aggregate IND data rather than DYB data because we know what subnationals are included in the aggregate data
final_subnational_data[aggregate == F & location_id %in% get_location_id("IND", location_hierarchy) & year_id != 1951, outlier_type := "duplicated"] # 1951 we don't yet have subnational data
final_subnational_data[aggregate == T & location_id %in% get_location_id("IND", location_hierarchy), outlier_type := "not outliered"]

# use aggregate CHN_44533 data rather than DYB data per Haidong (aggregate is 1.145 billion vs 1.130 billion DYB data)
final_subnational_data[aggregate == F & location_id %in% get_location_id("CHN_44533", location_hierarchy) & year_id == 1990, outlier_type := "duplicated"]
final_subnational_data[aggregate == T & location_id %in% get_location_id("CHN_44533", location_hierarchy) & year_id == 1990, outlier_type := "not outliered"]

final_subnational_data[, c("level") := NULL]
census_data <- rbind(census_data, final_subnational_data, use.names = T)
census_data[, data_stage := "aggregated subnationals"]

setcolorder(census_data, c(census_id_vars, "split", "aggregate", "sex_id", "age_group_id", "mean"))
setkeyv(census_data, c(census_id_vars, "split", "aggregate", "sex_id", "age_group_id"))
readr::write_csv(census_data, path = paste0(output_dir, "/outputs/04_aggregate_subnationals.csv"))


# Check whether subnational data sums to national data --------------------

## output check of subnational aggregates compared to other sources
subnational_aggregate_check <- census_data[, list(mean = sum(mean)), by = c(census_id_vars, "split", "aggregate")]
subnational_aggregate_check <- merge(subnational_aggregate_check, location_hierarchy[, list(location_id, ihme_loc_id)], by = "location_id")
subnational_aggregate_check <- subnational_aggregate_check[ihme_loc_id %in% c(parent_subnational_ihme_locs, "CHN_44533", "GBR_4749")]
subnational_aggregate_check[, mean := formatC(mean, format = "d", big.mark = ",")] # format numbers with commas
subnational_aggregate_check <- subnational_aggregate_check[ihme_loc_id != "GBR"]
setcolorder(subnational_aggregate_check, c("ihme_loc_id", census_id_vars, "split", "aggregate", "mean"))
setkeyv(subnational_aggregate_check, c("ihme_loc_id", "year_id", "source_name", "aggregate", "outlier_type"))
readr::write_csv(subnational_aggregate_check, path = paste0(output_dir, "/diagnostics/subnational_aggregates.csv"))
