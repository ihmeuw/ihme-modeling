################################################################################
# Description: Split historical subnationals to gbd locations.
# - collapse data down to specified terminal age group
# - split historical subnationals using gbd population proportions
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
gbd_pop <- fread(paste0(output_dir, "/inputs/gbd_pop_current_round_best.csv"))
gbd_pop[, c("run_id", "lower", "upper") := NULL]

subnational_mappings <- fread(paste0(output_dir, "/inputs/subnational_mappings.csv"))
sub_parent_split_ihme_loc_ids <- unique(subnational_mappings$national_ihme_loc_id)
sub_parent_split_loc_ids <- location_hierarchy[ihme_loc_id %in% sub_parent_split_ihme_loc_ids, location_id]
sub_split_loc_ids <- location_hierarchy[grepl(paste0(",", paste0(sub_parent_split_loc_ids, collapse = ",|,"), ","), path_to_top_parent), location_id]
sub_split_loc_ids <- unique(c(sub_split_loc_ids, subnational_mappings[, parent_location_id],
                              subnational_mappings[, child_location_id]))

# we are unable to map these exactly to ihme locations but can aggregate to the national level so want to keep to the next step
locations_to_not_split <- c("IND_other_U", "IND_other_R")
locations_to_not_split <- subnational_mappings[child_id %in% locations_to_not_split, child_location_id]


# Split data --------------------------------------------------------------

census_data <- fread(paste0(output_dir, "/outputs/02_distribute_unknown_sex_age.csv"))

# collapse all data down to terminal age group
census_data <- merge(census_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
census_data[, collapse_to_age := terminal_age]
census_data <- agg_age_data(census_data, id_vars = census_id_vars, age_grouping_var = "collapse_to_age")

# split out data in subnationals that have historical locations
census_subnational_data <- census_data[location_id %in% sub_split_loc_ids & outlier_type == "not outliered"]
census_data <- census_data[!(location_id %in% sub_split_loc_ids & outlier_type == "not outliered")]

subnational_data <- lapply(sub_parent_split_ihme_loc_ids, function(subnational_ihme_loc) {

  print(subnational_ihme_loc)

  # prep mapping for this subnational
  old_subnational_unit_mappings <- subnational_mappings[national_ihme_loc_id == subnational_ihme_loc]
  old_subnational_unit_mappings <- old_subnational_unit_mappings[, list(location_id = child_location_id,
                                                                        parent_location_id)]

  # determine hierarchy loc_ids and historical loc_ids
  subnational_loc_ids <- get_subnational_location_ids(subnational_ihme_loc, location_hierarchy)
  subnational_loc_ids <- unique(c(subnational_loc_ids, old_subnational_unit_mappings[, parent_location_id],
                                  old_subnational_unit_mappings[, location_id]))

  # separate out data that needs to be location split
  subnational_data <- census_subnational_data[location_id %in% subnational_loc_ids]
  data_to_split <- subnational_data[!location_id %in% location_hierarchy[, location_id] & !location_id %in% locations_to_not_split]
  subnational_data <- subnational_data[location_id %in% location_hierarchy[, location_id] | location_id %in% locations_to_not_split]

  # merge on subnational mapping
  setnames(data_to_split, "location_id", "parent_location_id")
  data_to_split <- merge(data_to_split, old_subnational_unit_mappings, by = c("parent_location_id"), all.x = T, allow.cartesian = T)
  if (nrow(data_to_split[is.na(location_id)]) > 0) stop("location hierarchy messed up")

  parent_location_years <- unique(data_to_split[, list(parent_location_id, year_id)])
  gbd_pop_subnational <- gbd_pop[location_id %in% subnational_loc_ids]

  if (nrow(parent_location_years) > 0) {
    # need to run separateley by parent_location-year because they could have different age groups
    split_data <- lapply(1:nrow(parent_location_years), function(i) {
      parent_loc_id <- parent_location_years[i, parent_location_id]
      year <- parent_location_years[i, year_id]

      data <- data_to_split[parent_location_id == parent_loc_id & year_id == year]

      # need to determine unique set of age groups in the combined locations,
      target_age_groups <- data.table(age_group_id = unique(data$age_group_id))
      target_age_groups <- merge(target_age_groups, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
      target_age_groups <- target_age_groups[, list(age_group_years_start = sort(unique(age_group_years_start)))]
      target_age_groups[, age_group_years_end := shift(age_group_years_start, type = "lead")]
      target_age_groups[age_group_years_start == max(age_group_years_start), age_group_years_end := age_group_table_terminal_age]
      target_age_groups <- merge(target_age_groups, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = c("age_group_years_start", "age_group_years_end"), all.x = T)

      # collapse the gbd populations into the census age groups and calculate proportions by location in each age-sex group
      age_groups_i <- unique(target_age_groups$age_group_id)
      gbd_pop_i <- gbd_pop_subnational[location_id %in% unique(data$location_id) & year_id == year]
      gbd_pop_i <- mortcore::agg_results(gbd_pop_i, id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                         value_vars = "mean", age_aggs = age_groups_i, agg_hierarchy = F)
      gbd_pop_i <- gbd_pop_i[age_group_id %in% age_groups_i]
      gbd_pop_i <- gbd_pop_i[, list(location_id, prop = mean / sum(mean)), by = c("year_id", "sex_id", "age_group_id")]

      # merge on gbd proportions and split data
      data <- merge(data, gbd_pop_i, by = c("location_id", "year_id", "sex_id", "age_group_id"), all.x = T)
      data <- data[, list(mean = mean * prop), by = c(census_id_vars, "sex_id", "age_group_id")]
      data[, split := T]
      setcolorder(data, c(census_id_vars, "split", "sex_id", "age_group_id", "mean"))
      return(data)
    })
    split_data <- rbindlist(split_data)
  } else {
    split_data <- NULL
  }

  # add back onto the dataset containing the original data that didn't need to be split
  subnational_data[, split := F]
  subnational_data <- rbind(split_data, subnational_data, use.names = T)
  return(subnational_data)
})
subnational_data <- rbindlist(subnational_data, use.names = T)

census_data[, split := F]
census_data <- rbind(census_data, subnational_data, use.names = T)

census_data[, data_stage := "split historical subnationals"]
setcolorder(census_data, c(census_id_vars, "split", "sex_id", "age_group_id", "mean"))
setkeyv(census_data, c(census_id_vars, "split", "sex_id", "age_group_id"))
readr::write_csv(census_data, path = paste0(output_dir, "/outputs/03_split_historical_subnationals.csv"))
