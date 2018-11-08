library(data.table)
library(ggplot2)
library(assertable)
library(parallel)
library(readr)
library(GBDpop, lib.loc = "FILEPATH")
library(mortdb, lib.loc = "FILEPATH")

rm(list=ls())

pop_dir <- "FILEPATH"
full_lt_dir <- "FILEPATH"
shared_functions_dir <- "FILEPATH"
subnational_mappings_dir <- paste0(pop_dir, "data_processing/census_processing/subnational_mapping/")

# settings needed for processing
test <- F
processed_data_version <- gsub("-", "_", Sys.Date())
best_5x1_pop_run_id <- NA
best_1x1_pop_run_id <- get_proc_lineage(model_name = "population", model_type = "estimate", run_id = best_5x1_pop_run_id)[, parent_run_id]
best_full_lt_version <- NA
best_pes_dismod_version_id <- NA
id_vars <- c("ihme_loc_id", "year_id", "nid", "underlying_nid", "source", "status")
terminal_age <- 95
baseline_year <- 1950

if (test) {
  user <- Sys.getenv("USER")
  processed_data_version <- paste0(processed_data_version, "_test_", user)
  test_locations <- c("PAK", "IND")
}

output_dir <- paste0(pop_dir, "/data_processing/census_processing/v", processed_data_version, "/")
if (!file.exists(output_dir)) dir.create(output_dir)
if (!file.exists(paste0(output_dir, "/data/"))) dir.create(paste0(output_dir, "/data/"))
if (!file.exists(paste0(output_dir, "/diagnostics/"))) dir.create(paste0(output_dir, "/diagnostics/"))

subnational_locations <- c("BRA", "KEN", "ZAF", "ETH", "CHN", "RUS", "IRN", "UKR", "IND", "IDN",
                           "JPN", "NZL", "NOR", "SWE", "GBR", "USA", "MEX")
subnational_locations_to_split <- c("BRA", "KEN", "CHN", "RUS", "IRN", "UKR", "IND", "IDN")

# we are unable to map these exactly to ihme locations but can aggregate to the national level so want to keep to the next step
locations_to_not_split <- c("IND_other_U", "IND_other_R")

# Read in input files -----------------------------------------------------

# pull location hierarchy
source(paste0(shared_functions_dir, "get_location_metadata.R"))
location_hierarchy = get_location_metadata(location_set_id=21, gbd_round_id=5)
location_hierarchy[ihme_loc_id == "GBR", is_estimate := 0]
model_locations <- location_hierarchy[is_estimate == 1, ihme_loc_id]
national_locations <- location_hierarchy[(level == 3 | ihme_loc_id %in% c("CHN_354", "CHN_361", "CHN_44533", "GBR_433", "GBR_434", "GBR_4636", "GBR_4749")) & !ihme_loc_id %in% c("CHN", "GBR"), ihme_loc_id]

# pull age groups
age_groups <- get_age_map(type = "all")

# load full lifetables for all locations
lt_files <- list.files(paste0(full_lt_dir, best_full_lt_version, "/full_lt/with_shock/"), full.names = T)
lt_files <- lt_files[grepl("summary_full_", lt_files)]
full_lifetables <- rbindlist(mclapply(lt_files, fread, mc.cores = 5))
full_lifetables <- merge(full_lifetables, location_hierarchy[, list(location_id, ihme_loc_id)],
                         by = "location_id", all.x = T)
full_lifetables <- full_lifetables[, list(ihme_loc_id, year_id, sex_id, age, life_table_parameter_name, mean)]
full_lifetables <- data.table::dcast(full_lifetables, formula = ihme_loc_id + year_id + sex_id + age ~ life_table_parameter_name, value.var = "mean")

# load sdi used in pes regression
source(paste0(shared_functions_dir, "get_covariate_estimates.R"))
sdi_data <- get_covariate_estimates(covariate_id = 881)
sdi_data <- merge(sdi_data, location_hierarchy, by = 'location_id')
sdi_data <- sdi_data[, list(ihme_loc_id, year_id, sdi = mean_value)]

# pull in dismod global age pattern of underenumeration
source(paste0(shared_functions_dir, "get_model_results.R"))
dismod_fit <- get_model_results('epi', model_version_id = best_pes_dismod_version_id, location_id = 1)
dismod_fit <- merge(dismod_fit, age_groups, by = "age_group_id", all.x = T)

# load best populations
format_pops <- function(data, value_name = "mean", arg_age_int = 5) {

  # collapse neonatal age groups to under 1 age group
  setnames(data, value_name, "value_mean")
  if (arg_age_int == 1) {
    data[age_group_id %in% 2:4, age_group_id := 28]
  }
  # merge on age values
  data <- merge(data, age_groups, by="age_group_id")
  data[, age_start := as.integer(stringr::str_extract(age_group_name, "^[0-9]+"))]
  data[grepl("<1 year|Under 5", age_group_name), age_start := 0]

  # collapse over the potentially aggregated under 1 age group
  data <- data[, list(pop = sum(value_mean)), by = c("ihme_loc_id", "year_id", "sex_id", "age_start")]

  return(data)
}
gbd_pop_locations_to_pull <- location_hierarchy[level >= 3, location_id]
gbd_pop_1_2017_best <- get_mort_outputs(model_name = "population single year",model_type = "estimate",
                                        location_ids = gbd_pop_locations_to_pull,
                                        sex_id = 1:2, gbd_year = 2017, run_id = best_1x1_pop_run_id)
gbd_pop_1_2017_best <- format_pops(gbd_pop_1_2017_best, value_name = "population", arg_age_int = 1)
setkeyv(gbd_pop_1_2017_best, c("ihme_loc_id", "year_id", "sex_id", "age_start"))


# Census specific settings
census_specific_settings <- fread("FILEPATH", na.strings = "")
write_csv(census_specific_settings, path = paste0(output_dir, "/census_specific_settings.csv"))
setnames(census_specific_settings, "use_age_heaping_smoother", "census_use_age_heaping_smoother")

location_specific_settings <- fread("FILEPATH", na.strings = "")

location_specific_settings[is.na(fill_missing_triangle_above) & grepl("IDN_", ihme_loc_id), fill_missing_triangle_above := 55]
location_specific_settings[is.na(fill_missing_triangle_above) & grepl("KEN_", ihme_loc_id), fill_missing_triangle_above := 55]
location_specific_settings[is.na(fill_missing_triangle_above) & grepl("ETH_", ihme_loc_id), fill_missing_triangle_above := 20]
location_specific_settings[is.na(fill_missing_triangle_above) & grepl("IND_", ihme_loc_id), fill_missing_triangle_above := 60]
location_specific_settings[is.na(fill_missing_triangle_above) & grepl("CHN_", ihme_loc_id) & !ihme_loc_id %in% c("CHN_354", "CHN_361", "CHN_44533"), fill_missing_triangle_above := 60]
location_specific_settings[is.na(fill_missing_triangle_above) & grepl("GBR_", ihme_loc_id) & !ihme_loc_id %in% c("GBR_4636", "GBR_434", "GBR_433", "GBR_4749"), fill_missing_triangle_above := 65]
location_specific_settings[is.na(scale_backprojected_baseline) & grepl("ZAF_", ihme_loc_id), scale_backprojected_baseline := T]
location_specific_settings[is.na(scale_backprojected_baseline) & grepl("IRN_", ihme_loc_id), scale_backprojected_baseline := T]

# this setting is to force a census to use or not use the smoother, will be NAs
location_specific_settings <- merge(location_specific_settings, location_hierarchy[, list(ihme_loc_id, super_region_name)], all.x = T, by = "ihme_loc_id")
location_specific_settings[is.na(use_age_heaping_smoother) &
                             super_region_name %in% c("High-income", "Central Europe, Eastern Europe, and Central Asia"),
                           use_age_heaping_smoother := F]
location_specific_settings[is.na(use_age_heaping_smoother) &
                             grepl(paste(c("CHN"), collapse = "|"), ihme_loc_id),
                           use_age_heaping_smoother := F]

location_specific_settings[is.na(use_backprojected_baseline), use_backprojected_baseline := T]
location_specific_settings[is.na(scale_backprojected_baseline), scale_backprojected_baseline := F]
location_specific_settings[is.na(fill_missing_triangle_above), fill_missing_triangle_above := terminal_age]

write_csv(location_specific_settings, path = paste0(output_dir, "/location_specific_settings.csv"))
location_specific_settings <- location_specific_settings[, list(ihme_loc_id, use_backprojected_baseline,
                                                                scale_backprojected_baseline, fill_missing_triangle_above,
                                                                location_use_age_heaping_smoother = use_age_heaping_smoother)]

location_collapse_five_year_age_groups <- location_hierarchy[level >= 3 & !super_region_name %in% c("High-income", "Central Europe, Eastern Europe, and Central Asia"), ihme_loc_id]

summary_counts <- NULL


# Raw census data ---------------------------------------------------------

census_data_original_raw <- fread(paste0(pop_dir, "data_prep/census_prep/prepped_census_data.csv"))
census_data_original_raw[, c("age_end", "n") := NULL]
census_data_original_raw <- census_data_original_raw[!ihme_loc_id %in% c("GBR", "CHN")] # we don't model level 3 since censuses were not conducted in the same years at level 4

# add on record type column
census_metadata <- fread(paste0(pop_dir, 'data_prep/census_prep/prepped_census_metadata.csv'))
census_metadata <- census_metadata[, c(id_vars, "record_type"), with = F]
census_data_original_raw <- merge(census_data_original_raw, census_metadata, by = id_vars, all.x = T)
id_vars <- c(id_vars, "record_type")

# manual drops that should be moved to the data prep code
census_data_original_raw[ihme_loc_id == "MDV" & year_id %in% c(1958:1961, 1963:1966, 1968:1971), status := "excluded"] # convergence issues
census_data_original_raw[grepl("IDN", ihme_loc_id) & year_id == 2000, status := "excluded"] # undercount causing migration swings and paper: "ESTIMATES OF INDONESIAN POPULATION NUMBERS: FIRST IMPRESSIONS FROM THE 2010 CENSUS"

census_data_original_raw <- census_data_original_raw[grepl("NZL_", ihme_loc_id) & year_id == 1996, status := "excluded"] # causing large swings in migration
census_data_original_raw <- census_data_original_raw[ihme_loc_id == "TKM" & year_id == 1995, status := "excluded"] # causing large swings in migration, thought to be less accurate

census_data_original_raw[ihme_loc_id == "DEU" & record_type == "registry" & year_id >= 2011, status := "excluded"] # outlier registry data post 2011
census_data_original_raw[ihme_loc_id == "DEU" & record_type != "registry" & year_id == 2011, status := "best"] # keep census point in 2011

census_data_original_raw <- census_data_original_raw[grepl("SGP", ihme_loc_id) & record_type == "registry" & year_id >= 2000, status := "check_extraction"] # super noisy for some reason
census_data_original_raw <- census_data_original_raw[grepl("SGP", ihme_loc_id) & record_type != "registry" & year_id >= 2000, status := "best"]

census_data_original_raw <- census_data_original_raw[grepl("IDN_", ihme_loc_id) & year_id == 1971, status := "check_extraction"] # potential issue with boundary changes

census_data_original_raw <- census_data_original_raw[ihme_loc_id == "BRA_4758" & year_id == 1980, status := "check_extraction"] # split location too high for first loop

census_data_original_raw <- census_data_original_raw[ihme_loc_id == "IRN_53442" & year_id == 1966, status := "check_extraction"] #child Khorasan−e−Razavi (IRN_44880) too high, child South Khorasan (IRN_44881) too high
census_data_original_raw <- census_data_original_raw[ihme_loc_id == "IRN_53447" & year_id %in% c(1976, 1996), status := "check_extraction"] # child Qazvin (IRN_44888) too low, child Zanjan (IRN_44894) too high
census_data_original_raw <- census_data_original_raw[ihme_loc_id == "IRN_44890" & year_id %in% c(1966, 1976), status := "check_extraction"] # Semnan (IRN_44890) too high

# preserve the single year age groups up to 85 by collapsing census to 85+
census_data_original_raw[ihme_loc_id == "GBR_434" & year_id == 2011 & age_start > 85, age_start := 85]

#if (test) census_data_original_raw <- census_data_original_raw[ihme_loc_id %in% test_locations]
census_data_original_raw <- census_data_original_raw[!((sex_id == -1 | age_start == -1) & pop == 0)]
census_data_original_raw <- GBDpop::collapse_terminal_age(census_data_original_raw,
                                                          id_vars = c(id_vars, "sex_id"),
                                                          terminal_age = terminal_age)
census_data_original_raw[, data_step := "raw"]
setcolorder(census_data_original_raw, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_original_raw, c(id_vars, "data_step", "sex_id", "age_start"))
write_csv(census_data_original_raw, path = paste0(output_dir, "/data/0_raw.csv"))


# Distribute unknown sex and age proportionally ---------------------------

census_data_original_raw[pop == 0, pop := 0.01] # need to offset zeroes in the data, especially older age groups

census_data_raw <- GBDpop::distribute_unknown_sex(census_data_original_raw,
                                                  id_vars = c(id_vars, "data_step"))
census_data_raw <- GBDpop::distribute_unknown_age(census_data_raw,
                                                  id_vars = c(id_vars, "data_step", "sex_id"))
census_data_raw[, data_step := "distributed_unknown"]
setcolorder(census_data_raw, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_raw, c(id_vars, "data_step", "sex_id", "age_start"))
write_csv(census_data_raw, path = paste0(output_dir, "/data/1_distributed_unknown.csv"))


# Split data --------------------------------------------------------------

census_data_split <- copy(census_data_raw)
census_data_split[, data_step := NULL]

census_subnational_data <- census_data_split[grepl(paste(paste0(subnational_locations_to_split, "_"), collapse = "|"), ihme_loc_id) & status == "best"]
census_data_split <- census_data_split[!(grepl(paste(paste0(subnational_locations_to_split, "_"), collapse = "|"), ihme_loc_id) & status == "best")]

subnational_data <- lapply(subnational_locations_to_split, function(subnational_loc) {

  print(subnational_loc)

  # load old subnational unit mappings if they exist
  subnational_mapping_file <- paste0(subnational_mappings_dir, subnational_loc, ".csv")
  old_subnational_unit_mappings <- fread(subnational_mapping_file)
  old_subnational_unit_mappings <- old_subnational_unit_mappings[, list(ihme_loc_id = child_id,
                                                                        parent_ihme_loc_id = parent_id)]

  gbd_pop_subnational <- gbd_pop_1_2017_best[grepl(subnational_loc, ihme_loc_id)]

  # separate out data that needs to be location split
  subnational_data <- census_subnational_data[grepl(subnational_loc, ihme_loc_id)]
  data_to_split <- subnational_data[!ihme_loc_id %in% location_hierarchy$ihme_loc_id & !ihme_loc_id %in% locations_to_not_split]
  subnational_data <- subnational_data[ihme_loc_id %in% location_hierarchy$ihme_loc_id | ihme_loc_id %in% locations_to_not_split]

  # merge on location hierarchy
  setnames(data_to_split, "ihme_loc_id", "parent_ihme_loc_id")
  data_to_split <- merge(data_to_split, old_subnational_unit_mappings, by = c("parent_ihme_loc_id"), all.x = T, allow.cartesian = T)
  if (nrow(data_to_split[is.na(ihme_loc_id)]) > 0) stop("location hierarchy messed up")

  parent_location_years <- unique(data_to_split[, list(parent_ihme_loc_id, year_id)])

  if (nrow(parent_location_years) > 0) {
    split_data <- lapply(1:nrow(parent_location_years), function(i) {
      parent_ihme_loc <- parent_location_years[i, parent_ihme_loc_id]
      year <- parent_location_years[i, year_id]

      data <- data_to_split[parent_ihme_loc_id == parent_ihme_loc & year_id == year]

      # need to run separateley by parent_location-year because they could have different age groups
      # collapse the gbd populations into the census age groups and calculate proportions by location in each age-sex group
      age_groups <- unique(data$age_start)
      gbd_pop <- gbd_pop_subnational[ihme_loc_id %in% unique(data$ihme_loc_id) & year_id == year]
      gbd_pop <- GBDpop::collapse_age_groups(gbd_pop, age_groups, id_vars = c("ihme_loc_id", "sex_id"))
      gbd_pop <- gbd_pop[, list(ihme_loc_id, prop = pop / sum(pop)), by = c("sex_id", "age_start")]

      # merge on gbd proportions and split data
      data <- merge(data, gbd_pop, by = c("ihme_loc_id", "sex_id", "age_start"), all.x = T)
      data <- data[, list(pop = pop * prop), by = c(id_vars, "sex_id", "age_start")]
      data[, split := T]
      setcolorder(data, c(id_vars, "split", "sex_id", "age_start", "pop"))
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

census_data_split[, split := F]
census_data_split <- rbind(census_data_split, subnational_data, use.names = T)
census_data_split[, age_start := as.integer(age_start)]
id_vars <- c(id_vars, "split")

census_data_split[, data_step := "split"]
setcolorder(census_data_split, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_split, c(id_vars, "data_step", "sex_id", "age_start"))


# Aggregate data ----------------------------------------------------------

census_data_aggregated <- copy(census_data_split)
census_data_aggregated[, data_step := NULL]
census_data_aggregated[, aggregate := F]

subnational_data <- census_data_aggregated[grepl(paste(subnational_locations, collapse = "|"), ihme_loc_id) & status == "best"]
subnational_data[, split := F] # we don't care for this part if the data was initially split

census_data_aggregated <- census_data_aggregated[!(grepl(paste(subnational_locations, collapse = "|"), ihme_loc_id) & status == "best")]


final_subnational_data <- lapply(subnational_locations, function(subnational_loc) {

  print(subnational_loc)

  # load old subnational unit mappings if they exist
  subnational_mapping_file <- paste0(subnational_mappings_dir, subnational_loc, ".csv")
  if (file.exists(subnational_mapping_file)) {
    old_subnational_unit_mappings <- fread(subnational_mapping_file)
    old_subnational_unit_mappings <- old_subnational_unit_mappings[, list(ihme_loc_id = child_id,
                                                                          parent_ihme_loc_id = parent_id,
                                                                          level, parent_old_subnational)]
  } else {
    old_subnational_unit_mappings <- NULL
  }

  # add on the normal location hierarchy
  gbd_map <- location_hierarchy[grepl(subnational_loc, ihme_loc_id),
                                list(ihme_loc_id, parent_id, level, parent_old_subnational = F)]
  gbd_map <- merge(gbd_map, location_hierarchy[, list(parent_id = location_id, parent_ihme_loc_id = ihme_loc_id)])
  gbd_map <- gbd_map[, list(ihme_loc_id, parent_ihme_loc_id, level, parent_old_subnational)]
  old_subnational_unit_mappings <- rbind(gbd_map, old_subnational_unit_mappings)

  # map each data point to its parent locations, if multiple parent locations,
  # create multiple rows for data point with different parent locations
  data <- subnational_data[grepl(subnational_loc, ihme_loc_id)]
  data <- merge(data, old_subnational_unit_mappings, by = c("ihme_loc_id"), all.x = T, allow.cartesian = T)

  agg_levels <- c(6, 5, 4)
  census_years <- unique(data$year_id)

  year_specific_results <- lapply(census_years, function(census_year) {

    year_data <- data[year_id == census_year]

    for (agg_level in agg_levels) {

      agg_data <- year_data[level == agg_level]

      if (nrow(agg_data) > 0) {
        # collapse all censuses across locations at a certain level to a common set of age groups
        common_age_groups <- GBDpop::find_common_age_groups(agg_data, id_vars = c(setdiff(id_vars, "ihme_loc_id"), "parent_ihme_loc_id", "level", "parent_old_subnational", "aggregate"))
        agg_data <- GBDpop::collapse_age_groups(agg_data, common_age_groups, id_vars = c(id_vars, "parent_ihme_loc_id", "level", "parent_old_subnational", "aggregate", "sex_id"))
      }

      # aggregate data to the old subnational units even if we have detailed enough locations
      # if a ihme_loc belongs to multiple old subnational units, will be counted correctly
      old_subnational_aggregates <- agg_data[parent_old_subnational == T,
                                             list(pop = sum(pop), aggregate = T),
                                             by = c("parent_ihme_loc_id", setdiff(id_vars, "ihme_loc_id"), "sex_id", "age_start")]
      setnames(old_subnational_aggregates, "parent_ihme_loc_id", "ihme_loc_id")
      old_subnational_aggregates <- merge(old_subnational_aggregates, old_subnational_unit_mappings, by = c("ihme_loc_id"), all.x = T)

      # actually aggregate the data up a level
      agg_data <- agg_data[parent_old_subnational == F,
                           list(pop = sum(pop), aggregate = T),
                           by = c("parent_ihme_loc_id", setdiff(id_vars, "ihme_loc_id"), "sex_id", "age_start")]
      setnames(agg_data, "parent_ihme_loc_id", "ihme_loc_id")
      agg_data <- merge(agg_data, old_subnational_unit_mappings, by = c("ihme_loc_id"), all.x = T, allow.cartesian = T)

      year_data <- rbind(year_data, old_subnational_aggregates, agg_data, use.names = T)
    }
    return(year_data)
  })
  year_specific_results <- rbindlist(year_specific_results)
  return(year_specific_results)
})
final_subnational_data <- rbindlist(final_subnational_data, use.names = T)
final_subnational_data <- final_subnational_data[!ihme_loc_id %in% c("GBR", "CHN")] # we don't model level 3 since censuses were not conducted in the same years at level 4

# by default mark all aggregates as duplicates
final_subnational_data[aggregate == T, status := "duplicate"]

# make exceptions for places where we only have subnational data prepped and need to use the aggregate at the national level
final_subnational_data[aggregate == T & grepl("KEN", ihme_loc_id) & level == 4, status := "best"]
final_subnational_data[aggregate == T & grepl("IND", ihme_loc_id) & level == 4, status := "best"]
final_subnational_data[aggregate == T & ihme_loc_id == "CHN_44533" & year_id == 1964, status := "best"]
final_subnational_data[aggregate == T & ihme_loc_id == "NOR" & between(year_id, 1986, 2017), status := "best"]

# use aggregated registry data rather than census data
final_subnational_data[aggregate == F & ihme_loc_id == "NOR" & year_id >= 1986, status := "duplicate"]
final_subnational_data[aggregate == T & ihme_loc_id == "NOR" & year_id >= 1986, status := "best"]
final_subnational_data[aggregate == F & ihme_loc_id == "SWE" & year_id >= 1968, status := "duplicate"]
final_subnational_data[aggregate == T & ihme_loc_id == "SWE" & year_id >= 1968, status := "best"]

# use more detailed aggregate subnational data for USA
final_subnational_data[aggregate == F & ihme_loc_id == "USA" & between(year_id, 1970, 1990), status := "duplicate"]
final_subnational_data[aggregate == T & ihme_loc_id == "USA" & between(year_id, 1970, 1990), status := "best"]

# use more detailed aggregate subnational data for the RUS
final_subnational_data[aggregate == F & ihme_loc_id == "RUS" & !year_id %in% c(2002, 2010), status := "duplicate"]
final_subnational_data[aggregate == T & ihme_loc_id == "RUS" & !year_id %in% c(2002, 2010), status := "best"]

# use aggregate subnational data for ETH, slightly less age misreporting in young adults in 1984 census
final_subnational_data[aggregate == F & ihme_loc_id == "ETH", status := "duplicate"]
final_subnational_data[aggregate == T & ihme_loc_id == "ETH", status := "best"]

# use more detailed aggregate subnational data for IDN
final_subnational_data[aggregate == F & ihme_loc_id == "IDN" & year_id > 1971, status := "duplicate"]
final_subnational_data[aggregate == T & ihme_loc_id == "IDN" & year_id > 1971, status := "best"]

# use aggregate IND data rather than DYB data because we know what subnationals are included in the aggregate data
final_subnational_data[aggregate == F & ihme_loc_id == "IND" & year_id != 1951, status := "duplicate"] # 1951 we don't yet have subnational data
final_subnational_data[aggregate == T & ihme_loc_id == "IND", status := "best"]

# use aggregate CHN_44533 data rather than DYB data per Haidong (aggregate is 1.145 billion vs 1.130 billion DYB data)
final_subnational_data[aggregate == F & ihme_loc_id == "CHN_44533" & year_id == 1990, status := "duplicate"]
final_subnational_data[aggregate == T & ihme_loc_id == "CHN_44533" & year_id == 1990, status := "best"]

# get rid of duplicates created for aggregating to different parents
final_subnational_data[, c("parent_ihme_loc_id", "level", "parent_old_subnational") := NULL]
final_subnational_data <- unique(final_subnational_data)

census_data_aggregated <- rbind(census_data_aggregated, final_subnational_data, use.names = T)
id_vars <- c(id_vars, "aggregate")

census_data_aggregated[, data_step := "aggregated"]
setcolorder(census_data_aggregated, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_aggregated, c(id_vars, "data_step", "sex_id", "age_start"))
write_csv(census_data_aggregated, path = paste0(output_dir, "/data/2_aggregated.csv"))


# Correct for age heaping -------------------------------------------------

## standardize age groups to 1, 5, and 10 year age groups
census_data_unheaped <- copy(census_data_aggregated)
census_data_unheaped[, data_step := NULL]

# determine the maximum age group width in each census
census_data_unheaped <- GBDpop::calculate_full_age_groups(census_data_unheaped, id_vars = c(id_vars, "sex_id"))
census_data_unheaped[, max_age_group_width := max(n), by = c(id_vars)]

# separate out into standard age group widths 1, 5, 10
single_year_age_group_censuses <- census_data_unheaped[max_age_group_width == 1]
five_year_age_group_censuses <- census_data_unheaped[max_age_group_width == 5]
ten_year_age_group_censuses <- census_data_unheaped[max_age_group_width == 10]
total_pop_censuses <- census_data_unheaped[!max_age_group_width %in% c(1, 5, 10)] # these are the censuses we eventually want more detailed age groups for
summary_counts <- rbind(summary_counts, data.table(variable = "collapsed_to_total_pop",
                                                   value = nrow(unique(total_pop_censuses[ihme_loc_id %in% national_locations & max_age_group_width != 0,
                                                                                          list(ihme_loc_id = substr(ihme_loc_id, 1, 3), year_id)]))))

# collapse into these standard 1, 5, 10 year age groups
single_year_age_group_censuses[, c("n", "age_end", "max_age_group_width") := NULL]
five_year_age_group_censuses <- five_year_age_group_censuses[, GBDpop::collapse_age_groups(.SD, seq(0, terminal_age, 5), id_vars = c("sex_id")),
                                                             by = c(id_vars)]
ten_year_age_group_censuses <- ten_year_age_group_censuses[, GBDpop::collapse_age_groups(.SD, seq(0, terminal_age, 10), id_vars = c("sex_id")),
                                                           by = c(id_vars)]
# if collapsing leads to too few age groups, just collapse to total pop
ten_year_age_group_censuses[, max_age := max(age_start), by = id_vars]
collapse_to_total_actually <- ten_year_age_group_censuses[max_age < 40] # should improve code above to reduce censuses we reduce to total pop
ten_year_age_group_censuses <- ten_year_age_group_censuses[max_age >= 40]
collapse_to_total_actually[, max_age := NULL]; ten_year_age_group_censuses[, max_age := NULL];
total_pop_censuses[, c("n", "age_end", "max_age_group_width") := NULL]
total_pop_censuses <- rbind(total_pop_censuses, collapse_to_total_actually, use.names = T)
total_pop_censuses <- total_pop_censuses[, GBDpop::collapse_age_groups(.SD, 0, id_vars = c("sex_id")),
                                         by = c(id_vars)]

# calculate age and sex ratios for all censuses
single_year_age_group_ratios <- GBDpop::calculate_age_sex_ratios(single_year_age_group_censuses, id_vars)
five_year_age_group_ratios <- GBDpop::calculate_age_sex_ratios(five_year_age_group_censuses, id_vars)
ten_year_age_group_ratios <- GBDpop::calculate_age_sex_ratios(ten_year_age_group_censuses, id_vars)
total_pop_year_age_group_ratios <- GBDpop::calculate_age_sex_ratios(total_pop_censuses, id_vars)
age_sex_ratios <- rbind(single_year_age_group_ratios, five_year_age_group_ratios,
                        ten_year_age_group_ratios, total_pop_year_age_group_ratios, use.names = T)
write_csv(age_sex_ratios, path = paste0(output_dir, "/diagnostics/age_sex_ratios_reported.csv"))

# calculate age sex accuracy index
single_year_age_group_accuracy_indexes <- GBDpop::calculate_age_sex_accuracy_index(single_year_age_group_ratios, id_vars, age_start_min = 10, age_start_max = 69)
five_year_age_group_accuracy_indexes <- GBDpop::calculate_age_sex_accuracy_index(five_year_age_group_ratios, id_vars, age_start_min = 10, age_start_max = 69)
ten_year_age_group_accuracy_indexes <- GBDpop::calculate_age_sex_accuracy_index(ten_year_age_group_ratios, id_vars, age_start_min = 10, age_start_max = 69)
age_sex_index <- rbind(single_year_age_group_accuracy_indexes, five_year_age_group_accuracy_indexes,
                       ten_year_age_group_accuracy_indexes, use.names = T)
write_csv(age_sex_index, path = paste0(output_dir, "/diagnostics/age_sex_index_reported.csv"))
age_sex_index[, c("SRS", "ARSM", "ARSF") := NULL]

# apply Feeney correction to single year age group censuses
single_year_age_group_censuses <- merge(single_year_age_group_censuses, age_sex_index, by = id_vars, all.x = T)
single_year_age_group_censuses <- merge(single_year_age_group_censuses, location_specific_settings[, list(ihme_loc_id, location_use_age_heaping_smoother)], by = c("ihme_loc_id"), all.x = T)
single_year_age_group_censuses <- merge(single_year_age_group_censuses, census_specific_settings[, list(ihme_loc_id, year_id, census_use_age_heaping_smoother)], by = c("ihme_loc_id", "year_id"), all.x = T)
single_year_age_group_censuses[!is.na(census_use_age_heaping_smoother), location_use_age_heaping_smoother := census_use_age_heaping_smoother]
single_year_age_group_censuses_none <- single_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) &  age_sex_accuracy_index <= 20) | !location_use_age_heaping_smoother]
single_year_age_group_censuses_none[, c("age_sex_accuracy_index", "location_use_age_heaping_smoother", "census_use_age_heaping_smoother") := NULL]
single_year_age_group_censuses_none[, smoother := "none"]
single_year_age_group_censuses_feeney <- GBDpop::feeney_ageheaping_correction(single_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) &  age_sex_accuracy_index > 20) | location_use_age_heaping_smoother],
                                                                              id_vars = c(id_vars, "sex_id"))
single_year_age_group_censuses_feeney[, smoother := "feeney"]
single_year_age_group_censuses_smoothed <- rbind(single_year_age_group_censuses_none, single_year_age_group_censuses_feeney, use.names = T)
rm(single_year_age_group_censuses, single_year_age_group_censuses_none, single_year_age_group_censuses_feeney)

# apply Arriaga corrections to five year age group censuses
five_year_age_group_censuses <- merge(five_year_age_group_censuses, age_sex_index, by = id_vars, all.x = T)
five_year_age_group_censuses <- merge(five_year_age_group_censuses, location_specific_settings[, list(ihme_loc_id, location_use_age_heaping_smoother)], by = c("ihme_loc_id"), all.x = T)
five_year_age_group_censuses <- merge(five_year_age_group_censuses, census_specific_settings[, list(ihme_loc_id, year_id, census_use_age_heaping_smoother)], by = c("ihme_loc_id", "year_id"), all.x = T)
five_year_age_group_censuses[!is.na(census_use_age_heaping_smoother), location_use_age_heaping_smoother := census_use_age_heaping_smoother]
five_year_age_group_censuses_none <- five_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index < 20) | !location_use_age_heaping_smoother]
five_year_age_group_censuses_none[, c("age_sex_accuracy_index", "location_use_age_heaping_smoother", "census_use_age_heaping_smoother") := NULL]
five_year_age_group_censuses_none[, smoother := "none"]
five_year_age_group_censuses_arriaga <- GBDpop::smooth_age_distribution(five_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & between(age_sex_accuracy_index, 20, 40)) | location_use_age_heaping_smoother],
                                                                        id_vars = c(id_vars, "sex_id"), method = "arriaga")
five_year_age_group_censuses_arriaga[, smoother := "arriaga"]
five_year_age_group_censuses_arriaga_strong <- GBDpop::smooth_age_distribution(five_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index > 40)],
                                                                               id_vars = c(id_vars, "sex_id"), method = "arriaga_strong")
five_year_age_group_censuses_arriaga_strong[, smoother := "arriaga_strong"]

five_year_age_group_censuses_smoothed <- rbind(five_year_age_group_censuses_none,
                                               five_year_age_group_censuses_arriaga,
                                               five_year_age_group_censuses_arriaga_strong,
                                               use.names = T)
rm(five_year_age_group_censuses, five_year_age_group_censuses_none, five_year_age_group_censuses_arriaga, five_year_age_group_censuses_arriaga_strong)

ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, age_sex_index, by = id_vars, all.x = T)
ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, location_specific_settings[, list(ihme_loc_id, location_use_age_heaping_smoother)], by = c("ihme_loc_id"), all.x = T)
ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, census_specific_settings[, list(ihme_loc_id, year_id, census_use_age_heaping_smoother)], by = c("ihme_loc_id", "year_id"), all.x = T)
ten_year_age_group_censuses[!is.na(census_use_age_heaping_smoother), location_use_age_heaping_smoother := census_use_age_heaping_smoother]
ten_year_age_group_censuses_none <- ten_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index <= 20) | !location_use_age_heaping_smoother]
ten_year_age_group_censuses_none[, c("age_sex_accuracy_index", "location_use_age_heaping_smoother", "census_use_age_heaping_smoother") := NULL]
ten_year_age_group_censuses_none[, smoother := "none"]
ten_year_age_group_censuses_arriaga_strong <- GBDpop::smooth_age_distribution(ten_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index > 20) | location_use_age_heaping_smoother],
                                                                              id_vars = c(id_vars, "sex_id"), method = "arriaga_strong", split_10_year = F)
ten_year_age_group_censuses_arriaga_strong[, smoother := "arriaga_strong"]
ten_year_age_group_censuses_smoothed <- rbind(ten_year_age_group_censuses_none, ten_year_age_group_censuses_arriaga_strong, use.names = T)
rm(ten_year_age_group_censuses, ten_year_age_group_censuses_none, ten_year_age_group_censuses_arriaga_strong)

# calculate age and sex ratios for all censuses
single_year_age_group_ratios_smoothed <- GBDpop::calculate_age_sex_ratios(single_year_age_group_censuses_smoothed, id_vars)
five_year_age_group_ratios_smoothed <- GBDpop::calculate_age_sex_ratios(five_year_age_group_censuses_smoothed, id_vars)
ten_year_age_group_ratios_smoothed <- GBDpop::calculate_age_sex_ratios(ten_year_age_group_censuses_smoothed, id_vars)
total_pop_year_age_group_ratios_smoothed <- GBDpop::calculate_age_sex_ratios(total_pop_censuses, id_vars)
age_sex_ratios_smoothed <- rbind(single_year_age_group_ratios_smoothed, five_year_age_group_ratios_smoothed,
                                 ten_year_age_group_ratios_smoothed, total_pop_year_age_group_ratios_smoothed, use.names = T)
write_csv(age_sex_ratios_smoothed, path = paste0(output_dir, "/diagnostics/age_sex_ratios_smoothed.csv"))

# calculate age sex accuracy index
single_year_age_group_accuracy_indexes_smoothed <- GBDpop::calculate_age_sex_accuracy_index(single_year_age_group_ratios_smoothed, id_vars, age_start_min = 10, age_start_max = 69)
five_year_age_group_accuracy_indexes_smoothed <- GBDpop::calculate_age_sex_accuracy_index(five_year_age_group_ratios_smoothed, id_vars, age_start_min = 10, age_start_max = 69)
ten_year_age_group_accuracy_indexes_smoothed <- GBDpop::calculate_age_sex_accuracy_index(ten_year_age_group_ratios_smoothed, id_vars, age_start_min = 10, age_start_max = 69)
age_sex_index_smoothed <- rbind(single_year_age_group_accuracy_indexes_smoothed, five_year_age_group_accuracy_indexes_smoothed,
                                ten_year_age_group_accuracy_indexes_smoothed, use.names = T)
write_csv(age_sex_index_smoothed, path = paste0(output_dir, "/diagnostics/age_sex_index_smoothed.csv"))

total_pop_censuses[, smoother := "none"]
census_data_unheaped <- rbind(single_year_age_group_censuses_smoothed, five_year_age_group_censuses_smoothed,
                              ten_year_age_group_censuses_smoothed, total_pop_censuses, use.names = T)
census_data_unheaped[, age_start := as.integer(age_start)]
census_data_unheaped[, data_step := "unheaped"]
setcolorder(census_data_unheaped, c(id_vars, "smoother", "data_step", "sex_id", "age_start", "pop"))
write_csv(census_data_unheaped, path = paste0(output_dir, "/data/3_unheaped.csv"))

id_vars <- c(id_vars, "smoother")

## output check of subnational aggregates compared to other sources
subnational_aggregate_check <- census_data_unheaped[, list(pop = sum(pop)), by = id_vars]
subnational_aggregate_check <- subnational_aggregate_check[ihme_loc_id %in% c(subnational_locations, "CHN_44533", "GBR_4749")]
subnational_aggregate_check <- subnational_aggregate_check[order(ihme_loc_id, year_id, source, aggregate, status)]
write_csv(subnational_aggregate_check, path = paste0(output_dir, "/diagnostics/subnational_aggregates.csv"))


# Census adjustments ------------------------------------------------------

census_data_ages_adjusted <- copy(census_data_unheaped)
census_data_ages_adjusted[, data_step := NULL]

## collapse to lower terminal age group
census_data_ages_adjusted <- merge(census_data_ages_adjusted,
                                   census_specific_settings[!is.na(collapse_to_age), list(ihme_loc_id, year_id, collapse_to_age)],
                                   all.x = T, by = c("ihme_loc_id", "year_id"))
census_data_ages_adjusted[, collapse_to_age := min(max(age_start[age_start <= collapse_to_age]), collapse_to_age),
                          by = c(id_vars, "sex_id")]
census_data_ages_adjusted[age_start > collapse_to_age, age_start := collapse_to_age]
census_data_ages_adjusted <- census_data_ages_adjusted[, list(pop = sum(pop)), by = c(id_vars, "sex_id", "age_start")]

## aggregate age groups
aggregate_age_groups <- function(census, age_groups, terminal_age) {
  age_groups <- aggregate_ages <- eval(parse(text=age_groups))

  census <- GBDpop::collapse_age_groups(census, age_groups = age_groups, id_vars = NULL)
  return(census)
}

# # separate out needed censuses
census_data_ages_adjusted <- merge(census_data_ages_adjusted,
                                   census_specific_settings[!is.na(aggregate_ages),
                                                            list(ihme_loc_id, year_id, aggregate_ages)],
                                   all.x = T, by = c("ihme_loc_id", "year_id"))
census_data_ages_adjusted[grepl(paste(location_collapse_five_year_age_groups, collapse = "|"), ihme_loc_id) & is.na(aggregate_ages),
                          aggregate_ages := "seq(0, terminal_age, 5)"]
aggregate_censuses <- census_data_ages_adjusted[!is.na(aggregate_ages)]
census_data_ages_adjusted <- census_data_ages_adjusted[is.na(aggregate_ages)]
census_data_ages_adjusted[, aggregate_ages := NULL]

aggregate_censuses <- aggregate_censuses[, aggregate_age_groups(.SD[, list(age_start, pop)], unique(aggregate_ages), terminal_age),
                                         by = c(id_vars, "sex_id")]
census_data_ages_adjusted <- rbind(census_data_ages_adjusted, aggregate_censuses)

# save step
census_data_ages_adjusted[, data_step := "ages_adjusted"]
setcolorder(census_data_ages_adjusted, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_ages_adjusted, c(id_vars, "data_step", "sex_id", "age_start"))
write_csv(census_data_ages_adjusted, path = paste0(output_dir, "/data/4_ages_adjusted.csv"))


# National split locations ------------------------------------------------

gen_hierarchy <- function(parent_ihme_loc_id, ihme_loc_id, year_start, year_end, keeplocs = NA) {
  hierdf <- data.table::CJ(parent_ihme_loc_id = parent_ihme_loc_id,
                           ihme_loc_id = ihme_loc_id,
                           year_start = year_start,
                           year_end = year_end)
  if(is.na(keeplocs)) keeplocs <- ihme_loc_id
  hierdf[, keep := ihme_loc_id %in% keeplocs]
  return(hierdf)
}

align_censuses <- function(censuses, hierarchy) {
  original_data_years <- unique(censuses$year_id)
  original_censuses <- copy(censuses)
  keep_years <- merge(censuses, hierarchy[keep == T], by = "ihme_loc_id")
  keep_years <- unique(keep_years[, year_id])

  # collapse all censuses to a common set of age groups
  common_age_groups <- GBDpop::find_common_age_groups(censuses, id_vars = c(id_vars))
  censuses <- GBDpop::collapse_age_groups(censuses, common_age_groups, id_vars = c(id_vars, "sex_id"))
  collapsed_original_censuses <- copy(censuses)

  # do annualized rate of change interpolation
  last_year_censuses <- censuses[, list(year_id = max(year_id),
                                        pop = pop[year_id == max(year_id)]),
                                 by = c("ihme_loc_id", "sex_id", "age_start")]
  censuses <- censuses[, list(start_year_id = year_id[-.N], next_year_id = year_id[-1],
                              pop = pop[-.N], next_pop = pop[-1]),
                       by = c("ihme_loc_id", "sex_id", "age_start")]
  censuses[, interpolation_length := next_year_id - start_year_id]
  censuses[, arc := log(next_pop / pop) / interpolation_length]

  censuses <- censuses[, list(year_id = start_year_id + (0:(interpolation_length - 1)),
                              pop = pop * exp((0:(interpolation_length - 1)) * arc)),
                       by = c("ihme_loc_id", "start_year_id", "sex_id", "age_start")]
  censuses[, start_year_id := NULL]
  censuses <- rbind(censuses, last_year_censuses, use.names = T)

  ## - Sum children and keep only years with actual data from one child
  censuses <- censuses[year_id %in% original_data_years]
  censuses <- censuses[, list(ihme_loc_id = unique(hierarchy$parent_ihme_loc_id),
                              n_locs = .N,
                              pop = sum(pop)),
                       by = c("year_id", "sex_id", "age_start")]
  censuses <- censuses[n_locs == nrow(hierarchy)]
  censuses[, n_locs := NULL]
  ## - Keep only years where data is identified to be split between two locations (e.g. avoiding double counting)
  censuses <- censuses[between(year_id, unique(hierarchy$year_start), unique(hierarchy$year_end))]
  ## - Keep only years where data existed for the primary constitutent location(s) (user defined), not all.
  censuses <- censuses[year_id %in% keep_years]

  censuses[, c("nid", "underlying_nid", "record_type") := NA]
  censuses[, status := "best"]
  censuses[, source := "split_location"]
  censuses[, split := F]
  censuses[, aggregate := F]
  censuses[, smoother := "none"]

  setcolorder(censuses, c(id_vars, "sex_id", "age_start", "pop"))

  return(censuses)
}

location_split_data <- function(census, gbd_pop, year) {

  gbd_pop <- gbd_pop[year_id == year]

  # need to run separateley by year because they can have different age groups
  # collapse the gbd populations into the census age groups
  age_groups <- unique(census$age_start)
  gbd_pop <- GBDpop::collapse_age_groups(gbd_pop, age_groups, id_vars = c("ihme_loc_id", "sex_id"))

  # calculate the proportion in each location by year-sex-age
  gbd_pop <- gbd_pop[, list("ihme_loc_id" = get("ihme_loc_id"), prop = pop / sum(pop)),
                     by = c("sex_id", "age_start")]

  # merge on proportions and calcualte split populations
  census <- merge(census, gbd_pop, by = c("sex_id", "age_start"), all.x = T)
  census <- census[, list(ihme_loc_id, sex_id, age_start, pop = pop * prop)]

  census[, c("nid", "underlying_nid", "record_type") := NA]
  census[, status := "best"]
  census[, source := "split_location"]
  census[, split := F]
  census[, aggregate := F]
  census[, smoother := "none"]

  return(census)
}

census_data_split_locations <- copy(census_data_ages_adjusted)
census_data_split_locations[, data_step := NULL]

# East and West Germany united in 1990
deu_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = 'DEU', ihme_loc_id = c('DEU_east', 'DEU_west'),
                                   year_start = 1950, year_end = 1990)
deu_data <- align_censuses(census_data_split_locations[ihme_loc_id %in% deu_loc_hierarchy$ihme_loc_id & status == "best"],
                           deu_loc_hierarchy)

# Northern Cyprus occupied by Turkey 1974
cyp_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = 'CYP', ihme_loc_id = c('CYP_south', 'CYP_TRNC'),
                                   year_start = 1974, year_end = 2017,
                                   keeplocs = 'CYP_south')
cyp_data <- align_censuses(census_data_split_locations[ihme_loc_id %in% cyp_loc_hierarchy$ihme_loc_id & status == "best"],
                           cyp_loc_hierarchy)

# Kosovo not included in Serbia censuses
srb_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = 'SRB', ihme_loc_id = c('SRB_noK', 'SRB_KOS'),
                                   year_start = 1991, year_end = 2011,
                                   keeplocs = 'SRB_noK')
srb_data <- align_censuses(census_data_split_locations[ihme_loc_id %in% srb_loc_hierarchy$ihme_loc_id & status == "best"],
                           srb_loc_hierarchy)

# Malaysia split into peninsular, sarawak, and sabah
mys_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = 'MYS', ihme_loc_id = c('MYS_PEN', 'MYS_SAB', 'MYS_SAR'),
                                   year_start = 1950, year_end = 1980)
mys_data <- align_censuses(census_data_split_locations[ihme_loc_id %in% mys_loc_hierarchy$ihme_loc_id & status == "best"],
                           mys_loc_hierarchy)


# Transnistria not included in modldova censuses
mda_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = 'MDA', ihme_loc_id = c('MDA_noTrans', 'MDA_TRN'),
                                   year_start = 2004, year_end = 2015,
                                   keeplocs = "MDA_noTrans")
mda_data <- align_censuses(census_data_split_locations[ihme_loc_id %in% mda_loc_hierarchy$ihme_loc_id & status == "best"],
                           mda_loc_hierarchy)
mda_data[year_id == 2014, status := "excluded"]

# Yugoslavia
yug_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = 'YUG', ihme_loc_id = c("SRB", "MNE", "BIH", "HRV", "MKD", "SVN"),
                                   year_start = 1950, year_end = 1992)
yug_data <- census_data_split_locations[ihme_loc_id %in% yug_loc_hierarchy$parent_ihme_loc_id]
yug_data <- yug_data[, location_split_data(.SD[, list(sex_id, age_start, pop)],
                                           gbd_pop_1_2017_best[ihme_loc_id %in% yug_loc_hierarchy$ihme_loc_id],
                                           year = year_id),
                     by = c("year_id")]

# mark as duplicate location years we already have split out
already_split_out_yug_data <- unique(census_data_split_locations[ihme_loc_id %in% yug_loc_hierarchy$ihme_loc_id & status == "best",
                                                                 list(ihme_loc_id, year_id, actual_data_exists = T)])
yug_data <- merge(yug_data, already_split_out_yug_data, by = c("ihme_loc_id", "year_id"), all.x = T)
yug_data[actual_data_exists == T, status := "duplicate"]
yug_data[, actual_data_exists := NULL]

# USSR: republics confirmed with Julia Gall looking at reported locations for 1959, 1979, and 1989 censuses
ussr_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = 'USSR',
                                    ihme_loc_id = c("RUS", "KAZ", "KGZ", "TJK", "TKM", "UZB", "BLR", "EST", "LVA", "LTU", "MDA", "UKR", "AZE", "ARM", "GEO"),
                                    year_start = 1950, year_end = 1989)

parent_loc_hierarchy <- rbind(deu_loc_hierarchy, cyp_loc_hierarchy, srb_loc_hierarchy, mys_loc_hierarchy,
                              mda_loc_hierarchy, yug_loc_hierarchy, ussr_loc_hierarchy, use.names = T, fill = T)

## Use death number scalars to account for non residents not counted in censuses in Singapore
sng_scalar <- fread("FILEPATH")
sng_scalar <- sng_scalar[, list(year_id, sex_id, age_start, nonres_scalar)]
sng_scalar_2017 <- sng_scalar[year_id == 2016]
sng_scalar_2017[, year_id := 2017]
sng_scalar <- rbind(sng_scalar, sng_scalar_2017)

sng_data <- census_data_split_locations[ihme_loc_id == 'SGP_noMig']
sng_data[age_start >= max(sng_scalar$age_start), age_start := max(sng_scalar$age_start)]
sng_data[age_start != max(sng_scalar$age_start), age_start := as.integer(plyr::round_any(age_start, 5, floor))]
sng_data <- sng_data[, .(pop = sum(pop)), by = c(id_vars, "sex_id", "age_start")]
sng_data <- merge(sng_data, sng_scalar, by = c('year_id', 'sex_id', 'age_start'), all.x = T)
sng_data[, pop := pop * nonres_scalar]
sng_data[, nonres_scalar := NULL]
sng_data[, ihme_loc_id := 'SGP']

ind_data <- census_data_split_locations[ihme_loc_id == "IND" & status == "best"]
ind_adjustments <- list("1991" = "IND_4854", # Jammu and Kashmir
                        "1981" = c("IND_4843"), # Assam (Mizoram included in the data)
                        "1961" = "IND_4869") # Sikkim
ind_data <- lapply(unique(ind_data$year_id), function(year) {
  data <- ind_data[year_id == year]

  census_age_groups <- unique(data$age_start)
  adjustment_years <- as.numeric(names(ind_adjustments))
  if (year %in% adjustment_years) {
    adjustment_locs <- ind_adjustments[[as.character(year)]]
    gbd_pop <- gbd_pop_1_2017_best[year_id == year & ihme_loc_id %in% adjustment_locs]
    gbd_pop <- GBDpop::collapse_age_groups(gbd_pop, census_age_groups, id_vars = c("year_id", "sex_id"))
    gbd_pop[, ihme_loc_id := "IND"]
    setnames(gbd_pop, "pop", "adjustment_pop")

    data <- merge(data, gbd_pop, by = c("ihme_loc_id", "year_id", "sex_id", "age_start"), all = T)
    data[, pop := pop + adjustment_pop]
    data[, adjustment_pop := NULL]
  }
  setcolorder(data, c(id_vars, "sex_id", "age_start", "pop"))
  return(data)
})
ind_data <- rbindlist(ind_data)

split_data <- rbind(deu_data, cyp_data, srb_data, mys_data, sng_data, mda_data, yug_data, ind_data, use.names = T)

# drop the old parent location data
census_data_split_locations <- census_data_split_locations[!(ihme_loc_id %in% parent_loc_hierarchy$ihme_loc_id &
                                                               !ihme_loc_id %in% location_hierarchy$ihme_loc_id)]
census_data_split_locations <- census_data_split_locations[ihme_loc_id != "IND"]
census_data_split_locations <- rbind(census_data_split_locations, split_data)

# As an output of this step we can subset to only ihme locations that we model
census_data_split_locations <- census_data_split_locations[ihme_loc_id %in% model_locations]

census_data_split_locations[, data_step := "split_locations"]
setcolorder(census_data_split_locations, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_split_locations, c(id_vars, "data_step", "sex_id", "age_start"))
write_csv(census_data_split_locations, path = paste0(output_dir, "/data/5_split_locations.csv"))

# PES correction ----------------------------------------------------------

dismod_fit <- dismod_fit[, list(sex_id, age_start = as.integer(age_group_years_start), underenum_pct = mean)]
dismod_fit <- dismod_fit[, list(underenum_pct = mean(underenum_pct)), by = c("sex_id", "age_start")]
dismod_fit <- dismod_fit[, list(age = 0:terminal_age,
                                underenum_pct = approx(x = age_start, y = underenum_pct,
                                                       method = "linear", xout = 0:terminal_age)$y),
                         by = "sex_id"]
dismod_fit[, underenum_pct := (1 - underenum_pct)]
dismod_fit[, underenum_pct := underenum_pct + 1]

pdf(paste0(output_dir, "/diagnostics/pes_age_patterns.pdf"), width = 15, height = 10)
plot <- ggplot(dismod_fit, aes(x = age, y = underenum_pct, colour = factor(sex_id, labels = c("Male", "Female")))) +
  geom_hline(yintercept = 0) +
  geom_line() + theme_bw() +
  labs(title = "Underenumeration global age pattern averaged over 1990-2017", colour = "Sex")
print(plot)
dev.off()

# determine all possible age group combinations
census_data_pes_correction <- copy(census_data_split_locations)
census_data_pes_correction <- GBDpop::calculate_full_age_groups(census_data_pes_correction, id_vars = c(id_vars, "data_step", "sex_id"))
census_age_groups <- unique(census_data_pes_correction[, list(age_start, age_end)])
census_age_groups[is.na(age_end), age_end := terminal_age]
census_age_groups <- census_age_groups[, list(age = age_start:age_end), by = c("age_start", "age_end")]

# calculate mean underenumeration value for each possible age group
dismod_fit <- merge(census_age_groups, dismod_fit, by = c("age"), all.x = T, allow.cartesian = T)
dismod_fit <- dismod_fit[, list(underenum_pct = mean(underenum_pct)), by = c("sex_id", "age_start", "age_end")]
dismod_fit[age_end == terminal_age, age_end := NA]

pes_data <- fread(paste0(pop_dir, "data_prep/pes_prep/compiled/compiled_pes_data.csv"))
pes_data <- pes_data[age_start == 0 & is.na(age_end) & sex_id == 3]
pes_data[, c("sdi", "region_name", "super_region_name", "location_id") := NULL]
pes_data[, year_id := as.integer(year_id)]
pes_data[, underenum_pct := as.numeric(underenum_pct)]
pes_data <- pes_data[, list(ihme_loc_id, year_id, sex_id, underenum_pct)]

## PREP DATA
pes_data <- merge(pes_data, sdi_data, by = c('ihme_loc_id', 'year_id'), all.x = T)
pes_data <- pes_data[!(ihme_loc_id == 'ZAF' & year_id == 2011)]
pes_data <- pes_data[!(ihme_loc_id == 'GHA' & year_id == 1960)]

## FIT REGRESSION AND SIMULATE COEFFICIENTS FROM MULTIVARIATE NORMAL

pes_correction_mod <- lm(underenum_pct ~ sdi, data = pes_data)

set.seed(49023)
simbetas <- MASS::mvrnorm(mu = coef(pes_correction_mod), Sigma = vcov(pes_correction_mod), n = 1000)
simbetas <- data.table(simbetas)
setnames(simbetas, c(1,2), c('int', 'sdi_coef'))
simbetas[, sim := seq(.N)]

possible_sdi_values <- seq(0, 1, 0.0001)
sim_corrections <- simbetas[, list(sdi = possible_sdi_values,
                                   pred_net_pes_correction = int + sdi_coef * possible_sdi_values),
                            by = c("sim")]
corrections <- sim_corrections[, list(pred_net_pes_correction = mean(pred_net_pes_correction),
                                      pred_net_pes_correction_lb = quantile(pred_net_pes_correction, 0.025),
                                      pred_net_pes_correction_ub = quantile(pred_net_pes_correction, 0.975),
                                      pred_net_pes_correction_var = var(pred_net_pes_correction)),
                               by = c("sdi")]

## DIAGNOSTICS PLOTS
plotdatadf <- melt(pes_data[, .(ihme_loc_id, year_id, sdi, underenum_pct)], measure = patterns('pct'), variable.name = 'space', value.name = 'pes_correction')
plotdatadf[, label := paste0(ihme_loc_id, " ", year_id)]

pdf(paste0(output_dir, "/diagnostics/net_pes_regression.pdf"), width = 15, height = 10)
plot <- ggplot(data = plotdatadf) + geom_line(data = corrections, aes(x = sdi, y = pred_net_pes_correction), color = 'red') +
  geom_ribbon(data = corrections, aes(x = sdi, ymin = pred_net_pes_correction_lb, ymax = pred_net_pes_correction_ub), fill = 'red', alpha = .3) +
  geom_point(aes(x = sdi, y = pes_correction), shape = 19, size = 2, alpha = .7) +
  ggrepel::geom_text_repel(aes(x = sdi, y = pes_correction, label = label), size = 2.5) +
  theme_bw()
print(plot)
dev.off()

## APPLY CORRECTION TO ALL CENSUS DATA EXCEPT THOSE WHERE WE NEED TO USE RAW CORRECTION
census_data_pes_correction <- merge(census_data_pes_correction, sdi_data, by = c("ihme_loc_id", "year_id"), all.x = T)
census_data_pes_correction[, sdi := plyr::round_any(sdi, 0.0001)]
census_data_pes_correction <- merge(census_data_pes_correction, corrections[, list(sdi, pred_net_pes_correction)], by = c('sdi'), all.x = T)
census_data_pes_correction[is.na(pred_net_pes_correction), pred_net_pes_correction := 0] # These historical locations don't have an sdi value
census_data_pes_correction[, sdi := NULL]

# Don't apply regression correction if we have raw pes corrections we trust
census_data_pes_correction[, country_ihme_loc_id := substr(ihme_loc_id, 1, 3)]
use_raw_net_pes_adjustment_locs <- c('AUS', 'USA', 'NZL')
no_adjust <- pes_data[ihme_loc_id %in% use_raw_net_pes_adjustment_locs,
                      list(country_ihme_loc_id = ihme_loc_id, year_id, raw_net_pes_correction = underenum_pct)]
census_data_pes_correction <- merge(census_data_pes_correction, no_adjust, by = c('country_ihme_loc_id', 'year_id'), all.x = T)
census_data_pes_correction[!is.na(raw_net_pes_correction), pred_net_pes_correction := raw_net_pes_correction]
census_data_pes_correction[, c("raw_net_pes_correction") := NULL]

# Merge on age specific predicted underenumeration percent
census_data_pes_correction[, pred_net_pes_correction := 1 + (pred_net_pes_correction / 100)]
census_data_pes_correction <- merge(census_data_pes_correction, dismod_fit, by = c("sex_id", "age_start", "age_end"), all = T)
census_data_pes_correction[, adjusted_pop := pop * underenum_pct]
census_data_pes_correction[, adjusted_total_pop := sum(adjusted_pop), by = c(id_vars, "data_step")]
census_data_pes_correction[, total_pop := sum(pop), by = c(id_vars, "data_step")]
census_data_pes_correction[, dismod_net_pes_correction := adjusted_total_pop / total_pop]
census_data_pes_correction[, constant := pred_net_pes_correction - dismod_net_pes_correction]
census_data_pes_correction[, adjusted_pop := pop * (underenum_pct + constant)]
census_data_pes_correction[, adjusted_total_pop := sum(adjusted_pop), by = c(id_vars, "data_step")]
census_data_pes_correction[, dismod_net_pes_correction := adjusted_total_pop / total_pop]

# For some location years or locations we don't want to use the adjusted populations
census_data_pes_correction[ihme_loc_id == "ZAF" & year_id == 2011, adjusted_pop := pop]
census_data_pes_correction[ihme_loc_id == "CAF" & year_id == 2003, adjusted_pop := pop]
census_data_pes_correction[ihme_loc_id == "NGA" & year_id == 2014, adjusted_pop := pop]
census_data_pes_correction[ihme_loc_id == "KAZ" & year_id == 2009, adjusted_pop := pop]
census_data_pes_correction[ihme_loc_id == "BGD" & year_id %in% c(1951, 1961, 1971, 1981, 1991), adjusted_pop := pop]
census_data_pes_correction[ihme_loc_id == "PAK" & year_id %in% c(1972, 1981), adjusted_pop := pop]
use_uncorrected_data_locations <- c("AUT", "BEL", "DNK", "FIN", "HUN", "ITA", "NOR", "SWE", "TUR", "CZE")
census_data_pes_correction[country_ihme_loc_id %in% use_uncorrected_data_locations, adjusted_pop := pop]
census_data_pes_correction[record_type == "registry", adjusted_pop := pop]
census_data_pes_correction[, c("pred_net_pes_correction", "underenum_pct", "adjusted_total_pop", "total_pop", "dismod_net_pes_correction", "constant") := NULL]
census_data_pes_correction[, c("age_end", "n", "pop", "country_ihme_loc_id") := NULL]
setnames(census_data_pes_correction, "adjusted_pop", "pop")

census_data_pes_correction[, data_step := "pes_corrected"]
setcolorder(census_data_pes_correction, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_pes_correction, c(id_vars, "data_step", "sex_id", "age_start"))
write_csv(census_data_pes_correction, path = paste0(output_dir, "/data/6_pes_corrected.csv"))
write_csv(corrections, path = paste0(output_dir, '/data/6_pes_correction_variance.csv'))


# Create baseline populations ---------------------------------------------

summary_counts <- rbind(summary_counts, data.table(variable = "scale_backprojected_baseline",
                                                   value = nrow(unique(unique(location_specific_settings[ihme_loc_id %in% national_locations & scale_backprojected_baseline == T,
                                                                                          list(ihme_loc_id = substr(ihme_loc_id, 1, 3))])))))
summary_counts <- rbind(summary_counts, data.table(variable = "use_backprojected_baseline",
                                                   value = nrow(unique(unique(location_specific_settings[ihme_loc_id %in% national_locations & use_backprojected_baseline == F,
                                                                                                         list(ihme_loc_id = substr(ihme_loc_id, 1, 3))])))))


#' Creates the baseline population in 1950 using backwards ccmpp. Splits to
#' single year age groups using the nLx proportions in the 1950 GBD lifetable.
#'
#' @param censuses data.table with all 'best' censuses for a given location
#' @param lifetables data.table with all summary lifetables for all locations
#' @param ihme_loc the 'ihme_loc_id' being run, used to subset the 'lifetables'
#' @param sex the 'sex_id' being run, used to subset the 'lifetables'
#' @param terminal_age terminal age needed in the baseline census
#' @param age_int age interval needed in the baseline census
#' @param baseline_year the 'year_id' needed for the baseline census
#' @return data.table with just the baseline population counts.
#'
#' @import data.table
generate_baseline_pop <- function(censuses, lifetables, ihme_loc, sex,
                                  terminal_age = 95, age_int = 1,
                                  baseline_year = 1950) {

  print(ihme_loc)
  lifetables <- lifetables[ihme_loc_id == ihme_loc & sex_id == sex]

  # get rid of censuses prior to intended baseline year.
  # these censuses could possibly be used in the future.
  censuses <- censuses[year_id >= baseline_year]

  # ignore censuses where total population exists
  censuses[, total_pop := length(age_start) == 1, by = "year_id"]
  if (nrow(censuses[total_pop == F]) == 0) {
    print(censuses)
    stop("No non-total pop census exists")
  }

  # do annualized rate of change interpolation
  scale_backprojected_baseline <- location_specific_settings[ihme_loc_id == ihme_loc, scale_backprojected_baseline]
  if (scale_backprojected_baseline) {
    census_years <- unique(censuses$year_id)
    total_pop_censuses <- censuses[year_id %in% census_years[c(1, 2)], list(pop = sum(pop)), by = c("year_id")]
    interpolation_length <- diff(census_years)[1]
    arc <- log(total_pop_censuses[year_id == census_years[2], pop] / total_pop_censuses[year_id == census_years[1], pop]) / interpolation_length
    years_to_baseline <- census_years[1] - baseline_year
    baseline_total_pop <- total_pop_censuses[year_id == census_years[1], pop] * exp(-years_to_baseline * arc)
  }

  oldest_census_available_year <- min(censuses[total_pop == F]$year_id)
  censuses[, total_pop := NULL]
  oldest_census <- censuses[year_id == oldest_census_available_year]
  initial_columns <- setdiff(names(oldest_census), c("year_id", "age_start", "pop"))

  # create single year age groups if they don't exist already
  if (!check_single_year_age_groups(oldest_census$age_start) | max(oldest_census$age_start) < terminal_age) {
    original_age_groups <- unique(oldest_census$age_start)
    original_age_intervals <- diff(original_age_groups)
    oldest_census <- oldest_census[, GBDpop::split_age_groups(.SD, lifetables[year_id == oldest_census_available_year],
                                                              terminal_age = terminal_age),
                                   by = setdiff(names(oldest_census), c("age_start", "pop"))]
    oldest_census <- oldest_census[, GBDpop::smooth_age_pattern(.SD, max_age_int = max(original_age_intervals)),
                                   by = setdiff(names(oldest_census), c("age_start", "pop"))]
  }

  # Need to do backwards ccmpp
  if (oldest_census_available_year > baseline_year) {

    # calculate nLx survival ratios
    lifetables <- lifetables[between(year_id, baseline_year, oldest_census_available_year)]
    survival <- lifetables[, GBDpop::calculate_nLx_survival_ratio(.SD, terminal_age = max(oldest_census$age_start),
                                                                  age_int = age_int),
                           by = c("year_id")]
    survival <- GBDpop::average_survival(survival)

    # project population backwards to the baseline year
    oldest_census <- oldest_census[, list(year_id, age = age_start, value = pop)]
    oldest_census <- GBDpop::backwards_ccmpp(baseline = oldest_census, surv = survival,
                                             years = baseline_year:oldest_census_available_year)

    # force more older ages to be missing
    fill_missing_triangle_above_age <- location_specific_settings[ihme_loc_id == ihme_loc, fill_missing_triangle_above]
    oldest_census[age > (fill_missing_triangle_above_age + (year_id - baseline_year)), value := NA]

    # fill the missing upper triangle in the baseline year
    oldest_census <- GBDpop::fill_missing_triangle(oldest_census, lifetables[year_id <= oldest_census_available_year])

    # scale to arc back projected total population in baseline year
    if (scale_backprojected_baseline) {
      baseline_totals <- oldest_census[year_id == baseline_year]
      baseline_totals[, scale_total_pop := baseline_total_pop]
      baseline_totals[, backwards_ccmp_total := sum(pop)]
      baseline_totals[, scalar := scale_total_pop / backwards_ccmp_total]
      baseline_totals <- baseline_totals[, list(pop = pop * scalar, missing_triangle), by = c("year_id", "age_start")]

      oldest_census <- oldest_census[year_id != baseline_year]
      oldest_census <- rbind(baseline_totals, oldest_census)
    }

    # fill in columns needed
    oldest_census[, c("nid", "underlying_nid") := NA_integer_]
    oldest_census[, status := "best"]
    oldest_census[, record_type := "backwards_ccmpp"]
    oldest_census[, source := "backwards_ccmpp"]
    oldest_census[, split := F]
    oldest_census[, aggregate := F]
    oldest_census[, smoother := "none"]

    # check that correct age groups exist in the new baseline
    assert_ids(oldest_census, list(year_id = baseline_year:oldest_census_available_year,
                                   age_start = seq(0, terminal_age, age_int)),
               quiet = T)
  } else {
    oldest_census[, missing_triangle := F]
  }

  oldest_census[, data_step := "baseline"]
  oldest_census[, year_id := as.integer(year_id)]
  setcolorder(oldest_census, c(initial_columns, c("year_id", "age_start", "pop", "missing_triangle")))
  return(oldest_census)
}

# create 1950 censuses for all locations
# can't use this method for Eritrea since we only have total population in one year
census_data_baseline <- copy(census_data_pes_correction)
baseline_censuses <- census_data_baseline[ihme_loc_id != "ERI" & status == "best",
                                          generate_baseline_pop(.SD, lifetables = full_lifetables, ihme_loc = ihme_loc_id, sex = sex_id),
                                          by = c("ihme_loc_id", "sex_id")]
write_csv(baseline_censuses, path = paste0(output_dir, "/diagnostics/backwards_ccmpp_results.csv"))
baseline_censuses <- baseline_censuses[year_id == baseline_year]
baseline_censuses[, missing_triangle := NULL]

census_data_baseline <- census_data_baseline[!(status == "best" & year_id == baseline_year)]
census_data_baseline <- rbind(census_data_baseline, baseline_censuses, use.names = T)
census_data_baseline[, data_step := "baseline"]

setcolorder(census_data_baseline, c(id_vars, "data_step", "sex_id", "age_start", "pop"))
setkeyv(census_data_baseline, c(id_vars, "data_step", "sex_id", "age_start"))
write_csv(census_data_baseline, path = paste0(output_dir, "/data/7_baseline.csv"))


# Compile together different data processing steps outputs ----------------

census_data_files <- paste0(output_dir, "/data/",
                            c("0_raw.csv", "1_distributed_unknown.csv",
                              "2_aggregated.csv", "3_unheaped.csv",
                              "4_ages_adjusted.csv", "5_split_locations.csv",
                              "6_pes_corrected.csv", "7_baseline.csv"))
census_data <- lapply(census_data_files, fread)
census_data <- rbindlist(census_data, use.names = T, fill = T)
census_data <- GBDpop::calculate_full_age_groups(census_data, id_vars = c(id_vars, "data_step", "sex_id"))

setcolorder(census_data, c(id_vars, "data_step", "sex_id", "age_start", "age_end", "n", "pop"))
setkeyv(census_data, c(id_vars, "data_step", "sex_id", "age_start", "age_end", "n"))

write_csv(census_data, path = paste0(output_dir, "/data/compiled.csv"))


# Check data formatting ---------------------------------------------------

census_data <- census_data[data_step == "baseline"]

# check for duplicates marked best
check_duplicates <- unique(census_data[status == "best",
                                       list(ihme_loc_id, year_id, source, status, nid, underlying_nid, split, aggregate)])
check_duplicates[, data_points := .N, by = c("ihme_loc_id", "year_id")]
if (nrow(check_duplicates[data_points > 1]) > 0) {
  print(check_duplicates[data_points > 1])
  stop("duplicates marked best")
}

# check that all locations have at least one census after the baseline
check_censuses <- unique(census_data[status == "best" & year_id >= baseline_year,
                                     list(ihme_loc_id)])
censuses_id_vars <- list(ihme_loc_id = model_locations)
assert_ids(check_censuses, id_vars = censuses_id_vars)

# check that all locations have baseline counts marked best (except ERI right now)
ignore_locations <- c("ERI")
check_baseline <- unique(census_data[status == "best" & year_id == baseline_year,
                                     list(ihme_loc_id, year_id, sex_id , age_start)])
baseline_id_vars <- list(ihme_loc_id = model_locations[!model_locations %in% ignore_locations],
                         year_id = 1950, sex_id = c(1, 2), age_start = seq(0, terminal_age, 1))
assert_ids(check_baseline, id_vars = baseline_id_vars)

# assert
assert_values(census_data, colnames=c("ihme_loc_id", "year_id", "sex_id", "source", "age_start", "n", "pop"), test="not_na")
assert_values(census_data, colnames="sex_id", test="in", test_val = c(1, 2))
assert_values(census_data, colnames="age_start", test="gte", test_val = 0)
assert_values(census_data, colnames="status", test="in", test_val = c("best", "excluded", "duplicate", "not_census_year", "check_extraction"))
assert_values(census_data, colnames="pop", test="gte", test_val = 0)
