################################################################################
# Description: Adjust national locations for differences from the official GBD
# locations.
# - In Germany, Cyprus, Serbia/Kosovo, Malaysia, Moldova/Transnistria, use
# annualized rate of change of age-sex specific population counts for split
# national locations to create full time series. Combine counts in overlapping
# years for the full national location
# - Split Yugoslavia population data into modern day components using gbd
# population estimates
# - Adjust India national data for years where census was not conducted in some
# states using gbd population estimates
# - data_stage: "processed, national location adjustments"
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

location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))
national_mapping <- fread(paste0(output_dir, "/inputs/national_mapping.csv"))
age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
gbd_pop <- fread(paste0(output_dir, "/inputs/gbd_pop_current_round_best.csv"))
gbd_pop[, c("run_id", "lower", "upper") := NULL]

census_id_vars <- c(census_id_vars, "split", "aggregate", "smoother")

gen_hierarchy <- function(parent_ihme_loc_id, ihme_loc_id, year_start, year_end, keeplocs = NA) {

  hierdf <- national_mapping[parent_id == parent_ihme_loc_id,
                             list(parent_ihme_loc_id = parent_id, ihme_loc_id = child_id,
                                  parent_location_id, location_id = child_location_id,
                                  year_start = year_start, year_end = year_end)]
  if(is.na(keeplocs)) keeplocs <- unique(hierdf$ihme_loc_id)
  hierdf[, keep := ihme_loc_id %in% keeplocs]
  return(hierdf)
}


# Align censuses with arc interpolation -----------------------------------

align_censuses <- function(censuses, hierarchy) {

  original_data_years <- unique(censuses$year_id)
  keep_years <- merge(censuses, hierarchy[(keep)], by = "location_id")
  keep_years <- unique(keep_years[, year_id])

  # collapse all censuses to a common set of age groups
  censuses <- merge(censuses, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
  censuses <- agg_age_data(censuses, id_vars = census_id_vars)
  setkeyv(censuses, c(census_id_vars, "sex_id", "age_group_id"))

  # do annualized rate of change interpolation
  last_year_censuses <- censuses[, list(year_id = max(year_id),
                                        mean = mean[year_id == max(year_id)]),
                                 by = c("location_id", "sex_id", "age_group_id")]
  censuses <- censuses[, list(start_year_id = year_id[-.N], next_year_id = year_id[-1],
                              mean = mean[-.N], next_pop = mean[-1]),
                       by = c("location_id", "sex_id", "age_group_id")]
  censuses[, interpolation_length := next_year_id - start_year_id]
  censuses[, arc := log(next_pop / mean) / interpolation_length]

  censuses <- censuses[, list(year_id = start_year_id + (0:(interpolation_length - 1)),
                              mean = mean * exp((0:(interpolation_length - 1)) * arc)),
                       by = c("location_id", "start_year_id", "sex_id", "age_group_id")]
  censuses[, start_year_id := NULL]
  censuses <- rbind(censuses, last_year_censuses, use.names = T)

  ## - Sum children and keep only years with actual data from one child
  censuses <- censuses[year_id %in% original_data_years]
  censuses <- censuses[, list(location_id = unique(hierarchy$parent_location_id),
                              n_locs = .N,
                              mean = sum(mean)),
                       by = c("year_id", "sex_id", "age_group_id")]
  censuses <- censuses[n_locs == nrow(hierarchy)]
  censuses[, n_locs := NULL]
  ## - Keep only years where data is identified to be split between two locations (e.g. avoiding double counting)
  censuses <- censuses[between(year_id, unique(hierarchy$year_start), unique(hierarchy$year_end))]
  ## - Keep only years where data existed for the primary constitutent location(s) (user defined), not all.
  censuses <- censuses[year_id %in% keep_years]

  censuses[, c("underlying_nid", "data_stage") := NA]
  censuses[, nid := 375237]
  censuses[, record_type := "unknown"]
  censuses[, method_short := "Unknown"]
  censuses[, pes_adjustment_type := "not applicable"]
  censuses[, outlier_type := "not outliered"]
  censuses[, source_name := "split_location"]
  censuses[, split := F]
  censuses[, aggregate := F]
  censuses[, smoother := "none"]
  setcolorder(censuses, c(census_id_vars, "sex_id", "age_group_id", "mean"))
  return(censuses)
}

census_data <- fread(paste0(output_dir, "/outputs/06_make_manual_age_group_changes.csv"))

# East and West Germany united in 1990
deu_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = "DEU",
                                   year_start = 1950, year_end = 1990)
deu_data <- align_censuses(census_data[location_id %in% deu_loc_hierarchy$location_id & outlier_type == "not outliered"],
                           deu_loc_hierarchy)

# Northern Cyprus occupied by Turkey 1974
cyp_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = "CYP",
                                   year_start = 1974, year_end = 2017,
                                   keeplocs = "CYP_south")
cyp_data <- align_censuses(census_data[location_id %in% cyp_loc_hierarchy$location_id & outlier_type == "not outliered"],
                           cyp_loc_hierarchy)

# Kosovo not included in Serbia censuses
srb_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = "SRB",
                                   year_start = 1991, year_end = 2011,
                                   keeplocs = "SRB_noK")
srb_data <- align_censuses(census_data[location_id %in% srb_loc_hierarchy$location_id & outlier_type == "not outliered"],
                           srb_loc_hierarchy)

# Malaysia split into peninsular, sarawak, and sabah
mys_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = "MYS",
                                   year_start = 1950, year_end = 1980)
mys_data <- align_censuses(census_data[location_id %in% mys_loc_hierarchy$location_id & outlier_type == "not outliered"],
                           mys_loc_hierarchy)

# Transnistria not included in modldova censuses
mda_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = "MDA",
                                   year_start = 2004, year_end = 2015,
                                   keeplocs = "MDA_noTrans")
mda_data <- align_censuses(census_data[location_id %in% mda_loc_hierarchy$location_id & outlier_type == "not outliered"],
                           mda_loc_hierarchy)
mda_data[year_id == 2014, outlier_type := "excluded"]


# Adjust SGP data for residents/non-residents -----------------------------

sgp_census_years <- c(1990, 2000, 2010)

## Use death number scalars to account for non residents not counted in censuses in Singapore
sgp_deaths <- fread("FILEPATH")
sgp_deaths <- sgp_deaths[year_id %in% sgp_census_years, list(year_id, sex_id, age_group_id, resident_status, deaths)]
sgp_deaths <- merge(sgp_deaths, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id")

sgp_resident_loc_id <- national_mapping[child_id == "SGP_noMig", child_location_id]
sgp_resident_census_data <- census_data[location_id == sgp_resident_loc_id & year_id %in% sgp_census_years & record_type == "census"]
sgp_resident_census_data <- merge(sgp_resident_census_data, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id")
sgp_broad_age_groups_data <- census_data[location_id == 69 & nid == 409202 & year_id %in% sgp_census_years]

census_data_agg <-
  mortcore::agg_results(
    census_data[location_id == 69 & nid == 409202 & !year_id %in% sgp_census_years],
    c(census_id_vars, "age_group_id", "sex_id"),
    "mean",
    agg_hierarchy = FALSE,
    age_aggs = 22
  )[
  age_group_id == 22
  ]

census_data <- rbind(census_data, census_data_agg, use.names = TRUE)
census_data[location_id == 69 & nid == 409202 & age_group_id != 22, outlier_type := "duplicated"]

sgp_data <- lapply(sgp_census_years, function(year) {
  print(year)

  resident_census_data <- sgp_resident_census_data[year_id == year]
  deaths <- sgp_deaths[year_id == year]
  broad_age_groups_data <- sgp_broad_age_groups_data[year_id == year, list(year_id, sex_id, age_group_id, broad_data = mean)]

  # find common age groups between resident census data and deaths data
  common_age_groups <- data.table(age_group_years_start = intersect(resident_census_data$age_group_years_start, deaths$age_group_years_start))
  common_age_groups <- mortcore::age_start_to_age_group_id(common_age_groups)

  # collapse resident census data to common age groups
  resident_census_data[, age_group_years_start := NULL]
  resident_census_data <- mortcore::agg_results(resident_census_data, id_vars = c(census_id_vars, "sex_id", "age_group_id"),
                                                value_vars = "mean", agg_hierarchy = F,
                                                age_aggs = common_age_groups$age_group_id)
  resident_census_data <- resident_census_data[age_group_id %in% common_age_groups$age_group_id]

  # collapse deaths data to common age groups
  deaths[, age_group_years_start := NULL]
  deaths <- mortcore::agg_results(deaths, id_vars = c("year_id", "sex_id", "age_group_id", "resident_status"),
                                  value_vars = "deaths", agg_hierarchy = F,
                                  age_aggs = common_age_groups$age_group_id)
  deaths <- deaths[age_group_id %in% common_age_groups$age_group_id]

  # calculate total over resident only scalars
  scalar <- dcast(deaths, ...~resident_status, value.var = "deaths")
  scalar[, nonres_scalar := total/resident_only]
  scalar <- scalar[, list(year_id, sex_id, age_group_id, scalar = total / resident_only)]

  # use scalars to scale up to resident + non resident population
  adjusted_census_data <- merge(resident_census_data, scalar, by = c("year_id", "sex_id", "age_group_id"))
  adjusted_census_data[, mean := mean * scalar]
  adjusted_census_data[, scalar := NULL]

  # calculate scalar compared to broad age group total population data
  broad_adjusted_census_data <- mortcore::agg_results(adjusted_census_data[, list(year_id, sex_id, age_group_id, adjusted_data = mean)],
                                                      id_vars = c("year_id", "sex_id", "age_group_id"), value_vars = "adjusted_data",
                                                      agg_hierarchy = F, age_aggs = unique(broad_age_groups_data$age_group_id))
  broad_adjusted_census_data <- broad_adjusted_census_data[age_group_id %in% unique(broad_age_groups_data$age_group_id)]
  broad_adjusted_census_data <- merge(broad_adjusted_census_data, broad_age_groups_data, by = c("year_id", "sex_id", "age_group_id"), all = T)
  broad_scalar <- broad_adjusted_census_data[, list(broad_scalar = broad_data / adjusted_data), by = c("year_id", "sex_id", "age_group_id")]

  # merge on scalars
  adjusted_census_data <- merge(adjusted_census_data, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id", all.x = T)
  broad_scalar <- merge(broad_scalar, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id", all.x = T)
  broad_scalar[, age_group_id := NULL]
  setkeyv(broad_scalar, c("year_id", "sex_id", "age_group_years_start"))
  setkeyv(adjusted_census_data, c(census_id_vars, "sex_id", "age_group_years_start"))
  scaled_census_data <- broad_scalar[adjusted_census_data, roll = +Inf, on = c("year_id", "sex_id", "age_group_years_start")]

  scaled_census_data[, mean := mean * broad_scalar]
  scaled_census_data[, c("broad_scalar", "age_group_years_start") := NULL]

  scaled_census_data[, location_id := get_location_id("SGP", location_hierarchy)]
  scaled_census_data[, c("underlying_nid", "data_stage") := NA]
  scaled_census_data[, nid := 375237]
  scaled_census_data[, record_type := "unknown"]
  scaled_census_data[, method_short := "Unknown"]
  scaled_census_data[, pes_adjustment_type := "not applicable"]
  scaled_census_data[, outlier_type := "not outliered"]
  scaled_census_data[, source_name := "split_location"]
  scaled_census_data[, split := F]
  scaled_census_data[, aggregate := F]
  scaled_census_data[, smoother := "none"]
  setcolorder(scaled_census_data, c(census_id_vars, "sex_id", "age_group_id", "mean"))

  return(scaled_census_data)
})
sgp_data <- rbindlist(sgp_data)


# Split aggregate historical locations (Yugoslavia) -----------------------

location_split_data <- function(data, gbd_pop) {

  # need to run separateley by year because they can have different age groups
  census_years <- unique(data$year_id)

  split_data <- lapply(census_years, function(year) {
    print(year)
    census_year <- data[year_id == year]
    gbd_pop_year <- gbd_pop[year_id == year]

    # collapse the gbd populations into the census age groups
    census_year <- merge(census_year, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
    target_age_start <- sort(unique(census_year$age_group_years_start))
    target_age_start <- paste0("c(", paste(target_age_start, collapse = ","), ")")

    gbd_pop_year <- merge(gbd_pop_year, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
    gbd_pop_year[, aggregate_ages := target_age_start]
    gbd_pop_year <- agg_age_data(gbd_pop_year, id_vars = c("location_id", "year_id"), age_grouping_var = "aggregate_ages")

    # calculate the proportion in each location by year-sex-age
    gbd_pop_year <- gbd_pop_year[, list(location_id, prop = mean / sum(mean)), by = c("year_id", "sex_id", "age_group_id")]

    # merge on proportions and calcualte split populations
    census_year[, location_id := NULL]
    census_year <- merge(census_year, gbd_pop_year, by = c("year_id", "sex_id", "age_group_id"), all.x = T)
    census_year <- census_year[, list(location_id, year_id, sex_id, age_group_id, mean = mean * prop)]

    census_year[, c("underlying_nid", "data_stage") := NA]
    census_year[, nid := 375237]
    census_year[, record_type := "unknown"]
    census_year[, method_short := "Unknown"]
    census_year[, pes_adjustment_type := "not applicable"]
    census_year[, outlier_type := "not outliered"]
    census_year[, source_name := "split_location"]
    census_year[, split := F]
    census_year[, aggregate := F]
    census_year[, smoother := "none"]
  })
  split_data <- rbindlist(split_data)
  return(split_data)
}


# Split into child locations using last set of GBD estimates
yug_loc_hierarchy <- gen_hierarchy(parent_ihme_loc_id = "YUG",
                                   year_start = 1950, year_end = 1992)
yug_data <- census_data[location_id %in% yug_loc_hierarchy$parent_location_id]
yug_data <- location_split_data(data = yug_data, gbd_pop = gbd_pop[location_id %in% yug_loc_hierarchy$location_id])

# mark as duplicate location years we already have split out
already_split_out_yug_data <- unique(census_data[location_id %in% yug_loc_hierarchy$location_id & outlier_type == "not outliered",
                                                 list(location_id, year_id, actual_data_exists = T)])
yug_data <- merge(yug_data, already_split_out_yug_data, by = c("location_id", "year_id"), all.x = T)
yug_data[actual_data_exists == T, outlier_type := "duplicated"]
yug_data[, actual_data_exists := NULL]


# Adjust IND data for years where no census in some subnationals ----------

ind_data <- census_data[location_id == get_location_id("IND", location_hierarchy) & outlier_type == "not outliered"]
census_data <- census_data[!(location_id == get_location_id("IND", location_hierarchy) & outlier_type == "not outliered")]
ind_adjustments <- list("1991" = "4854", # Jammu and Kashmir
                        "1981" = "4843", # Assam (Mizoram included in the data)
                        "1961" = "4869") # Sikkim
adjustment_years <- as.numeric(names(ind_adjustments))
ind_data <- lapply(unique(ind_data$year_id), function(year) {
  print(year)
  data <- ind_data[year_id == year]
  if (year %in% adjustment_years) {
    # aggregate gbd_pop to census data age groups
    adjustment_locs <- as.numeric(ind_adjustments[[as.character(year)]])
    gbd_pop_year <- gbd_pop[year_id == year & location_id %in% adjustment_locs]
    data <- merge(data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
    gbd_pop_year <- merge(gbd_pop_year, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
    gbd_pop_year[, aggregate_ages :=  paste0("c(", paste(sort(unique(data$age_group_years_start)), collapse = ","), ")")]
    gbd_pop_year <- agg_age_data(gbd_pop_year, id_vars = c("location_id", "year_id"), age_grouping_var = "aggregate_ages")
    gbd_pop_year[, location_id := get_location_id("IND", location_hierarchy)]
    setnames(gbd_pop_year, "mean", "adjustment_pop")

    # add on population for missing subnationals to the national data numbers
    data <- merge(data, gbd_pop_year, by = c("location_id", "year_id", "sex_id", "age_group_id"), all = T)
    data[, mean := mean + adjustment_pop]
    data[, c("age_group_years_start", "age_group_years_end", "adjustment_pop") := NULL]
  }
  setcolorder(data, c(census_id_vars, "sex_id", "age_group_id", "mean"))
  return(data)
})
ind_data <- rbindlist(ind_data)

split_data <- rbind(deu_data, cyp_data, srb_data, mys_data, mda_data, yug_data, sgp_data, ind_data, use.names = T)
census_data <- rbind(census_data, split_data)

census_data[, data_stage := "national location adjustments"]
setcolorder(census_data, c(census_id_vars, "sex_id", "age_group_id", "mean"))
setkeyv(census_data, c(census_id_vars, "sex_id", "age_group_id"))
readr::write_csv(census_data, path = paste0(output_dir, "/outputs/07_make_national_location_adjustments.csv"))
