knitr::opts_chunk$set(echo = TRUE)

#' # Introduction
#' This document details the data preparation and modeling steps undertaken in deriving Zika incidence, prevalence, morbidity and mortality estimates for GBD 2023.
#' 
#' # Setup
#' 
#' #### Clear global environment
## ------------------------------------------------------------------------

rm(list = ls())

#' #### Get current user and set paths
## ------------------------------------------------------------------------

user <- Sys.info()[["user"]] ## Current user name
user_path <- paste0("FILEPATH") ## Location of user home folder
core_path <- paste0("FILEPATH") ## Location of Zika modeling folder
draws_output_path <- paste0("FILEPATH") # Location where draws will be saved

#' #### Set random seed (or not)
## ------------------------------------------------------------------------

random.seed <- 1 # Set random seed (set to NULL to not use a random seed; model selection was performed using a random seed of "1")
set.seed(random.seed)

#' #### Set GBD parameters
## ------------------------------------------------------------------------

gbd_round_id <- 7 ### GBD 2020
decomp_step <- "iterative"
pop_run_id <- ADDRESS
output_years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022) # Years for which final estimates are needed [note that 2016 is not in the standard list for GBD 2020 but these are critical years for Zika]
model_years <- c(1990:2022) 
inla_years <- c(2014:2019) 

#' #### Load required libraries
## ------------------------------------------------------------------------
pacman::p_load(ggplot2, data.table, INLA, matrixStats, readxl, car, magrittr, boot, openxlsx) # Add to list as needed
library(readstata13, lib.loc = user_path) # Needed for reading Stata data files (.dta)

#' #### Load shared functions
## ------------------------------------------------------------------------
source("/ihme/cc_resources/libraries/current/r/get_ids.R")
source("/ihme/cc_resources/libraries/current/r/get_population.R")
source("/ihme/cc_resources/libraries/current/r/get_location_metadata.R")
source("/ihme/cc_resources/libraries/current/r/get_age_metadata.R")
source("/ihme/cc_resources/libraries/current/r/get_model_results.R")
source("/ihme/cc_resources/libraries/current/r/get_covariate_estimates.R")
source("/ihme/cc_resources/libraries/current/r/get_bundle_data.R")
source("/ihme/cc_resources/libraries/current/r/get_bundle_version.R")
source("/ihme/cc_resources/libraries/current/r/get_draws.R")
source("/ihme/cc_resources/libraries/current/r/save_results_epi.R")
source("/ihme/cc_resources/libraries/current/r/get_crosswalk_version.R")
source("/ihme/cc_resources/libraries/current/r/save_crosswalk_version.R")
source("/ihme/cc_resources/libraries/current/r/get_elmo_ids.R")

run_date <- gsub("-", "_", Sys.time()) %>% gsub(":", "_", .) %>% gsub(" ", "_", .) 

#' # Data Cleaning and Subsetting
#' Prepare data set for modeling.
## ------------------------------------------------------------------------

#' #### Import raw data bundle
## ------------------------------------------------------------------------
df_raw <- get_bundle_version(ADDRESS, fetch = "all")
df <- copy(df_raw) 

#' #### Report number of rows in data set
## ------------------------------------------------------------------------
nrow(df)

#' #### Retrieve and merge location metadata
## ------------------------------------------------------------------------
location_hierarchy <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
df <- merge(df, location_hierarchy, by = "location_id", all.x = TRUE)

#' #### Create country field (ISO3)
## ------------------------------------------------------------------------
df$country <- substr(df$ihme_loc_id.x, 1, 3)

#' #### Specify sex_id
## ------------------------------------------------------------------------
unique(df$sex)
df[sex == "Male", sex_id := 1]
df[sex == "Female", sex_id := 2]
df[sex == "Both", sex_id := 3]

#' #### Retrieve GBD populations
## ------------------------------------------------------------------------
gbd_population <- get_population(age_group_id = "22", sex_id = c(1, 2, 3), year_id = model_years, location_id = unique(df$location_id), location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step, run_id = pop_run_id)
df <- merge(df, gbd_population, by.x = c("location_id", "year_start", "sex_id"), by.y = c("location_id", "year_id", "sex_id"), all.x = TRUE)

#' #### Retrieve geographic restrictions
## ------------------------------------------------------------------------
grs <- fread("FILEPATH")

#' #### Retrieve expansion factors
## ------------------------------------------------------------------------
#' These factors were originally calculated for dengue.
efs <- as.data.table(read.dta13("FILEPATH"))

#' Set expansion factors to regional medians, for countries lacking them
locs <- location_hierarchy[most_detailed == 1]
locs$country <- substr(locs$ihme_loc_id, 1, 3)
efs <- merge(efs, locs, by.x = "countryIso", by.y = "country", all = TRUE)

for (region in unique(efs[is.na(efAlpha), region_name])) { 
  efs[is.na(efAlpha) & region_name == region]$efAlpha <- median(efs[!is.na(efAlpha) & region_name == region, efAlpha])
  efs[is.na(efBeta) & region_name == region]$efBeta <- median(efs[!is.na(efBeta) & region_name == region, efBeta])
}

#' #### Calculate mean ef and merge it to dataset
## ------------------------------------------------------------------------
efs[, ef := (efAlpha + efBeta)/ efAlpha]
df <- as.data.table(merge(df, unique(efs[, c("countryIso", "ef")]), by.x = "country", by.y = "countryIso", all.x = TRUE))
unique(df[is.na(ef), location_name_short])

#' #### Apply expansion factors to generate new case estimates
## ------------------------------------------------------------------------
df$cases_original <- df$cases # First, save the original case numbers
df[, cases := round(cases * ef, 0)] # Apply efs

#' #### Visualize original vs. adjusted cases
## ------------------------------------------------------------------------
ggplot(df) + theme_classic() + geom_point(aes(x = log(cases_original), y = log(cases), color = region_name)) + geom_abline(intercept = 0, slope = 1)

#' #### Restrict to endemic locations
## ------------------------------------------------------------------------
endemic_locs <- unique(grs[value_endemicity == 1 & most_detailed == 1, location_id]) # Identify endemic locations (those that are considered endemic in at least one year)
df <- df[location_id %in% endemic_locs]
which(!(endemic_locs %in% df$location_id)) # Check for missing location_ids in df; should be zero; if not, add empty rows [come back to this]

#' #### Set cases to integer
## ------------------------------------------------------------------------
df$cases <- as.integer(round(df$cases, 0))

#' #### Review 2018 data
## ------------------------------------------------------------------------
#' Specify ARG 2018 data to retain
df[country == "ARG" & year_start == 2018 & is.na(value_keep_row) & origin == "local", value_keep_row := 1]
df[country == "ARG" & year_start == 2018 & is.na(value_keep_row), value_keep_row := 0]

#' Don't use values for imported cases.
df[year_start == 2018 & origin == "imported" & is.na(value_keep_row), value_keep_row := 0]

#' Drop male and female rows for MEX 2018 data (keeping the "both" values).
df[year_start == 2018 & country == "MEX" & (sex %in% c("Male", "Female")), value_keep_row := 0]

#' Drop rows reporting both local and imported cases if local values are available.
agg_origin <- as.data.table(aggregate(origin ~ location_id + year_start, df[year_start == 2018], unique))
drop_any <- agg_origin[origin %in% c('c("", "local", "imported", "any")', 'c("local", "imported", "any")'), location_id]
df[location_id %in% drop_any & year_start == 2018 & origin == "any", value_keep_row := 0]

#' Keep remaining 2018 data.
df[year_start == 2018 & is.na(value_keep_row), value_keep_row := 1]

#' #### Review and complete the value_keep_row field where missing
## ------------------------------------------------------------------------
table(df[is.na(value_keep_row), year_start]) # Summarize rows missing value_keep_row, by year

df[!is.na(group) & group_review == 1 & is.na(value_keep_row), value_keep_row := 1] # Keep rows with NA value_keep_row when group_review is 1.
df[!is.na(group) & group_review == 0 & is.na(value_keep_row), value_keep_row := 0] # Drop rows with NA value_keep_row when group_review is 0.
table(df[is.na(value_keep_row), year_start]) # Summarize rows still missing value_keep_row, by year
nrow(df[is.na(value_keep_row)]) # Number of rows still missing value_keep_row
length(unique(df[is.na(value_keep_row), c(location_id, year_start)])) # Number of unique location and year combinations among data still missing value_keep_row (this should equal the value from the previous line)

#' #### Flag remaining duplicate rows
## ------------------------------------------------------------------------
#' 2019 US Virgin Islands data, present from two sources
df[location_id == 422 & year_start == 2019 & nid == "442216" & is.na(value_keep_row), value_keep_row := 0] # Drop CDC row in favor of PAHO row (identical data)

#' 2019 Puerto Rico data, present from two sources
df[location_id == 385 & year_start == 2019 & nid == "442216" & is.na(value_keep_row), value_keep_row := 0] # Drop CDC row in favor of PAHO row (identical data)

#' #### For now, don't use national estimates for countries with subnational estimation
## ------------------------------------------------------------------------
#' #### Also drop Canada
## ------------------------------------------------------------------------
df[location_id %in% c(101), value_keep_row := 0]

#' #### Drop all U.S. states except Florida and Texas
## ------------------------------------------------------------------------
df[location_id %in% c(523:531, 533:565, 567:573), value_keep_row := 0]

#' #### After manual review, keep remaining 2019 data
## ------------------------------------------------------------------------
df[is.na(group) & is.na(group_review) & year_start == 2019 & is.na(value_keep_row), value_keep_row := 1]
table(df[is.na(value_keep_row), year_start]) # Summarize rows still missing value_keep_row, by year (should be empty)

df[is.na(value_keep_row) & country == "BGD" & year_start == 2016, value_keep_row := 1] # Keep BGD row

#' #### Investigate Florida and Texas cases
## ------------------------------------------------------------------------
df[location_id == 566 & value_keep_row == 1 & year_start == 2017, cases := 5]
df[location_id == 566 & value_keep_row == 1 & year_start == 2019, cases := 0]
temp <- data.table("location_id" = c(532, 566), "value_keep_row" = 1, "year_start" = 2018, "year_end" = 2018, "cases" = 0, "country" = "USA", "measure" = "incidence", "case_name" = "reported Zika cases")
temp <- merge(temp, location_hierarchy, by = "location_id", all.x = TRUE)
temp <- merge(temp, gbd_population, by.x = c("location_id", "year_start"), by.y = c("location_id", "year_id"), all.x = TRUE)
df <- rbind(df, temp, use.names = TRUE, fill = TRUE)
df[location_id %in% c(532, 566) & value_keep_row == 1 & year_start %in% c(2015, 2016), case_name := "confirmed local Zika cases"]

#' Drop duplicate Florida and Texas data rows for 2018
df <- df[!(location_id %in% c(532, 566) & year_start == 2018 & is.na(nid))]

#' #### Drop remaining imported cases
## ------------------------------------------------------------------------
df <- df[case_name != "imported Zika cases"]

df[location_id %in% c(98), value_keep_row := 0]

#' #### Subset to rows for modeling
## ------------------------------------------------------------------------
df <- df[value_keep_row == 1]

#' #### Drop locations outside the Americas (for now)
## ------------------------------------------------------------------------
#' We will add these back to the draws in a later step.
df_non_americas <- df[!(region_name %in% c("Andean Latin America", "Caribbean", "Central Latin America", "High-income North America", "Southern Latin America", "Tropical Latin America"))]
df <- df[region_name %in% c("Andean Latin America", "Caribbean", "Central Latin America", "High-income North America", "Southern Latin America", "Tropical Latin America")]
grs <- grs[region_name %in% c("Andean Latin America", "Caribbean", "Central Latin America", "High-income North America", "Southern Latin America", "Tropical Latin America")]

#' #### Add rows for 2014 for locations in the Americas
## ------------------------------------------------------------------------
temp <- data.table("location_id" = unique(df[region_name %in% c("Andean Latin America", "Caribbean", "Central Latin America", "High-income North America", "Southern Latin America", "Tropical Latin America"), location_id]), "value_keep_row" = 1, "year_start" = 2014, "year_end" = 2014, "cases" = 0, "cases_original" = 0, "measure" = "incidence", "case_name" = "assumed Zika cases")
temp <- rbind(temp, data.table("location_id" = 97, "value_keep_row" = 1, "year_start" = 2015, "year_end" = 2015, "cases" = 0, "cases_original" = 0, "measure" = "incidence", "case_name" = "assumed Zika cases"))
temp <- merge(temp, efs[, c("location_id", "ef")], by = "location_id", all.x = TRUE)
temp <- merge(temp, location_hierarchy, by = "location_id", all.x = TRUE)
temp <- merge(temp, gbd_population[sex_id == 3], by.x = c("location_id", "year_start"), by.y = c("location_id", "year_id"), all.x = TRUE)
temp$country <- substr(temp$ihme_loc_id, 1, 3)
temp$ihme_loc_id.x <- temp$ihme_loc_id
df <- rbind(df, temp, use.names = TRUE, fill = TRUE)

#' #### Check whether any location-years have multiple data points
## ------------------------------------------------------------------------
location_years <- data.table(aggregate(cases ~ location_id + year_start, df, FUN = function(x) length(x)))
location_years[cases > 1] # Should be empty

#' #### Check for rows lacking location or population information
## ------------------------------------------------------------------------
table(df[is.na(location_set_version_id), country])
table(df[is.na(population), country])

#' #### Report number of rows in data set
## ------------------------------------------------------------------------
nrow(df)

#' #### Check that all rows have valid outcomes
## ------------------------------------------------------------------------
nrow(df[is.na(cases) | cases < 0]) # Should be zero

#' #### Check for "endemic" countries with zero cases
agg_country <- data.table(aggregate(cases ~ country, df, FUN = function(x) as.integer(sum(x)))) # Sum cases by country
drop_loc <- merge(agg_country, unique(df[, c("country", "location_id", "region_name")]), by = "country", all.x = TRUE)
drop_loc <- drop_loc[cases == 0 & !(region_name %in% c("Andean Latin America", "Caribbean", "Central Latin America", "High-income North America", "Southern Latin America", "Tropical Latin America")), location_id] # Retrieve location_ids to drop

df <- df[!(location_id %in% drop_loc)] # Drop locations with zero reported cases
endemic_locs <- endemic_locs[which(!(endemic_locs %in% drop_loc))] # Update endemic_locs object

#' #### Check age and sample size
nrow(df[is.na(sample_size) & age_start != 0 & age_end != 99]) # Should be zero

#' Now set sample size equal to population, if sample size is missing.
df[is.na(sample_size), sample_size := population]

## ------------------------------------------------------------------------
#' #### Summarize rows by starting year
## ------------------------------------------------------------------------
table(df$year_start)

#' #### Summarize rows by country (top 10 in decreasing order)
## ------------------------------------------------------------------------
head(sort(table(df$country), decreasing = TRUE), n = 10)

#' #### Summarize rows by country and year
## ------------------------------------------------------------------------
xtabs(~ country + year_start, df)

#' #### Summarize rows by region and year
## ------------------------------------------------------------------------
xtabs(~ region_name + year_start, df)

#' #### Sum and plot total cases by year
## ------------------------------------------------------------------------
agg_yr <- data.table(aggregate(cases ~ year_start, df, FUN = function(x) as.integer(sum(x))))
ggplot(data = agg_yr) + theme_classic() + geom_line(aes(x = year_start, y = cases))

#' #### Sum cases by country (top 10 in decreasing order)
## ------------------------------------------------------------------------
agg_country <- data.table(aggregate(cases ~ country, df, FUN = function(x) as.integer(sum(x))))
agg_country[order(cases, decreasing = TRUE)][1:10,]

#' #### Sum and plot cases by country and year (top 10 in decreasing order)
## ------------------------------------------------------------------------
agg_country_yr <- data.table(aggregate(cases ~ country + year_start, df, FUN = function(x) as.integer(sum(x))))
agg_country_yr[order(cases, decreasing = TRUE)][1:10,]
ggplot(data = agg_country_yr) + theme_classic() + geom_line(aes(x = year_start, y = cases, color = country))

#' #### Sum and plot cases by region and year (top 10 in decreasing order)
## ------------------------------------------------------------------------
agg_region_yr <- data.table(aggregate(cases ~ region_name + year_start, df, FUN = function(x) as.integer(sum(x))))
agg_region_yr[order(cases, decreasing = TRUE)][1:10,]
ggplot(data = agg_region_yr) + theme_classic() + geom_line(aes(x = year_start, y = cases, color = region_name))

#' #### Sum cases by location_id and year (top 10 in decreasing order)
## ------------------------------------------------------------------------
agg_location_yr <- data.table(aggregate(cases ~ location_id + year_start, df, FUN = function(x) as.integer(sum(x))))
agg_location_yr <- data.table(merge(agg_location_yr, location_hierarchy[, c("location_id", "location_name")], by = "location_id", all.x = TRUE))
agg_location_yr[order(cases, decreasing = TRUE)][1:10,]

#' #### Calculate and plot incidence by country and year
## ------------------------------------------------------------------------
agg_country_yr_pop <- data.table(aggregate(population ~ country + year_start, df, FUN = function(x) as.integer(sum(x))))
agg_country_yr_pop <- as.data.table(merge(agg_country_yr, agg_country_yr_pop, by = c("country", "year_start"), all = TRUE))
agg_country_yr_pop[, incidence := cases / population]
agg_country_yr_pop[order(incidence, decreasing = TRUE)][1:10,]
ggplot(data = agg_country_yr_pop) + theme_classic() + geom_line(aes(x = year_start, y = incidence, color = country))

#' #### Summarize rows by measure
## ------------------------------------------------------------------------
table(df$measure)

#' #### Summarize rows by case name
## ------------------------------------------------------------------------
df[case_name == "reported  Zika cases", case_name := "reported Zika cases"]
table(df$case_name)

#' # Covariates
## ------------------------------------------------------------------------
covs <- get_covariate_estimates(covariate_id = "247", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
covs <- rbind(covs, get_covariate_estimates(covariate_id = "ADDRESS", age_group_id = "22", sex_id = "3", year_id = model_years, location_id = endemic_locs, location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step))

#' #### Convert long to wide
## ------------------------------------------------------------------------
covs <- dcast(covs, location_id + year_id ~ covariate_name_short, value.var = "mean_value")
setnames(covs, "evi_2000_2012", "evi")
setnames(covs, "pop_1500mplus_prop", "pop_1500mplus")
setnames(covs, "pop_dens_over_1000_psqkm_pct", "pop_dens")
setnames(covs, "rainfall_pop_weighted", "rainfall")
setnames(covs, "sanitation_prop", "sanitation")

#' #### Merge covariate values onto data set.
## ------------------------------------------------------------------------
df <- merge(df, covs, by.x = c("location_id", "year_start"), by.y = c("location_id", "year_id"), all.x = TRUE)

#' #### Check that there are no missing covariate values.
## ------------------------------------------------------------------------
nrow(df[is.na(dengue_prob)]); nrow(df[is.na(sdi)]); nrow(df[is.na(haqi)]); nrow(df[is.na(evi)]); nrow(df[is.na(mean_temperature)]); nrow(df[is.na(pop_1500mplus)]); nrow(df[is.na(pop_dens)]); nrow(df[is.na(rainfall)]); nrow(df[is.na(sanitation)]); nrow(df[is.na(dengueOutbreaks)]); nrow(df[is.na(prop_urban)]); nrow(df[is.na(solar_rad)])

#' # Final Pre-Modeling Data Prep
## ------------------------------------------------------------------------
#' Additional steps are taken to prepare the data set for modeling.
#' #### Create a new temporal field for modeling
## ------------------------------------------------------------------------
df <- df[year_start >= 2014]

df <- df[order(location_id, year_start)] # First, order by location_id and year
df$incidence <- df$cases / df$sample_size # Calculate incidence
max_year <- df[df[, .I[which.max(incidence)], by = location_id]$V1][, c("location_id", "year_start")] # Detect earliest year with maximum incidence for a given location
agg_location <- data.table(aggregate(cases ~ location_id, df, FUN = function(x) as.integer(sum(x)))) # Sum cases within a location_id
max_year[location_id %in% agg_location[cases == 0, location_id], year_start := 2016] # Assume a peak year of 2016 for any locations with zero total reported cases across all years in data set
setnames(max_year, "year_start", "peak_year")
df <- merge(df, max_year, by = "location_id", all.x = TRUE)
df$peak_lag <- df$year_start - df$peak_year

#' #### Create prediction data set
## ------------------------------------------------------------------------
pred_set <- data.table(expand.grid("location_id" = unique(df$location_id), "year_start" = inla_years)) # Data table with every combination of location_id and year
pred_set <- merge(pred_set, gbd_population[sex_id == 3], by.x = c("location_id", "year_start"), by.y = c("location_id", "year_id"), all.x = TRUE) # Merge on population values

nrow(pred_set[is.na(population)]) # Check for missing population data
setnames(pred_set, "population", "sample_size")
pred_set$cases <- NA # Set cases to NA so that INLA returns predictions

pred_set <- merge(pred_set, location_hierarchy, by = "location_id", all.x = TRUE) # Merge to location metadata (to retrieve country code)
pred_set$country <- substr(pred_set$ihme_loc_id, 1, 3) # Create country field (ISO3)

pred_set <- merge(pred_set, max_year[, c("location_id", "peak_year")], by = "location_id", all.x = TRUE)
pred_set$peak_lag <- pred_set$year_start - pred_set$peak_year

#' Merge covariate values onto data set.
pred_set <- merge(pred_set, covs, by.x = c("location_id", "year_start"), by.y = c("location_id", "year_id"), all.x = TRUE) # Merge on covariates

pred_set <- merge(pred_set, efs[, c("location_id", "ef")], by = "location_id", all.x = TRUE)

#' #### Check that there are no missing covariate values.
## ------------------------------------------------------------------------

nrow(pred_set[is.na(dengue_prob)]); nrow(pred_set[is.na(sdi)]); nrow(pred_set[is.na(haqi)]); nrow(pred_set[is.na(evi)]); nrow(pred_set[is.na(mean_temperature)]); nrow(pred_set[is.na(pop_1500mplus)]); nrow(pred_set[is.na(pop_dens)]); nrow(pred_set[is.na(rainfall)]); nrow(pred_set[is.na(sanitation)]); nrow(pred_set[is.na(dengueOutbreaks)]); nrow(pred_set[is.na(prop_urban)]); nrow(pred_set[is.na(solar_rad)])

#' #### Summarize rows by location_id and year
## ------------------------------------------------------------------------

sum(as.integer(xtabs(~ location_id + year_start, pred_set) != 1)) # Check that all location-years are present (sum should be zero)

#' # Fit INLA models
## ------------------------------------------------------------------------

#' #### Perform VIF analysis to reduce covariate list
## ------------------------------------------------------------------------
#' We iteratively progress through covariate lists, eliminating the covariate with the highest VIF at each step, until all covariates have VIF
#' under 5.

df$log_sample_size <- log(df$sample_size)

#' First select covariates for the Americas

vif(glm(cases ~ location_id + year_start + dengue_prob + sdi + haqi + evi + mean_temperature + pop_1500mplus + pop_dens + rainfall + sanitation + prop_urban + solar_rad + offset(log_sample_size), data = df, family = poisson(link = "log")))
vif(glm(cases ~ location_id + year_start + dengue_prob + sdi + haqi + evi + mean_temperature + pop_1500mplus + rainfall + sanitation + prop_urban + solar_rad + offset(log_sample_size), data = df, family = poisson(link = "log")))
vif(glm(cases ~ location_id + year_start + dengue_prob + haqi + evi + mean_temperature + pop_1500mplus + rainfall + sanitation + prop_urban + solar_rad + offset(log_sample_size), data = df, family = poisson(link = "log")))
vif(glm(cases ~ location_id + year_start + haqi + evi + mean_temperature + pop_1500mplus + rainfall + sanitation + prop_urban + solar_rad + offset(log_sample_size), data = df, family = poisson(link = "log")))

#' #### Set up data stack
## ------------------------------------------------------------------------

stk.y <- inla.stack(data = list(y = cbind(df$cases)), A = list(1), effects = list(list(b0 = 1, location_id = df$location_id, year_start = df$year_start, year_start2 = df$year_start, year_start3 = df$year_start, pop_offset = log(df$sample_size), country = df$country, region = df$region_name, peak_lag = df$peak_lag, dengue_prob = df$dengue_prob, sdi = df$sdi, haqi = df$haqi, evi = df$evi, mean_temperature = df$mean_temperature, pop_1500mplus = df$pop_1500mplus, pop_dens = df$pop_dens, rainfall = df$rainfall, sanitation = df$sanitation, dengueOutbreaks = df$dengueOutbreaks, prop_urban = df$prop_urban, solar_rad = df$solar_rad, ef = log(df$ef))), tag = 'resp')
stk.pred <- inla.stack(data = list(y = cbind(pred_set$cases)), A = list(1), effects = list(list(b0 = 1, location_id = pred_set$location_id, year_start = pred_set$year_start, year_start2 = pred_set$year_start, year_start3 = pred_set$year_start, pop_offset = log(pred_set$sample_size), country = pred_set$country, region = pred_set$region_name, peak_lag = pred_set$peak_lag, dengue_prob = pred_set$dengue_prob, sdi = pred_set$sdi, haqi = pred_set$haqi, evi = pred_set$evi, mean_temperature = pred_set$mean_temperature, pop_1500mplus = pred_set$pop_1500mplus, pop_dens = pred_set$pop_dens, rainfall = pred_set$rainfall, sanitation = pred_set$sanitation, dengueOutbreaks = pred_set$dengueOutbreaks, prop_urban = pred_set$prop_urban, solar_rad = pred_set$solar_rad, ef = log(pred_set$ef))), tag = 'pred')
stk.all <- inla.stack(stk.y, stk.pred)

#' #### Now compare covariate sets and forms, comparing via WAIC
## ------------------------------------------------------------------------
#' Covariates are compared using negative binomial models with AR1 temporal effects by country and region, and a RW1 model on peak lag

#' Model without covariates:
form_cov_1 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "iid", group = as.integer(country) - min(as.integer(country)) + 1, control.group = list(model = "ar1")) + f(year_start2, model = "iid", group = as.integer(region) - min(as.integer(region)) + 1, control.group = list(model = "ar1")) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01))))
#' Model with fixed effects for all retained covariates:
form_cov_2 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "iid", group = as.integer(country) - min(as.integer(country)) + 1, control.group = list(model = "ar1")) + f(year_start2, model = "iid", group = as.integer(region) - min(as.integer(region)) + 1, control.group = list(model = "ar1")) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + scale(haqi) + scale(evi) + scale(mean_temperature) + scale(pop_1500mplus) + scale(rainfall) + scale(sanitation) + scale(prop_urban) + scale(solar_rad)
#' Model with RW1 model for all covariates:
form_cov_3 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "iid", group = as.integer(country) - min(as.integer(country)) + 1, control.group = list(model = "ar1")) + f(year_start2, model = "iid", group = as.integer(region) - min(as.integer(region)) + 1, control.group = list(model = "ar1")) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(haqi), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(evi), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(mean_temperature), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(pop_1500mplus), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(rainfall), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(sanitation), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(prop_urban), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(solar_rad), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01))))
#' Model with RW1 model for all covariates except rainfall, sanitation, prop_urban and solar_rad (which were modelled with fixed effects due to unrealistically-high precision):
form_cov_4 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "iid", group = as.integer(country) - min(as.integer(country)) + 1, control.group = list(model = "ar1")) + f(year_start2, model = "iid", group = as.integer(region) - min(as.integer(region)) + 1, control.group = list(model = "ar1")) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(haqi), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(evi), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(mean_temperature), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(pop_1500mplus), model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + scale(rainfall) + scale(sanitation) + scale(prop_urban) + scale(solar_rad)
#' Model with RW2 models for all covariates except rainfall, sanitation, prop_urban and solar_rad (which were modelled with fixed effects due to unrealistically-high precision):
form_cov_5 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "iid", group = as.integer(country) - min(as.integer(country)) + 1, control.group = list(model = "ar1")) + f(year_start2, model = "iid", group = as.integer(region) - min(as.integer(region)) + 1, control.group = list(model = "ar1")) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(haqi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(evi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(mean_temperature), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(pop_1500mplus), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + scale(rainfall) + scale(sanitation) + scale(prop_urban) + scale(solar_rad)

#' Fit covariate models:
model_fit_cov_1 <- inla(form_cov_1, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb", h = 1e-3), control.compute = list(config = FALSE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
model_fit_cov_2 <- inla(form_cov_2, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb", h = 1e-3), control.compute = list(config = FALSE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
model_fit_cov_3 <- inla(form_cov_3, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb", h = 1e-1), control.compute = list(config = FALSE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
model_fit_cov_4 <- inla(form_cov_4, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb", h = 1e-3), control.compute = list(config = FALSE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
model_fit_cov_5 <- inla(form_cov_5, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb"), control.compute = list(config = FALSE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)

#' Compare WAIC of covariate models:
data.table(model = c(1:5), waic = c(model_fit_cov_1$waic$waic, model_fit_cov_2$waic$waic, model_fit_cov_3$waic$waic, model_fit_cov_4$waic$waic, model_fit_cov_5$waic$waic))[order(waic)]

#' Covariate model 5 had the best WAIC and was chosen as the final covariate model after review of its covariate effect plots.
summary(model_fit_cov_5)

#' #### Now compare different temporal models using the selected covariate sets.
## ------------------------------------------------------------------------
#' #### Fit full models
form_1 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "iid", group = as.integer(country) - min(as.integer(country)) + 1, control.group = list(model = "ar1")) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(haqi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(evi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(mean_temperature), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(pop_1500mplus), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + scale(rainfall) + scale(sanitation) + scale(prop_urban) + scale(solar_rad)
form_2 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "iid", group = as.integer(country) - min(as.integer(country)) + 1, control.group = list(model = "ar1")) + f(year_start2, model = "iid", group = as.integer(region) - min(as.integer(region)) + 1, control.group = list(model = "ar1")) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(haqi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(evi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(mean_temperature), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(pop_1500mplus), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + scale(rainfall) + scale(sanitation) + scale(prop_urban) + scale(solar_rad)
form_3 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(year_start, model = "ar1") + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(haqi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(evi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(mean_temperature), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(pop_1500mplus), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + scale(rainfall) + scale(sanitation) + scale(prop_urban) + scale(solar_rad)
form_4 <- y ~ 0 + b0 + f(as.factor(location_id), model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(peak_lag, model = "rw1", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(haqi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(evi), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(mean_temperature), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + f(inla.group(pop_1500mplus), model = "rw2", scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01)))) + scale(rainfall) + scale(sanitation) + scale(prop_urban) + scale(solar_rad)

#' Run INLA models
model_fit1 <- inla(form_1, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb"), control.compute = list(config = TRUE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
model_fit2 <- inla(form_2, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb"), control.compute = list(config = TRUE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
model_fit3 <- inla(form_3, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb", h = 1e-3), control.compute = list(config = TRUE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
model_fit4 <- inla(form_4, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb"), control.compute = list(config = TRUE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)

#' Compare WAIC among models (sorted by best to worst WAIC)
data.table(model = c(1:4), waic = c(model_fit1$waic$waic, model_fit2$waic$waic, model_fit3$waic$waic, model_fit4$waic$waic))[order(waic)]

#' Model 2 had the best WAIC score, and was selected as the final model after review of predictions from all four models.
summary(model_fit2)

#' Plot haqi
haqi_results <- model_fit2$summary.random$`inla.group(haqi)`
setnames(haqi_results, c("0.025quant", "0.5quant", "0.975quant"), c("lower", "median", "upper"))
ggplot(data = haqi_results) + theme_classic() + geom_line(aes(x = ID, y = mean)) + geom_ribbon(aes(x = ID, ymin = lower, ymax = upper), alpha = 0.05) + geom_abline(intercept = 0, slope = 0, linetype = "dotted") + ggtitle(label = paste0("Random Effect for HAQI"), subtitle = "Mean (95% UI)") + xlab("HAQI") + ylab("Random Effect (log)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

#' Plot evi
evi_results <- model_fit2$summary.random$`inla.group(evi)`
setnames(evi_results, c("0.025quant", "0.5quant", "0.975quant"), c("lower", "median", "upper"))
ggplot(data = evi_results) + theme_classic() + geom_line(aes(x = ID, y = mean)) + geom_ribbon(aes(x = ID, ymin = lower, ymax = upper), alpha = 0.05) + geom_abline(intercept = 0, slope = 0, linetype = "dotted") + ggtitle(label = paste0("Random Effect for EVI"), subtitle = "Mean (95% UI)") + xlab("EVI") + ylab("Random Effect (log)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

#' Plot temperature effect
temperature_results <- model_fit2$summary.random$`inla.group(mean_temperature)`
setnames(temperature_results, c("0.025quant", "0.5quant", "0.975quant"), c("lower", "median", "upper"))
ggplot(data = temperature_results) + theme_classic() + geom_line(aes(x = ID, y = mean)) + geom_ribbon(aes(x = ID, ymin = lower, ymax = upper), alpha = 0.05) + geom_abline(intercept = 0, slope = 0, linetype = "dotted") + ggtitle(label = paste0("Random Effect for Mean Temperature"), subtitle = "Mean (95% UI)") + xlab("Temperature (°C)") + ylab("Random Effect (log)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

#' Plot pop_1500mplus effect
pop_1500mplus_results <- model_fit2$summary.random$`inla.group(pop_1500mplus)`
setnames(pop_1500mplus_results, c("0.025quant", "0.5quant", "0.975quant"), c("lower", "median", "upper"))
ggplot(data = pop_1500mplus_results) + theme_classic() + geom_line(aes(x = ID, y = mean)) + geom_ribbon(aes(x = ID, ymin = lower, ymax = upper), alpha = 0.05) + geom_abline(intercept = 0, slope = 0, linetype = "dotted") + ggtitle(label = paste0("Random Effect for Proportion of Population Living Above 1500m"), subtitle = "Mean (95% UI)") + xlab("Proportion of Population Living Above 1500m") + ylab("Random Effect (log)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

#' # Derive and Plot Model Predictions
#' #### Obtain predictions (mean and 95% UI)
## ------------------------------------------------------------------------
index.pred <- inla.stack.index(stk.all, tag = "pred")$data

preds <- cbind(pred_set, model_fit1$summary.fitted.values[index.pred,])
preds2 <- cbind(pred_set, model_fit2$summary.fitted.values[index.pred,])
preds3 <- cbind(pred_set, model_fit3$summary.fitted.values[index.pred,])
preds4 <- cbind(pred_set, model_fit4$summary.fitted.values[index.pred,])

#' #### Rename columns for easier plotting
## ------------------------------------------------------------------------

setnames(preds, c("mean", "0.025quant", "0.5quant", "0.975quant"), c("mean_pred", "lower_pred", "median_pred", "upper_pred"))
setnames(preds2, c("mean", "0.025quant", "0.5quant", "0.975quant"), c("mean_pred", "lower_pred", "median_pred", "upper_pred"))
setnames(preds3, c("mean", "0.025quant", "0.5quant", "0.975quant"), c("mean_pred", "lower_pred", "median_pred", "upper_pred"))
setnames(preds4, c("mean", "0.025quant", "0.5quant", "0.975quant"), c("mean_pred", "lower_pred", "median_pred", "upper_pred"))

#' #### Merge training data with predictions
## ------------------------------------------------------------------------

obs_vs_preds <- data.table(merge(df, preds[, c("location_id", "year_start", "mean_pred", "lower_pred", "median_pred", "upper_pred")], by = c("location_id", "year_start"), all.x = TRUE))
obs_vs_preds2 <- data.table(merge(df, preds2[, c("location_id", "year_start", "mean_pred", "lower_pred", "median_pred", "upper_pred")], by = c("location_id", "year_start"), all.x = TRUE))
obs_vs_preds3 <- data.table(merge(df, preds3[, c("location_id", "year_start", "mean_pred", "lower_pred", "median_pred", "upper_pred")], by = c("location_id", "year_start"), all.x = TRUE))
obs_vs_preds4 <- data.table(merge(df, preds4[, c("location_id", "year_start", "mean_pred", "lower_pred", "median_pred", "upper_pred")], by = c("location_id", "year_start"), all.x = TRUE))

#' #### Plot observed vs. predicted incidence
## ------------------------------------------------------------------------

ggplot(obs_vs_preds) + theme_classic() + geom_point(aes(x = cases / sample_size, y = mean_pred / sample_size, color = country)) + geom_smooth(aes(x = cases / sample_size, y = mean_pred / sample_size), size = 0.5) + geom_abline(intercept = 0, slope = 1) + ggtitle(label = paste0("Observed vs. Predicted Incidence, Model 1")) + xlab("Observed Incidence") + ylab("Predicted Incidence (mean)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
ggplot(obs_vs_preds2) + theme_classic() + geom_point(aes(x = cases / sample_size, y = mean_pred / sample_size, color = country)) + geom_smooth(aes(x = cases / sample_size, y = mean_pred / sample_size), size = 0.5) + geom_abline(intercept = 0, slope = 1) + ggtitle(label = paste0("Observed vs. Predicted Incidence, Model 2")) + xlab("Observed Incidence") + ylab("Predicted Incidence (mean)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
ggplot(obs_vs_preds3) + theme_classic() + geom_point(aes(x = cases / sample_size, y = mean_pred / sample_size, color = country)) + geom_smooth(aes(x = cases / sample_size, y = mean_pred / sample_size), size = 0.5) + geom_abline(intercept = 0, slope = 1) + ggtitle(label = paste0("Observed vs. Predicted Incidence, Model 3")) + xlab("Observed Incidence") + ylab("Predicted Incidence (mean)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
ggplot(obs_vs_preds4) + theme_classic() + geom_point(aes(x = cases / sample_size, y = mean_pred / sample_size, color = country)) + geom_smooth(aes(x = cases / sample_size, y = mean_pred / sample_size), size = 0.5) + geom_abline(intercept = 0, slope = 1) + ggtitle(label = paste0("Observed vs. Predicted Incidence, Model 4")) + xlab("Observed Incidence") + ylab("Predicted Incidence (mean)") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

#' #### Combine predictions for all models
## ------------------------------------------------------------------------

preds$model <- 1
preds2$model <- 2
preds3$model <- 3
preds4$model <- 4

preds_all <- copy(preds)
preds_all <- rbind(preds_all, preds2)
preds_all <- rbind(preds_all, preds3)
preds_all <- rbind(preds_all, preds4)

preds_all$model <- as.factor(preds_all$model)

#' #### Get current GBD estimates for comparison
## ------------------------------------------------------------------------
results <- get_model_results('epi', location_set_id=35, location_id = endemic_locs, age_group_id = 22, year_id = model_years[which(model_years >= 2010)], sex_id=c(1, 2), model_version_id = ADDRESS, gbd_round_id=(gbd_round_id - 1), measure = 6, decomp_step=decomp_step)
results$model <- "GBD 2023"

#' #### Plot time series by model and location
## ------------------------------------------------------------------------
preds_all <- preds_all[!(model %in% c())]

for (i in unique(preds$location_id)) {
  g <- ggplot() + theme_classic() + coord_cartesian(xlim = c(2014, 2019)) +
    geom_ribbon(data = results[location_id == i & sex_id == 2], aes(x = year_id, ymin = lower, ymax = upper, fill = model), alpha = 0.05) +
    geom_ribbon(data = preds_all[location_id == i], aes(x = year_start, ymin = lower_pred / sample_size, ymax = upper_pred / sample_size, group = model, fill = model), alpha = 0.05) +
    geom_line(data = preds_all[location_id == i], aes(x = year_start, y = mean_pred / sample_size, group = model, color = model)) +
    geom_line(data = results[location_id == i & sex_id == 2], aes(x = year_id, y = mean, color = model), size = 0.5) +
    geom_point(data = df[location_id == i], aes(x = year_start, y = cases_original / sample_size)) +
    geom_point(data = df[location_id == i], aes(x = year_start, y = cases / sample_size), fill = "white", color = 1, alpha = 0.5, pch = 21) +
    ggtitle(label = paste0("Acute Symptomatic Zika Incidence, ", location_hierarchy[location_id == i, location_name], " (", location_hierarchy[location_id == i, ihme_loc_id], ", ", i, ")"), subtitle = "Mean and 95% UI. Reported: Solid points. Adjusted: Open points.") + xlab("Year") + ylab("Incidence") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  print(g)
}

#' Now plot predictions just from the final model (model 2)
## ------------------------------------------------------------------------
preds_all <- preds_all[model %in% c("2")]

for (i in unique(preds$location_id)) {
  g <- ggplot() + theme_classic() + coord_cartesian(xlim = c(2014, 2022)) +
    geom_ribbon(data = results[location_id == i & sex_id == 2], aes(x = year_id, ymin = lower, ymax = upper, fill = model), alpha = 0.05) +
    geom_ribbon(data = preds_all[location_id == i], aes(x = year_start, ymin = lower_pred / sample_size, ymax = upper_pred / sample_size, group = model, fill = model), alpha = 0.05) + 
    # geom_line(data = preds_all[location_id == i], aes(x = year_start, y = mean_pred / sample_size, group = model, color = model)) +
    geom_line(data = preds_all[location_id == i], aes(x = year_start, y = median_pred / sample_size, group = model, color = model)) +
    geom_line(data = results[location_id == i & sex_id == 2], aes(x = year_id, y = mean, color = model), size = 0.5) +
    geom_point(data = df[location_id == i], aes(x = year_start, y = cases_original / sample_size)) +
    geom_point(data = df[location_id == i], aes(x = year_start, y = cases / sample_size), fill = "white", color = 1, alpha = 0.5, pch = 21) +
    ggtitle(label = paste0("Acute Symptomatic Zika Incidence, ", location_hierarchy[location_id == i, location_name], " (", location_hierarchy[location_id == i, ihme_loc_id], ", ", i, ")"), subtitle = "Mean and 95% UI. Reported: Solid points. Adjusted: Open points.") + xlab("Year") + ylab("Incidence") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  print(g)
}

#' #### Decision for final model
## ------------------------------------------------------------------------
#' Model 2 had the best WAIC, relatively low bias, and good behavior
final_model <- copy(model_fit2)

#' #### Obtain draws from model posterior
## ------------------------------------------------------------------------
draw_object <- inla.posterior.sample(1000, final_model, intern = FALSE, seed = random.seed)

draws <- copy(pred_set[, c("location_id", "year_start")])
for (i in 1:length(draw_object)) {
  draws <- cbind(draws, exp(draw_object[[i]]$latent[index.pred])) # Exponentiate and combine linear predictor draws
}

colnames(draws)[3:1002] <- paste0("draw_", 0:999) # Rename draw columns
setnames(draws, "year_start", "year_id")

#' #### Plot posterior median and draw median (should be close)
## ------------------------------------------------------------------------
ggplot(data = data.table(cbind(pred_median = preds_all$median_pred, draw_median = rowQuantiles(as.matrix(draws[, 3:1002]), probs = 0.5)))) + theme_classic() + geom_point(aes(x = pred_median, y = draw_median)) + geom_abline(intercept = 0, slope = 1) + geom_smooth(aes(x = pred_median, y = draw_median))

#' #### Add draws for 2020-2022
## ------------------------------------------------------------------------
append <- data.table()
for (loc in unique(draws$location_id)) {
  temp_set <- copy(draws[location_id == loc & year_id == 2019])
  for (year in 2020:2022) {
    temp_set$year_id <- year
    temp_set[, c(paste0("draw_", 0:999))] <- temp_set[, c(paste0("draw_", 0:999))] * 0.7
    append <- rbind(append, temp_set)
  }
}

draws <- rbindlist(list(draws, append), use.names = TRUE, fill = TRUE)

#' #### Produce draws for endemic locations outside the Americas
## ------------------------------------------------------------------------
#' Given the sparsity of data outside the Americas, so generate random Poisson draws from the observed case counts.

df_non_americas <- df_non_americas[cases > 0, c("location_id", "year_start", "sex_id", "age_group_id", "cases")] # Restrict to rows with any cases
setnames(df_non_americas, "year_start", "year_id")
draws_non_americas <- data.table()
for (i in 1:nrow(df_non_americas)) {
  temp_draws <- cbind(df_non_americas[i,], t(rpois(1000, df_non_americas[i, cases])))
  draws_non_americas <- rbind(draws_non_americas, temp_draws)
}
rm(temp_draws)
setnames(draws_non_americas, paste0("V", 1:1000), paste0("draw_", 0:999))
draws_non_americas[, c("sex_id", "age_group_id", "cases") := NULL]

#' Produce zero-case draw rows for these locations for years without data.
empty_draws <- data.table()
for (loc in unique(draws_non_americas$location_id)) {
  temp_draws <- expand.grid("location_id" = loc, "year_id" = c(inla_years, 2020:2022)[which(!(c(inla_years, 2020:2022) %in% unique(draws_non_americas[location_id == loc, year_id])))])
  temp_draws[, c(paste0("draw_", 0:999))] <- 0
  empty_draws <- rbind(empty_draws, temp_draws)
}

#' Append non-Americas draws to Americas draws
draws <- rbindlist(list(draws, draws_non_americas, empty_draws), use.names = TRUE)
draws <- merge(draws, gbd_population[sex_id == 3], by = c("location_id", "year_id"), all.x = TRUE) # First, merge population
draws[, c(paste0("draw_", 0:999))] <- draws[, c(paste0("draw_", 0:999))] / draws$population # Now convert to incidence
draws[, c("age_group_id", "sex_id", "population", "run_id") := NULL] # Drop columns to avoid later conflicts

#' #### Replicate draws by sex and age IDs
## ------------------------------------------------------------------------
age_group_ids <- c(2:3, 6:20, 22, 30:32, 34, 235, 238, 388:389)
sex_ids <- c(1:2)

expansion <- expand.grid("location_id" = unique(draws$location_id), "age_group_id" = age_group_ids, "sex_id" = sex_ids)
draws <- merge(draws, expansion, by = "location_id", allow.cartesian = TRUE)
rm(expansion)

#' #### Merge population onto draws
## ------------------------------------------------------------------------

age_sex_populations <- get_population(age_group_id = age_group_ids, sex_id = sex_ids, year_id = model_years, location_id = unique(draws$location_id), location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step, run_id = pop_run_id)
draws <- merge(draws, age_sex_populations, by = c("location_id", "year_id", "age_group_id", "sex_id"), all.x = TRUE)

#' #### Retrieve and process age-stratified incidence data
## ------------------------------------------------------------------------
age_data <- as.data.table(read_xlsx("FILEPATH"))
age_data[, age_midpoint := floor((as.integer(age_start) + as.integer(age_end)) / 2)] # Calculate age midpoint (not population-weighted)
age_data$group <- as.integer(as.factor(paste0(age_data$year_start, "_", age_data$value_subsite, "_", age_data$value_filepath)))
groups <- as.data.table(table(age_data$group)) # Count rows per group
age_data <- age_data[group %in% groups[N > 1, V1]] # Drop groups with only a single row

age_data <- age_data[(value_case / sample_size) < 0.1] # Drop outlier value above 0.1 (for consistency with previous model version)
age_data <- age_data[!is.na(value_autochthonous_confirmed)] # Restrict to confirmed autochthonous cases
age_data <- age_data[sex %in% c(1, 2)] # Restrict to male or female sex (not "both")

#' #### Produce prediction data set for age-sex model
## ------------------------------------------------------------------------
# get_ids("age_group_set") # Get age group set IDs to identify the appropriate ID for GBD 2020
age_metadata <- get_age_metadata(age_group_set_id = 19, gbd_round_id = gbd_round_id) # Get age metadata for GBD 2020
age_metadata[, age_midpoint := (age_group_years_start + age_group_years_end) / 2] # Calculate a simple age midpoint (doesn't account for population structure)

#' Create prediction set
pred_set_age <- data.table(expand.grid("age_group_id" = unique(age_metadata$age_group_id), "sex_id" = c(1, 2))) # Data table with every combination of age_group_id and sex_id
pred_set_age <- merge(pred_set_age, age_metadata, by = "age_group_id", all.x = TRUE) # Merge onto age metadata

#' Create data stack
stk.y.age <- inla.stack(data = list(y = as.integer(age_data$value_autochthonous_confirmed)), A = list(1), effects = list(list(b0 = 1, group = as.factor(age_data$group), pop_offset = log(as.numeric(age_data$sample_size)), age_midpoint = age_data$age_midpoint, sex = age_data$sex)), tag = 'resp_age')
stk.pred.age <- inla.stack(data = list(y = NA), A = list(1), effects = list(list(b0 = 1, group = NA, pop_offset = log(1), age_midpoint = pred_set_age$age_midpoint, sex = pred_set_age$sex_id)), tag = 'pred_age')
stk.all.age <- inla.stack(stk.y.age, stk.pred.age)

#' #### Fit age-sex model
## ------------------------------------------------------------------------
#' The age-sex model consists of group random effects, separate RW1 age models by sex, and sex fixed effects.
form_age_sex <- y ~ 0 + b0 + f(group, model = "iid") + f(pop_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(age_midpoint, model = "rw1", replicate = as.integer(sex), scale.model = TRUE, hyper = list(theta = list(prior="pc.prec", param=c(1, 0.01))))
model_age_sex <- inla(form_age_sex, family = c("nbinomial"), data = inla.stack.data(stk.all.age), control.predictor = list(A = inla.stack.A(stk.all.age), link = 1), control.inla = list(int.strategy = "eb"), control.compute = list(config = TRUE, waic = TRUE, openmp.strategy = "default", smtp = "default"), verbose = TRUE, num.threads=1)
summary(model_age_sex)

#' Obtain predictions by age and sex
index.pred.age <- inla.stack.index(stk.all.age, tag = "pred_age")$data
preds_age <- cbind(pred_set_age, model_age_sex$summary.fitted.values[index.pred.age,])
setnames(preds_age, c("0.025quant", "0.5quant", "0.975quant"), c("lower", "median", "upper"))

#' Plot predictions by age and sex
gg <- ggplot(preds_age) + theme_classic() + geom_line(aes(x = age_midpoint, y = mean, group = as.factor(sex_id), color = as.factor(sex_id))) + 
  geom_ribbon(aes(x = age_midpoint, ymin = lower, ymax = upper, group = as.factor(sex_id), fill = as.factor(sex_id)), alpha = 0.05) + 
  ggtitle(label = "Zika Relative Incidence by Age and Sex, GBD 2020", subtitle = "Mean and 95% UI. Y-axis values are unscaled") +
  xlab("Age Midpoint (years)") + ylab("Relative Incidence") +
  theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
print(gg)

#' Retrieve age curve from GBD 2019 model
previousAgeDistribution <- as.data.table(read.dta13("FILEPATH"))
age_metadata_2019 <- get_age_metadata(age_group_set_id = 12, gbd_round_id = (gbd_round_id - 1))
previousAgeDistribution <- merge(previousAgeDistribution, age_metadata_2019, by = "age_group_id", all.x = TRUE)
previousAgeDistribution[, age_midpoint := (age_group_years_start + age_group_years_end) / 2]
previousAgeDistribution[, lower := ageSexCurve - 1.96 * ageSexCurveSe]
previousAgeDistribution[, upper := ageSexCurve + 1.96 * ageSexCurveSe]

#' Plot GBD 2019 age model
gg <- ggplot(previousAgeDistribution) + theme_classic() + geom_line(aes(x = age_midpoint, y = exp(ageSexCurve), group = as.factor(sex_id), color = as.factor(sex_id))) + 
  geom_ribbon(aes(x = age_midpoint, ymin = exp(lower), ymax = exp(upper), group = as.factor(sex_id), fill = as.factor(sex_id)), alpha = 0.05) +
  ggtitle(label = "Zika Relative Incidence by Age and Sex, GBD 2019", subtitle = "Mean and 95% UI. Y-axis values are unscaled") +
  xlab("Age Midpoint (years)") + ylab("Relative Incidence") +
  theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
print(gg)

#' #### Apply sex and age curves to incidence draws
## ------------------------------------------------------------------------
#' First, generate age-sex draws
draws_object_age_sex <- inla.posterior.sample(1000, model_age_sex, intern = FALSE, seed = random.seed)

draws_age_sex <- copy(pred_set_age[, c("age_group_id", "sex_id")])
for (i in 1:length(draws_object_age_sex)) {
  draws_age_sex <- cbind(draws_age_sex, exp(draws_object_age_sex[[i]]$latent[index.pred.age])) # Exponentiate and combine linear predictor draws
}

colnames(draws_age_sex)[3:1002] <- paste0("draw_", 0:999) # Rename draw columns

#' Merge total populations onto draws
gbd_population_temp <- copy(gbd_population[sex_id == 3])
setnames(gbd_population_temp, "population", "total_population")
draws <- merge(draws, gbd_population_temp[, c("location_id", "year_id", "total_population")], by = c("location_id", "year_id"), all.x = TRUE)
draws[, pop_prop := population / total_population] # Calculate proportion of total population represented by each age-sex (in a given location-year)

#' Merge sex-specific population onto draws
all_age_pop_by_sex <- get_population(age_group_id = 22, sex_id = c(1, 2), year_id = model_years, location_id = unique(draws$location_id), location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step, run_id = pop_run_id)
setnames(all_age_pop_by_sex, "population", "total_population_by_sex")
draws <- merge(draws, all_age_pop_by_sex[, c("location_id", "year_id", "sex_id", "total_population_by_sex")], by = c("location_id", "year_id", "sex_id"), all.x = TRUE)
draws[, pop_prop_sex := population / total_population_by_sex] # Calculate proportion of total sex-specific population represented by each age-sex (in a given location-year)

#' Apply age-sex model to incidence draws
draws_working <- copy(draws) # Create working copy of draws
draws_age_sex_working <- copy(draws_age_sex) # Create working copy of age-sex draws
adjusted_draws <- data.table() # Empty data table to which age- and sex-adjusted draws will be appended

for (loc in unique(draws_working$location_id)) { # Loop through locations
  temp <- draws_working[location_id == loc]
  for (y in unique(temp$year_id)) { # Loop through years
    temp_year <- temp[year_id == y]
    age_sex_current <- merge(draws_age_sex_working, temp_year[, c("age_group_id", "sex_id", "pop_prop", "pop_prop_sex")], by = c("age_group_id", "sex_id")) # Merge on population prop. (this intentionally drops age_group_id 22, "all ages")
    age_sex_current[, c(paste0("draw_", 0:999))] <- age_sex_current[, c(paste0("draw_", 0:999))] * age_sex_current$pop_prop # Multiply raw age-sex prevalence by population prop.
    summed <- age_sex_current[, lapply(.SD, sum, na.rm=TRUE), .SDcols = c(paste0("draw_", 0:999))] # Calculate age-sex prevalence, weighted by population
    scaling <- temp_year[age_group_id == 22 & sex_id == 1, c(paste0("draw_", 0:999))] / summed # Calculate draw-level scaling
    age_sex_current[, c(paste0("draw_", 0:999))] <- sweep(draws_age_sex_working[, c(paste0("draw_", 0:999))], MARGIN = 2, as.matrix(scaling), `*`) # Apply scaling
    
    # Create aggregate all-age draws by sex 
    age_group_22 <- copy(age_sex_current) 
    age_group_22[, c(paste0("draw_", 0:999))] <- age_group_22[, c(paste0("draw_", 0:999))] * age_group_22$pop_prop_sex
    age_group_22 <- age_group_22[, lapply(.SD, sum, na.rm=TRUE), .SDcols = c(paste0("draw_", 0:999)), by = sex_id] # Calculate all-age prevalence by sex
    age_group_22$age_group_id <- 22
    
    age_sex_current <- rbindlist(list(age_sex_current, age_group_22), use.names = TRUE, fill = TRUE) # Append all-age draws
    age_sex_current$year_id <- y # Add year_id
    age_sex_current$location_id <- loc # Add location_id
    adjusted_draws <- rbindlist(list(adjusted_draws, age_sex_current), use.names = TRUE, fill = TRUE) # Append to adjusted_draws object
  }
}

draws <- copy(adjusted_draws)
rm(adjusted_draws) # Some cleanup
rm(draws_working) # Some cleanup

#' #### Add zero draws for pre-2014 years
## ------------------------------------------------------------------------
#' Create 0-incidence draws for years prior to 2014.
expansion <- as.data.table(expand.grid("location_id" = unique(draws$location_id), "year_id" = model_years[which(!(model_years %in% draws$year_id) & model_years < 2014)], "age_group_id" = age_group_ids, "sex_id" = sex_ids))
expansion[, c(paste0("draw_", 0:999))] <- 0 # Set incidence to 0 for years prior to 2014
draws <- rbindlist(list(draws, expansion), use.names = TRUE, fill = TRUE)
rm(expansion)

#' #### Plot time series with draw-level summaries
## ------------------------------------------------------------------------
draws$mean <- rowMeans(as.matrix(draws[, c(paste0("draw_", 0:999))]))
draws$lower <- rowQuantiles(as.matrix(draws[, c(paste0("draw_", 0:999))]), probs = c(0.025))
draws$upper <- rowQuantiles(as.matrix(draws[, c(paste0("draw_", 0:999))]), probs = c(0.975))

df_non_americas <- merge(df_non_americas, gbd_population[age_group_id == 22 & sex_id == 3], by = c("location_id", "year_id", "sex_id", "age_group_id"), all.x = TRUE)
setnames(df_non_americas, "population", "sample_size")

#' Plot all-age incidence of acute Zika by sex, for 2008-2022
draws$model <- "GBD 2020"
for (i in unique(draws$location_id)) {
  g <- ggplot() + theme_classic() + coord_cartesian(xlim = c(2008, 2022)) +
    geom_ribbon(data = results[location_id == i], aes(x = year_id, ymin = lower, ymax = upper, group = as.factor(sex_id), fill = model), alpha = 0.05) +
    geom_ribbon(data = draws[location_id == i & age_group_id == 22], aes(x = year_id, ymin = lower, ymax = upper, group = as.factor(sex_id), fill = model), alpha = 0.05) +
    geom_line(data = draws[location_id == i & age_group_id == 22], aes(x = year_id, y = mean, group = as.factor(sex_id), linetype = as.factor(sex_id), color = model)) +
    geom_line(data = results[location_id == i], aes(x = year_id, y = mean, group = as.factor(sex_id), linetype = as.factor(sex_id), color = model), size = 0.5) +
    geom_point(data = df[location_id == i], aes(x = year_start, y = cases_original / sample_size)) +
    geom_point(data = df[location_id == i], aes(x = year_start, y = cases / sample_size), fill = "white", color = 1, alpha = 0.5, pch = 21) +
    geom_point(data = df_non_americas[location_id == i], aes(x = year_id, y = cases / sample_size), fill = "white", color = 1, alpha = 0.5, pch = 21) +
    ggtitle(label = paste0("Acute Symptomatic Zika Incidence, ", location_hierarchy[location_id == i, location_name], " (", location_hierarchy[location_id == i, ihme_loc_id], ", ", i, ")"), subtitle = "Mean and 95% UI. All-age incidence by sex.\nReported: Solid points. Adjusted: Open points.") + xlab("Year") + ylab("Incidence") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  print(g)
}

#' #### Create zero-draw template for non-endemic locations
## ------------------------------------------------------------------------
locs <- location_hierarchy[most_detailed == 1] # Get all most-detailed locations
non_endemic <- locs[!(location_id %in% unique(draws$location_id)), location_id] # Identify location_ids lacking draws

zero_draws <- as.data.table(expand.grid("location_id" = 0, "year_id" = model_years, "age_group_id" = age_group_ids, "sex_id" = sex_ids))
zero_draws[, c(paste0("draw_", 0:999))] <- 0 # Set incidence to 0
zero_draws[, c("modelable_entity_id", "measure_id") := as.integer(NA)] # Set up template
zero_draws <- zero_draws[, c("modelable_entity_id", "measure_id", "location_id", "year_id", "sex_id", "age_group_id", paste0("draw_", 0:999))] # Set column order
zero_draws <- zero_draws[age_group_id != 22 & year_id %in% output_years] # Drop all-age draws

#' #### Prep draws object
## ------------------------------------------------------------------------
#' First we prepare the draws object as a template for modelable entity-specific draws.
draws[, c("pop_prop", "pop_prop_sex", "mean", "lower", "upper", "model") := NULL] # Drop extraneous columns
draws[, c("modelable_entity_id", "measure_id") := as.integer(NA)] # Set up template
draws <- draws[, c("modelable_entity_id", "measure_id", "location_id", "year_id", "sex_id", "age_group_id", paste0("draw_", 0:999))]

#' #### Create output directory for draws
## ------------------------------------------------------------------------
save_path <- "FILEPATH"

## ---- eval = FALSE ---------------------
dir.create(save_path, showWarnings = TRUE)
#'

#' #### Prepare incidence draws for ME 10401
## ------------------------------------------------------------------------
draws_10401_inc <- copy(draws) # Create a new draws copy for ME 10401
draws_10401_inc[, c("modelable_entity_id", "measure_id") := list(10401, 6)] # all Zika infections, incidence
draws_10401_inc <- draws_10401_inc[age_group_id != 22] # Drop all-age draws

draws_10401_prev <- copy(draws_10401_inc) # Copy incidence draws
draws_10401_prev[, c(paste0("draw_", 0:999))] <- draws_10401_prev[, c(paste0("draw_", 0:999))] * 6/365 # Assume a duration of 6 days
draws_10401_prev[, measure_id := 5] # Prevalence

save_path <- "FILEPATH"
## ---- eval = FALSE ---------------------
dir.create(save_path, showWarnings = TRUE)
message(paste0("Draws being saved in ", save_path))
#'

#' Append incidence and prevalence draws.
draws_10401_appended <- rbindlist(list(draws_10401_inc, draws_10401_prev), use.names = TRUE, fill = TRUE)

#' Now save draws to disk.
## ---- eval = FALSE ---------------------
for (loc in unique(draws_10401_appended$location_id)) {
  # NOTE: Uncomment the line below to save new draws. Otherwise, leave this commented out to avoid overwriting outputs during rendering.
  write.csv(draws_10401_appended[location_id == loc & year_id %in% output_years,], "FILEPATH") # Save draws by location
}
#'

#' Now save draws for non-endemic locations to disk.
zero_draws_10401_prev <- copy(zero_draws)
zero_draws_10401_prev[, c("modelable_entity_id", "measure_id") := list(ADDRESS, 5)]
zero_draws_10401_inc <- copy(zero_draws_10401_prev)
zero_draws_10401_inc$measure_id <- 6
zero_draws_10401 <- rbindlist(list(zero_draws_10401_inc, zero_draws_10401_prev), use.names = TRUE, fill = TRUE)
## ---- eval = FALSE ---------------------
for (loc in unique(non_endemic)) {
  zero_draws_10401$location_id <- loc
  write.csv(zero_draws_10401, "FILEPATH", row.names = FALSE) # Save draws by location
}
#'

#' #### Retrieve outcomes summary file
## ------------------------------------------------------------------------
#' We first retrieve the outcomes file from an earlier GBD round. We will use these values for now.
outcomes <- as.data.table(read.dta13("FILEPATH"))

#' #### Generate draws for MEID 11028
## ------------------------------------------------------------------------
#' To generate draws for MEID 11028 (asymptomatic Zika), we first copy the incidence and prevalence draws objects from MEID 10401
#' (symptomatic acute).
draws_11028_inc <- copy(draws_10401_inc) # Copy incidence draws
draws_11028_inc$modelable_entity_id <- ADDRESS
draws_11028_prev <- copy(draws_10401_prev) # Copy prevalence draws
draws_11028_prev$modelable_entity_id <- ADDRESS

#' Now loop through draws and adjust using GBD 2019 approach indicated above.
alpha <- outcomes[modelable_entity_id == 11028, alpha] # Get alpha for 11028
beta <- outcomes[modelable_entity_id == 11028, beta] # Get beta for 11028
divisors <- rbeta(1000, alpha, beta) # Generate 1000 random draws from the beta distribution

draws_11028_inc[, c(paste0("draw_", 0:999))] <- sweep(draws_11028_inc[, c(paste0("draw_", 0:999))], MARGIN = 2, as.matrix(divisors), `/`) # Apply divisors
draws_11028_prev[, c(paste0("draw_", 0:999))] <- sweep(draws_11028_prev[, c(paste0("draw_", 0:999))], MARGIN = 2, as.matrix(divisors), `/`) # Apply divisors

draws_11028_inc[, c(paste0("draw_", 0:999))] <- draws_11028_inc[, c(paste0("draw_", 0:999))] - draws_10401_inc[, c(paste0("draw_", 0:999))] # Subtract acute symptomatic incidence
draws_11028_prev[, c(paste0("draw_", 0:999))] <- draws_11028_prev[, c(paste0("draw_", 0:999))] - draws_10401_prev[, c(paste0("draw_", 0:999))] # Subtract acute symptomatic prevalence

save_path <- "FILEPATH"
## ---- eval = FALSE ---------------------
dir.create(save_path, showWarnings = TRUE)
#'
message(paste0("Draws being saved in ", save_path))

#' Append incidence and prevalence draws.
draws_11028_appended <- rbindlist(list(draws_11028_inc, draws_11028_prev), use.names = TRUE, fill = TRUE)

#' Now save incidence and prevalence draws to disk.
## ---- eval = FALSE ---------------------
for (loc in unique(draws_11028_appended$location_id)) {
  # NOTE: Uncomment the two lines below to save new draws. Otherwise, leave this commented out to avoid overwriting outputs during rendering.
  write.csv(draws_11028_appended[location_id == loc & year_id %in% output_years,], "FILEPATH", row.names = FALSE) # Save draws by location
}
#'

#' Now save incidence and prevalence draws for non-endemic locations to disk.
zero_draws_11028_prev <- copy(zero_draws)
zero_draws_11028_prev[, c("modelable_entity_id", "measure_id") := list(11028, 5)]
zero_draws_11028_inc <- copy(zero_draws_11028_prev)
zero_draws_11028_inc$measure_id <- 6
zero_draws_11028 <- rbindlist(list(zero_draws_11028_inc, zero_draws_11028_prev), use.names = TRUE, fill = TRUE)

## ---- eval = FALSE ---------------------
for (loc in unique(non_endemic)) {
  zero_draws_11028$location_id <- loc
  write.csv(zero_draws_11028, "FILEPATH", row.names = FALSE) # Save draws by location
}
#'

#' #### Generate draws objects
## ------------------------------------------------------------------------
#' First we copy the incidence draws for MEID 10401
draws_10400_inc <- copy(draws_10401_inc) # Copy incidence draws
draws_10400_inc$modelable_entity_id <- ADDRESS

#' Next, we add the incidence draws from MEID 11028
draws_10400_inc[, c(paste0("draw_", 0:999))] <- draws_10400_inc[, c(paste0("draw_", 0:999))] + draws_11028_inc[, c(paste0("draw_", 0:999))]

save_path <- "FILEPATH"
## ---- eval = FALSE ---------------------
dir.create(save_path, showWarnings = TRUE)
#'
message(paste0("Incidence draws being saved in ", save_path))

#' Now save incidence draws to disk.
## ---- eval = FALSE ---------------------
for (loc in unique(draws_10400_inc$location_id)) {
  write.csv(draws_10400_inc[location_id == loc & year_id %in% output_years,], "FILEPATH", row.names = FALSE) # Save draws by location
}

#' Now save incidence draws for non-endemic locations to disk.
zero_draws[, c("modelable_entity_id", "measure_id") := list(ADDRESS, 6)] # Symptomatic Zika infections, incidence
for (loc in unique(non_endemic)) {
  zero_draws$location_id <- loc
  write.csv(zero_draws, "FILEPATH", row.names = FALSE) # Save draws by location
}

#' #### Compare estimates among 10400, 10401 and 11028
## ------------------------------------------------------------------------
draws_10401_inc$mean <- rowMeans(as.matrix(draws_10401_inc[, c(paste0("draw_", 0:999))]))
draws_10401_inc$lower <- rowQuantiles(as.matrix(draws_10401_inc[, c(paste0("draw_", 0:999))]), probs = c(0.025))
draws_10401_inc$upper <- rowQuantiles(as.matrix(draws_10401_inc[, c(paste0("draw_", 0:999))]), probs = c(0.975))
draws_11028_inc$mean <- rowMeans(as.matrix(draws_11028_inc[, c(paste0("draw_", 0:999))]))
draws_11028_inc$lower <- rowQuantiles(as.matrix(draws_11028_inc[, c(paste0("draw_", 0:999))]), probs = c(0.025))
draws_11028_inc$upper <- rowQuantiles(as.matrix(draws_11028_inc[, c(paste0("draw_", 0:999))]), probs = c(0.975))
draws_10400_inc$mean <- rowMeans(as.matrix(draws_10400_inc[, c(paste0("draw_", 0:999))]))
draws_10400_inc$lower <- rowQuantiles(as.matrix(draws_10400_inc[, c(paste0("draw_", 0:999))]), probs = c(0.025))
draws_10400_inc$upper <- rowQuantiles(as.matrix(draws_10400_inc[, c(paste0("draw_", 0:999))]), probs = c(0.975))

for (i in unique(draws_10401_inc$location_id)) {
  g <- ggplot() + theme_classic() + coord_cartesian(xlim = c(2008, 2022)) +
    geom_ribbon(data = draws_10401_inc[location_id == i & age_group_id == 11], aes(x = year_id, ymin = lower, ymax = upper, group = as.factor(sex_id), fill = as.factor(modelable_entity_id)), alpha = 0.05) +
    geom_line(data = draws_10401_inc[location_id == i & age_group_id == 11], aes(x = year_id, y = mean, group = as.factor(sex_id), linetype = as.factor(sex_id), color = as.factor(modelable_entity_id))) +
    geom_ribbon(data = draws_11028_inc[location_id == i & age_group_id == 11], aes(x = year_id, ymin = lower, ymax = upper, group = as.factor(sex_id), fill = as.factor(modelable_entity_id)), alpha = 0.05) +
    geom_line(data = draws_11028_inc[location_id == i & age_group_id == 11], aes(x = year_id, y = mean, group = as.factor(sex_id), linetype = as.factor(sex_id), color = as.factor(modelable_entity_id))) +
    geom_ribbon(data = draws_10400_inc[location_id == i & age_group_id == 11], aes(x = year_id, ymin = lower, ymax = upper, group = as.factor(sex_id), fill = as.factor(modelable_entity_id)), alpha = 0.05) +
    geom_line(data = draws_10400_inc[location_id == i & age_group_id == 11], aes(x = year_id, y = mean, group = as.factor(sex_id), linetype = as.factor(sex_id), color = as.factor(modelable_entity_id))) +
    ggtitle(label = paste0("Zika Incidence, ", location_hierarchy[location_id == i, location_name], " (", location_hierarchy[location_id == i, ihme_loc_id], ", ", i, ")"), subtitle = "Mean and 95% UI. Incidence by sex, among 30-34 yr olds") + xlab("Year") + ylab("Incidence") + theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  print(g)
}

#' # Estimate CZS: MEID 10403
## ------------------------------------------------------------------------
#' #### Get fertility rates
## ------------------------------------------------------------------------

asfr <- get_covariate_estimates(covariate_id = ADDRESS, sex_id = 2, year_id = model_years, location_id = locs$location_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
setnames(asfr, "mean_value", "asfr_mean")

#' #### Get stillbirth to live birth ratios
## ------------------------------------------------------------------------

qnn  <- get_covariate_estimates(covariate_id = ADDRESS, year_id = model_years, location_id = locs$location_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
setnames(qnn, "mean_value", "qnn_mean")
qnn <- qnn[, .(location_id, year_id, qnn_mean)]

#' #### Estimate pregnant women
## ------------------------------------------------------------------------

preg_df <- merge(asfr, qnn, by = c("location_id", "year_id"))
preg_df[, prPreg := 46/52 * asfr_mean * (1 + qnn_mean)]

pop  <- get_population(sex_id = 2, age_group_id = unique(preg_df$age_group_id), year_id = model_years, location_id = locs$location_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step, run_id = pop_run_id)
pop <- pop[, .(location_id, year_id, population, age_group_id)]

preg_df <- merge(preg_df, pop, by = c("location_id", "year_id", "age_group_id"), all.x = TRUE)
preg_df[, nPreg := population * prPreg]

preg_df <- preg_df[, .(age_group_id, location_id, year_id, sex_id, population, asfr_mean, prPreg, nPreg)]
setnames(preg_df, "asfr_mean", "asfr")

preg_df <- merge(preg_df, location_hierarchy, by = "location_id")

#' #### Get births by sex
## ------------------------------------------------------------------------

births <- get_covariate_estimates(covariate_id = ADDRESS, sex_id = c(1, 2), year_id = model_years, location_id = locs$location_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
setnames(births, "mean_value", "births")

#' Aggregate births by location and year
births_all <- aggregate(births ~ location_id + year_id, births, sum)
setnames(births_all, "births", "total_births")

#' Merge onto births by sex and calculate proportion of births by sex
births <- merge(births, births_all, by = c("location_id", "year_id"), all.x = TRUE)
births[, prop_births := births / total_births]

#' #### Estimate births to women infected with Zika in first trimester
## ------------------------------------------------------------------------
#' First copy incidence draws for 10400 (female only).
draws_10403_inc <- copy(draws_10400_inc[sex_id == 2])

#' Next, merge on number of pregnancies.
draws_10403_inc <- merge(draws_10403_inc, preg_df[sex_id == 2, c("location_id", "age_group_id", "year_id", "nPreg")], by = c("location_id", "age_group_id", "year_id"), all.x = TRUE)

#' Now multiply incidence draws by number of pregnancies to derive draws of numbers of pregnancies to Zika-infected mothers.
draws_10403_inc[, c(paste0("draw_", 0:999))] <- sweep(draws_10403_inc[, c(paste0("draw_", 0:999))], MARGIN = 1, as.matrix(draws_10403_inc$nPreg), `*`)

#' Collapse across age_group_ids.
collapsed <- draws_10403_inc[, lapply(.SD, sum), by = .(location_id, year_id), .SDcol = c(paste0("draw_", 0:999))]

#' Calculate mean births to Zika-infected mothers. Note that we are not accounting for uncertainty in the original incidence estimates.
#' We can return to this issue later as time allows.
collapsed$mean_births_zika <- rowMeans(as.matrix(collapsed[, c(paste0("draw_", 0:999))]))

#' Add draws for 1989 (simply replicate draws for 1990).
collapsed_1989 <- copy(collapsed[year_id == 1990])
collapsed_1989$year_id <- 1989
collapsed <- rbindlist(list(collapsed, collapsed_1989), use.names = TRUE, fill = TRUE)

#' Drop draws rows.
collapsed <- collapsed[, c("location_id", "year_id", "mean_births_zika")]

#' Sort by location and year.
setorderv(collapsed, c("location_id", "year_id"))

collapsed_backup <- copy(collapsed) # Save a backup copy of all locations
collapsed <- collapsed[location_id %in% endemic_locs] # Restrict to endemic locations

#' Loop through rows and calculate draws
for (i in 1:nrow(collapsed)) {
  if (collapsed[i, year_id] == 1989) {
    next
  } else {
    collapsed[i, "mean_births_zika_adjusted"] <- ((collapsed[i, "mean_births_zika"] * (52 - 24) / 52) + (collapsed[i - 1, "mean_births_zika"] * 43 / 52)) #* collapsed[i, prop_births]
  }
}

#' Add back non-endemic locations
collapsed <- rbindlist(list(collapsed, collapsed_backup[!(location_id %in% endemic_locs)]), use.names = TRUE, fill = TRUE)
collapsed <- collapsed[year_id != 1989] # Drop 1989
collapsed[is.na(mean_births_zika_adjusted), mean_births_zika_adjusted := 0] # Set births to zero for non-endemic locations

#' #### Retrieve CZS data bundle
## ------------------------------------------------------------------------
#' Retrieve the CZS data set and merge the estimated births to Zika-infected mothers
czs_data <- get_bundle_data(bundle_id = 568, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
czs_data <- czs_data[value_keep_row == 1] # Restrict to rows for modeling 
czs_data$year_id <- czs_data$year_end 

czs_data <- merge(czs_data, collapsed[, c("location_id", "year_id", "mean_births_zika_adjusted")], by = c("location_id", "year_id"), all.x = TRUE)
czs_data <- czs_data[location_id %in% locs$location_id]
czs_data$cases <- as.integer(czs_data$cases)

czs_data$obs_inc <- czs_data$cases / czs_data$mean_births_zika_adjusted # Calculate observed incidence

czs_data <- czs_data[!is.na(obs_inc) & obs_inc != "Inf"] # Drop rows for non-endemic "endemic" countries and those with zero Zika births
czs_data <- czs_data[!(nid == 340878 & location_id == 168 & year_id == 2017)] # Outlier AGO 2017 data (suspected cases; CZS cases >>> reported Zika cases)

czs_data$country <- substr(czs_data$ihme_loc_id, 1, 3) # Create country field

#' #### Set up prediction set
## ------------------------------------------------------------------------
pred_set_czs <- copy(collapsed) # Data table with every combination of location_id and year_id
pred_set_czs[, mean_births_zika := NULL] # Drop column
pred_set_czs_zeroes <- pred_set_czs[mean_births_zika_adjusted == 0] # Save rows with zero Zika births (will be added back later)
pred_set_czs <- pred_set_czs[mean_births_zika_adjusted > 0] # Restrict to rows with > 0 Zika births
pred_set_czs <- merge(pred_set_czs, location_hierarchy[, c("location_id", "ihme_loc_id")], by = "location_id", all.x = TRUE)
pred_set_czs$country <- substr(pred_set_czs$ihme_loc_id, 1, 3) # Create country field

#' #### Fit INLA model for CZS
## ------------------------------------------------------------------------
stk.y <- inla.stack(data = list(y = czs_data$cases), A = list(1), effects = list(list(b0 = 1, location_id = czs_data$location_id, country = czs_data$country, year_id = czs_data$year_start, births_offset = log(czs_data$mean_births_zika_adjusted))), tag = 'resp')
stk.pred <- inla.stack(data = list(y = NA), A = list(1), effects = list(list(b0 = 1, location_id = pred_set_czs$location_id, country = pred_set_czs$country, year_id = pred_set_czs$year_id, births_offset = log(pred_set_czs$mean_births_zika_adjusted))), tag = 'pred')
stk.all <- inla.stack(stk.y, stk.pred)

form_czs <- y ~ 0 + b0 + f(births_offset, model = "linear", mean.linear = 1, prec.linear = 1e12) + f(as.factor(location_id), model = "iid")# + f(year_id, model = "iid") # + f(year_id, model = "iid") + f(year_id, model = "ar1")

#' Fit model
model_fit_czs <- inla(form_czs, family = c("nbinomial"), data = inla.stack.data(stk.all), control.predictor = list(A = inla.stack.A(stk.all), link = 1), control.inla = list(int.strategy = "eb"), control.compute = list(config = TRUE, waic = TRUE, openmp.strategy = "default", smtp = "taucs"), verbose = TRUE, num.threads=1)
summary(model_fit_czs)

#' #### Retrieve predictions and generate draws
index.pred <- inla.stack.index(stk.all, tag = "pred")$data

draw_object_czs <- inla.posterior.sample(1000, model_fit_czs, intern = FALSE, seed = random.seed)

draws_czs <- copy(pred_set_czs[, c("location_id", "year_id")])
for (i in 1:length(draw_object_czs)) {
  draws_czs <- cbind(draws_czs, exp(draw_object_czs[[i]]$latent[index.pred])) # Exponentiate and combine linear predictor draws
}

colnames(draws_czs)[3:1002] <- paste0("draw_", 0:999) # Rename draw columns

#' Add zero draws for location-years with zero predicted births to Zika-infected mothers
pred_set_czs_zeroes[, c(paste0("draw_", 0:999))] <- 0
draws_czs <- rbindlist(list(draws_czs, pred_set_czs_zeroes), use.names = TRUE, fill = TRUE)
draws_czs$mean_births_zika_adjusted <- NULL # Drop births column
setorderv(draws_czs, c("location_id", "year_id"))

#' At this point, draws represent under-1 cases for both sexes combined.

#' Get under-1 population
under_1_pop <- get_population(age_group_id = 28, sex_id = 3, decomp_step = decomp_step, gbd_round_id = gbd_round_id, location_id = unique(draws_czs$location_id), location_set_id = 35, year_id = unique(draws_czs$year_id), run_id = pop_run_id)
draws_czs <- merge(draws_czs, under_1_pop, by = c("location_id", "year_id"), all.x = TRUE)

#' Now divide case draws by under-1 population to derive incidence draws
draws_czs[, c(paste0("draw_", 0:999))] <- sweep(draws_czs[, c(paste0("draw_", 0:999))], MARGIN = 1, as.matrix(draws_czs$population), `/`)
draws_czs[, sex_id := NULL] # Drop sex column

#' Draws now represent under-1 incidence for both sexes combined.
#' Since CZS is a chronic condition, assume that prevalence equals incidence
#' Set modelable_entity_id and measure_id
draws_czs[, c("modelable_entity_id", "measure_id") := list(10403, 5)]

#' Now split by under-1 age groups and by sex, assuming constant prevalence across age groups and sexes
setnames(under_1_pop, "population", "under_1_pop")
under_1_age_group_pops <- get_population(age_group_id = c(2, 3, 388, 389), sex_id = c(1, 2), decomp_step = decomp_step, gbd_round_id = gbd_round_id, location_id = unique(draws_czs$location_id), location_set_id = 35, year_id = unique(draws_czs$year_id), run_id = pop_run_id)
under_1_age_group_pops <- merge(under_1_age_group_pops, under_1_pop, by = c("location_id", "year_id"), all.x = TRUE, allow.cartesian = TRUE)
under_1_age_group_pops[, pop_prop := population / under_1_pop]
draws_czs <- merge(draws_czs, under_1_age_group_pops[, c("location_id", "year_id", "age_group_id.x", "sex_id.x", "pop_prop")], all.x = TRUE, allow.cartesian = TRUE)

#' Finally, calculate age- and sex-specific prevalence.
draws_czs[, c(paste0("draw_", 0:999))] <- sweep(draws_czs[, c(paste0("draw_", 0:999))], MARGIN = 1, as.matrix(draws_czs$pop_prop), `*`)

#' Clean up draws
draws_czs <- draws_czs[, c("modelable_entity_id", "measure_id", "location_id", "year_id", "sex_id.x", "age_group_id.x", paste0("draw_", 0:999))] # Set column order
setnames(draws_czs, c("sex_id.x", "age_group_id.x"), c("sex_id", "age_group_id")) # Rename columns

draws_czs_birth <- draws_czs[age_group_id == 2]
draws_czs_birth$age_group_id <- 164
draws_czs <- rbindlist(list(draws_czs, draws_czs_birth), use.names = TRUE)

save_path < -"FILEPATH"
dir.create(save_path, showWarnings = TRUE)
message(paste0("Prevalence draws being saved in ", save_path))

#' Now save prevalence draws to disk
for (loc in unique(draws_czs$location_id)) {
  write.csv(draws_czs[location_id == loc & year_id %in% output_years,], "FILEPATH", row.names = FALSE) # Save draws by location
}
#'

#' Now save incidence draws for non-endemic locations to disk.
zero_draws[, c("modelable_entity_id", "measure_id") := list(ADDRESS, 5)]
zero_draws_czs <- zero_draws[age_group_id %in% c(2, 3, 388, 389)] # Restrict to final age groups
zero_draws_czs_birth <- zero_draws_czs[age_group_id == 2]
zero_draws_czs_birth$age_group_id <- 164
zero_draws_czs <- rbindlist(list(zero_draws_czs, zero_draws_czs_birth), use.names = TRUE)
## ---- eval = FALSE ---------------------
for (loc in unique(non_endemic)) {
  zero_draws_czs$location_id <- loc
  write.csv(zero_draws_czs, "FILEPATH", row.names = FALSE) # Save draws by location
}
#'

#' # Estimate GBS: MEID 10402
## ------------------------------------------------------------------------
#' #### Establish draws object for GBS (10402)
draws_10402_prev <- copy(draws_10401_inc)
draws_10402_prev[, c("modelable_entity_id", "measure_id") := list(ADDRESS, 5)]

#' #### Apply metaregression alpha and beta
alpha <- outcomes[outcome == "gbs", alpha]
beta <- outcomes[outcome == "gbs", beta]

scales <- rbeta(1000, alpha, beta) # Generate 1000 random draws from the beta distribution

draws_10402_prev[, c(paste0("draw_", 0:999))] <- sweep(draws_10402_prev[, c(paste0("draw_", 0:999))], MARGIN = 2, as.matrix(scales), `*`) # Apply scales

#' #### Get GBS remission draws
gbs_remission_draws <- as.data.table(get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi", age_group_id = age_group_ids, measure_id = 7, location_id = unique(draws_10402_prev$location_id), year_id = output_years, gbd_round_id = gbd_round_id, decomp_step = decomp_step))
if (unique(gbs_remission_draws$model_version_id != 509342)) {
  stop("Wrong model version retrieved for GBS remission draws; please check this")
}

#' #### Calculate mean remission by location, year, age and sex
gbs_remission_draws$mean_remission <- rowMeans(as.matrix(gbs_remission_draws[, c(paste0("draw_", 0:999))]))
gbs_remission_draws$original_draw <- 1 # Flag original draws

#' #### Replicate 2015 remission draws
temp_2016 <- copy(gbs_remission_draws[year_id == 2015])
temp_2017 <- copy(gbs_remission_draws[year_id == 2015])
temp_2018 <- copy(gbs_remission_draws[year_id == 2019])
temp_2016$year_id <- 2016
temp_2017$year_id <- 2017
temp_2018$year_id <- 2018

gbs_remission_draws <- rbindlist(list(gbs_remission_draws, temp_2016, temp_2017, temp_2018), use.names = TRUE)

#' #### Apply remission draws
draws_10402_prev <- draws_10402_prev[year_id %in% output_years]

gbs_remission_draws <- gbs_remission_draws[age_group_id %in% age_group_ids & age_group_id != 22] # Restrict to final age groups, if needed

#'  Check that location, age group, sex and year line up between draw objects.
setorderv(gbs_remission_draws, c("location_id", "age_group_id", "sex_id", "year_id")) # Reorder rows to make sure that they line up with other draws object
setorderv(draws_10402_prev, c("location_id", "age_group_id", "sex_id", "year_id")) # Reorder rows to make sure that they line up with other draws object
if (sum(1 - as.integer(gbs_remission_draws[,c("location_id", "age_group_id", "sex_id", "year_id")] == draws_10402_prev[,c("location_id", "age_group_id", "sex_id", "year_id")])) > 0) { # Should be zero
  stop("Draws rows are not aligned for MEID 10402 and GBS remission")
}

#' Generate prevalence estimates using remission draws.
draws_10402_prev[, c(paste0("draw_", 0:999))] <- draws_10402_prev[, c(paste0("draw_", 0:999))] / gbs_remission_draws[, c(paste0("draw_", 0:999))]

#' #### Save 10402 draws
save_path <- "FILEPATH"
dir.create(save_path, showWarnings = TRUE)
message(paste0("Prevalence draws being saved in ", save_path))

#' Now save prevalence draws to disk.
for (loc in unique(draws_10402_prev$location_id)) {
  write.csv(draws_10402_prev[location_id == loc & year_id %in% output_years,], "FILEPATH", row.names = FALSE) # Save draws by location
}

#' Now save incidence draws for non-endemic locations to disk.
zero_draws[, c("modelable_entity_id", "measure_id") := list(ADDRESS, 5)] # Symptomatic Zika infections, incidence
## ---- eval = FALSE ---------------------
for (loc in unique(non_endemic)) {
  zero_draws$location_id <- loc
  write.csv(zero_draws, "FILEPATH", row.names = FALSE) # Save draws by location
}
