################################################################
##                                                            ##
## Purpose: To prep the input data for the stillbirths model. ##
##                                                            ##
################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(argparse)
library(assertable)
library(data.table)
library(ggplot2)
library(haven)
library(plyr)
library(purrr)

library(mortcore)
library(mortdb)

user <- Sys.getenv("USER")
root <- "FILEPATH"

## GET ARGUMENTS AND SETTINGS

args <- commandArgs(trailingOnly = TRUE)
settings_dir <- args[1]

if (interactive()) {
  version_data <- 999
  settings_dir <- paste0("FILEPATH", version_data, "FILEPATH")
}

load(settings_dir)
list2env(settings, envir = environment())

## Functions

source(get_covariate_estimates.R)
source(get_bundle_data.R)
source(stillbirth_data_functions.R)

## Get official location names

locs <- mortdb::get_locations(level = "estimate", gbd_year = gbd_year)
locs <- locs[, c("ihme_loc_id", "location_name", "location_id", "region_name", "super_region_name")]
readr::write_csv(
  locs,
  paste0(data_dir, version_data, "FILEPATH")
)

countries <- mortdb::get_locations(
  level = "country",
  gbd_year = gbd_year
)
countries <- countries[, c("location_name", "ihme_loc_id")]

################
## Covariates ##
################

## Get covariates

maternal_educ_yrs_pc <- get_covariate_estimates(
  covariate_id = VERSION,
  location_id = locs[, location_id],
  year_id = year_start:year_end,
  model_version_id = maternal_edu_version,
  decomp_step = maternal_edu_decomp_step,
  gbd_round_id = get_gbd_round(gbd_year)
)

## Get births

births <- mortdb::get_mort_outputs(
  model_name = "birth",
  model_type = "estimate",
  run_id = parents[["birth estimate"]],
  sex_ids = 3,
  location_ids = locs[, location_id],
  year_ids = year_start:year_end,
  age_group_ids = 169
)

births <- births[, c("location_id", "year_id", "mean")]
setnames(births, "mean", "births")

## Import neonatal and under 5 mortality rate

if (file.exists("FILEPATH")) {

  nmr <- fread("FILEPATH")

} else {

  nmr <- mortdb::get_mort_outputs(
    model_name = "no shock life table",
    model_type = "estimate",
    run_id = parents[["no shock life table estimate"]],
    life_table_parameter_ids = 3,
    estimate_stage_ids = 5,
    gbd_year = gbd_year,
    sex_ids = 3,
    year_ids = year_start:year_end,
    age_group_ids = c(1, 42)
  )

  nmr[sex_id == 3, sex := "both"]
  nmr <- nmr[!is.na(ihme_loc_id), c("ihme_loc_id", "year_id", "sex", "age_group_id", "mean", "lower", "upper")]
  nmr <- dcast(
    nmr,
    ihme_loc_id + year_id + sex ~ age_group_id,
    value.var = c("mean", "lower", "upper")
  )
  colnames(nmr) <- c("ihme_loc_id", "year_id", "sex", "q_u5_med", "q_nn_med", "q_u5_lower", "q_nn_lower", "q_u5_upper", "q_nn_upper")
  nmr$sex <- NULL

  age_sex_niu <- fread(paste0("FILEPATH", parents[["age sex estimate"]], "FILEPATH"))[sex == "both"]
  age_sex_tkl <- fread(paste0("FILEPATH", parents[["age sex estimate"]], "FILEPATH"))[sex == "both"]

  age_sex_niu[, q_nn := 1 - ((1 - q_enn) * (1 - q_lnn))]
  age_sex_tkl[, q_nn := 1 - ((1 - q_enn) * (1 - q_lnn))]

  age_sex_niu_summary <- age_sex_niu[,
    .(q_nn_med = mean(q_nn), q_u5_med = mean(q_u5),
      q_nn_lower = quantile(q_nn, prob = c(.025)), q_u5_lower = quantile(q_u5, prob = c(.025)),
      q_nn_upper = quantile(q_nn, prob = c(.975)), q_u5_upper = quantile(q_u5, prob = c(.975))),
    by = "year"
  ]
  age_sex_tkl_summary <- age_sex_tkl[,
    .(q_nn_med = mean(q_nn), q_u5_med = mean(q_u5),
      q_nn_lower = quantile(q_nn, prob = c(.025)), q_u5_lower = quantile(q_u5, prob = c(.025)),
      q_nn_upper = quantile(q_nn, prob = c(.975)), q_u5_upper = quantile(q_u5, prob = c(.975))),
    by = "year"
  ]

  age_sex_niu_summary[, `:=` (ihme_loc_id = "NIU", year_id = floor(year), year = NULL)]
  age_sex_tkl_summary[, `:=` (ihme_loc_id = "TKL", year_id = floor(year), year = NULL)]

  print(paste0("NIU 2020 nn: ", round(nmr[ihme_loc_id == "NIU" & year_id == 2020]$q_nn_med, 3), " -> ", round(age_sex_niu_summary[year_id == 2020]$q_nn_med, 3)))
  print(paste0("NIU 2021 nn: ", round(nmr[ihme_loc_id == "NIU" & year_id == 2021]$q_nn_med, 3), " -> ", round(age_sex_niu_summary[year_id == 2021]$q_nn_med, 3)))
  print(paste0("NIU 2020 u5: ", round(nmr[ihme_loc_id == "NIU" & year_id == 2020]$q_u5_med, 3), " -> ", round(age_sex_niu_summary[year_id == 2020]$q_u5_med, 3)))
  print(paste0("NIU 2021 u5: ", round(nmr[ihme_loc_id == "NIU" & year_id == 2021]$q_u5_med, 3), " -> ", round(age_sex_niu_summary[year_id == 2021]$q_u5_med, 3)))

  print(paste0("TKL 2020 nn: ", round(nmr[ihme_loc_id == "TKL" & year_id == 2020]$q_nn_med, 3), " -> ", round(age_sex_tkl_summary[year_id == 2020]$q_nn_med, 3)))
  print(paste0("TKL 2021 nn: ", round(nmr[ihme_loc_id == "TKL" & year_id == 2021]$q_nn_med, 3), " -> ", round(age_sex_tkl_summary[year_id == 2021]$q_nn_med, 3)))
  print(paste0("TKL 2020 u5: ", round(nmr[ihme_loc_id == "TKL" & year_id == 2020]$q_u5_med, 3), " -> ", round(age_sex_tkl_summary[year_id == 2020]$q_u5_med, 3)))
  print(paste0("TKL 2021 u5: ", round(nmr[ihme_loc_id == "TKL" & year_id == 2021]$q_u5_med, 3), " -> ", round(age_sex_tkl_summary[year_id == 2021]$q_u5_med, 3)))

  nmr <- rbind(
    nmr[!(ihme_loc_id %in% c("NIU", "TKL") & year_id %in% 2020:2021)],
    age_sex_niu_summary[year_id %in% 2020:2021],
    age_sex_tkl_summary[year_id %in% 2020:2021]
  )

  readr::write_csv(nmr, paste0(data_dir, version_data, "FILEPATH"))

  nmr <- nmr[ihme_loc_id %in% locs$ihme_loc_id]

  readr::write_csv(nmr, paste0(data_dir, version_data, "FILEPATH"))

}

# Create covariates file

covariates <- merge(
  maternal_educ_yrs_pc,
  locs,
  by = "location_id",
  all.x = TRUE
)
covariates <- covariates[, c("ihme_loc_id", "covariate_name_short", "mean_value",
                             "year_id", "region_name", "location_id")]
covariates <- dcast(
  covariates,
  year_id + ihme_loc_id + location_id ~ covariate_name_short,
  value.var = "mean_value"
)

covariates <- merge(
  covariates,
  nmr,
  by = c("ihme_loc_id", "year_id"),
  all = TRUE
)
covariates <- merge(
  covariates,
  births,
  by = c("location_id", "year_id"),
  all.x = TRUE
)

covariates[, log_q_nn_med := log(q_nn_med)]

assertable::assert_values(covariates, colnames = names(covariates), test = "not_na")

## Write covariate dataset

readr::write_csv(covariates, paste0(data_dir, version_data, "FILEPATH"))

###########################
## Clean LSEIG Estimates ##
###########################

estimates_2000 <- fread(
  paste0(root, "FILEPATH"),
  header = TRUE
)
estimates_2008 <- fread(
  paste0(root, "FILEPATH"),
  header = TRUE
)
estimates_2009 <- fread(
  paste0(root, "FILEPATH"),
  header = TRUE
)
estimates_2015 <- fread(
  paste0(root, "FILEPATH"),
  header = TRUE
)

lseig_estimates <- as.data.table(
  rbind.fill(estimates_2000, estimates_2008, estimates_2009, estimates_2015)
)

lseig_estimates <- lseig_estimates[!is.na(sbr)]

country_names <- c(
  "Bolivia", "Brunei", "Czech Republic", "Iran", "Macedonia", "Moldova",
  "North Korea", "Occupied Palestinian Territory", "Russia", "South Korea",
  "Swaziland", "Tanzania", "The Bahamas", "The Gambia", "Vietnam"
)

ihme_country_names <- c(
  "Bolivia (Plurinational State of)", "Brunei Darussalam", "Czechia",
  "Iran (Islamic Republic of)", "North Macedonia", "Republic of Moldova",
  "Democratic People's Republic of Korea", "Palestine", "Russian Federation",
  "Republic of Korea", "Eswatini", "United Republic of Tanzania", "Bahamas",
  "Gambia", "Viet Nam"
)

lseig_estimates[, location_name := mapvalues(lseig_estimates$location_name, country_names, ihme_country_names)]

lseig_estimates[location_name == "C<f4>te d'Ivoire", location_name := "Cote d'Ivoire"]
lseig_estimates[location_name == "Turkey", location_name := "Turkiye"]

lseig_estimates <- merge(
  lseig_estimates,
  countries,
  by = "location_name",
  all.x = TRUE
)

lseig_estimates <- lseig_estimates[, c("ihme_loc_id", "sbr", "year")]

assertable::assert_values(
  lseig_estimates,
  colnames = c("ihme_loc_id", "sbr", "year"),
  test = "not_na"
)

## Save clean version of lseig estimates

readr::write_csv(
  lseig_estimates,
  paste0(data_dir, version_data, "FILEPATH")
)

######################
## Clean LSEIG Data ##
######################

lseig_data <- fread(
  paste0(root, "FILEPATH")
)

lseig_data$country <- as.character(lseig_data$country)

country_names <- c(
  "Bolivia", "Cape Verde", "China", "Czech Republic", "DR Congo", "Swaziland", "Turkey",
  "The former Yugoslav Republic of Macedonia", "United States", "Venezuela"
)

ihme_country_names <- c(
  "Bolivia (Plurinational State of)", "Cabo Verde", "China (without Hong Kong and Macao)",
  "Czechia", "Democratic Republic of the Congo", "Eswatini", "Turkiye", "North Macedonia",
  "United States of America", "Venezuela (Bolivarian Republic of)"
)

lseig_data[, country := mapvalues(lseig_data$country, country_names, ihme_country_names)]
lseig_data <- merge(
  lseig_data,
  locs,
  by.x = "country",
  by.y = "location_name",
  all.x = TRUE
)
assertable::assert_values(
  lseig_data,
  colnames = c("ihme_loc_id"),
  test = "not_na"
)

lseig_data <- lseig_data[!(ihme_loc_id %in% c("MEX_4657", "NGA_25344", "USA_533")), ] # Removes rows added b/c of duplicate location names in hierarchy

## Create source categories for the lseig data

lseig_data$source <- tolower(lseig_data$source)

lseig_data[source %in% grep("registry", source, value = TRUE), source_type := "Birth Registry"]
lseig_data[source %in% grep("perinatal", source, value = TRUE), source_type := "Birth Registry"]

lseig_data[source %in% grep("consultation", source, value = TRUE), source_type := "Consultation"]

lseig_data[source %in% grep("cmace 2009 report", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("dhis", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("estadisticas", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("health", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("l'etat", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("moh", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("russia", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("ppip", source, value = TRUE), source_type := "Statistical Report"]
lseig_data[source %in% grep("stat", source, value = TRUE), source_type := "Statistical Report"]

lseig_data[source %in% grep("dhs", source, value = TRUE), source_type := "Survey"]
lseig_data[source %in% grep("dlhs", source, value = TRUE), source_type := "Survey"]
lseig_data[source %in% grep("hmis", source, value = TRUE), source_type := "Survey"]
lseig_data[source %in% grep("matlab", source, value = TRUE), source_type := "Survey"]
lseig_data[source %in% grep("moma trial", source, value = TRUE), source_type := "Survey"]
lseig_data[source %in% grep("nmnss", source, value = TRUE), source_type := "Survey"]
lseig_data[source %in% grep("survey", source, value = TRUE), source_type := "Survey"]
lseig_data[source %in% grep("whomcs", source, value = TRUE), source_type := "Survey"]

lseig_data[source %in% grep("demographic", source, value = TRUE), source_type := "VR"]
lseig_data[source %in% grep("eurozone", source, value = TRUE), source_type := "VR"]

lseig_data[!is.na(sbr) & is.na(source_type), source_type := "Sci Lit"] # all remaining sources at this point are last names

## Further classify source type

comp_file <- as.data.table(
  haven::read_dta(
    "FILEPATH",
    encoding = "latin1"
  )
)

comp_file[, year_id := floor(year)]
comp_file[, complete := ifelse(comp >= 0.95, 1, 0)]

comp <- comp_file[!((ihme_loc_id %like% "IND"| ihme_loc_id == "BGD") & iso3_sex_source %like% "VR")] # keeping completeness for BGD and IND SRS instead

comp <- comp[(iso3_sex_source %like% "VR" | iso3_sex_source %like% "SRS") & iso3_sex_source %like% "both",
             c("ihme_loc_id", "year_id", "u5_comp_pred", "iso3_sex_source")]

setnames(comp, "year_id", "year")
comp <- unique(comp)

lseig_data <- merge(
  lseig_data,
  comp,
  by = c("ihme_loc_id", "year"),
  all.x = TRUE
)

lseig_data[source_type %in% c("Statistical Report", "Birth Registry", "VR") & !is.na(u5_comp_pred), source_type := "VR"]
lseig_data[source_type %in% c("Statistical Report", "Birth Registry", "VR") & is.na(u5_comp_pred), source_type := "Statistical Report"]

assertable::assert_values(
  lseig_data,
  colnames = c("sbr", "source_type"),
  test = "not_na"
)

source_type_list <- c("VR", "Survey", "Statistical Report", "Sci Lit", "Consultation")
assertable::assert_values(
  lseig_data,
  colnames = "source_type",
  test = "in",
  test_val = source_type_list
)

## Recode definitions to match our data

lseig_data[definition == "500g", definition := "500_grams"]
lseig_data[definition == "1000g", definition := "1000_grams"]

lseig_data[, definition := tolower(definition)]
lseig_data[, definition := gsub(" ", "_", definition)]
lseig_data[definition == "all", definition := "other"]
lseig_data[, std_def := definition]

assertable::assert_values(
  lseig_data,
  colnames = c("std_def"),
  test = "not_na"
)

def_list <- c(
  "28_weeks", "26_weeks", "24_weeks", "22_weeks", "20_weeks",
  "1000_grams", "500_grams", "other", "not_defined"
)
assertable::assert_values(
  lseig_data,
  colnames = "std_def",
  test = "in",
  test_val = def_list
)

## Reassign LSEIG definitions

lseig_data[ihme_loc_id == "CHL" & std_def == "other", std_def := "28_weeks"]
lseig_data[ihme_loc_id == "ECU" & std_def == "other", std_def := "28_weeks"]
lseig_data[ihme_loc_id == "KGZ" & std_def == "other", std_def := "28_weeks"]

lseig_data[ihme_loc_id == "AUT", std_def := "500_grams"] # values match Austria Health Statistics Report
lseig_data[ihme_loc_id == "MNG", std_def := "20_weeks"] # values match Mongolia Health Indicators

## Save clean version of lseig data

readr::write_csv(
  lseig_data,
  "FILEPATH"
)

###############
## Pull Data ##
###############

mortality_file <- "FILEPATH"

## Pull in tabs data (prepped in prep_stillbirth_tabs.R)

tabs <- fread("FILEPATH")

## Get data from source_type-specific functions (too slow when run interactively)

if (interactive()) {

  lit <- fread("FILEPATH")
  sbh <- fread("FILEPATH")
  vr <- fread("FILEPATH")

} else {

  lit <- fread("FILEPATH")
  sbh <- fread("FILEPATH")
  vr <- getVrOutput()

}

readr::write_csv(
  lit,
  "FILEPATH"
  na = ""
)
readr::write_csv(
  sbh,
  "FILEPATH",
  na = ""
)
readr::write_csv(
  vr,
  "FILEPATH",
  na = ""
)
readr::write_csv(
  tabs,
  "FILEPATH",
  na = ""
)

## Clean sbh data

sbh[, definition := "28_weeks"]
setnames(sbh, "sb_count", "stillbirths")
setnames(sbh, "lb_count", "total_births")

sbh[, sbr := stillbirths/total_births]

sbh <- sbh[total_births != 0]

sbh[, age_group_id := 22]
sbh[, year := substr(source_year, 1, 4)]
sbh[, year_id := substr(source_year, 1, 4)]
sbh[, data_type := "survey"]
sbh[, sex_id := 3]

sbh <- merge(
  sbh,
  locs[, c("ihme_loc_id", "location_id")],
  by = "ihme_loc_id",
  all.x = TRUE
)

## Drop tabulated DHS extractions to prioritize microdata
tabs <- tabs[!(nid %in% sbh$nid)]

## Drop non-representative sci lit

if (drop_non_rep_sci_lit) {

  lit_to_outlier <- lit[!(representative_name %in% c("Nationally representative only", "Representative for subnational location only"))]

  readr::write_csv(
    lit_to_outlier,
    "FILEPATH"
  )

}

## Combine outputs from all data sources

combined_data <- rbindlist(
  list(vr, lit, sbh, tabs),
  use.names = TRUE,
  fill = TRUE
)

combined_data <- as.data.table(unique(combined_data))

## Add on location-related variables

combined_data$location_id <- NULL

combined_data <- merge(
  combined_data,
  locs[, c("ihme_loc_id", "location_id", "super_region_name")],
  by = "ihme_loc_id",
  all.x = TRUE
)

## Clean source_type column

combined_data[data_type %in% c("vr_complete", "vr_incomplete"), data_type := "vital registration"]

combined_data[data_type == "census", source_type := "Census"]
combined_data[data_type == "gov_report", source_type := "Statistical Report"]
combined_data[data_type == "literature", source_type := "Sci Lit"]
combined_data[data_type == "survey", source_type := "Survey"]
combined_data[data_type == "vital registration", source_type := "VR"]

## Remove only census (BMU 1970)

combined_data <- combined_data[source_type != "Census"]

## Remove ZAF VR before 1996

combined_data <- combined_data[!(ihme_loc_id %like% "ZAF" & year_id < 1996 & source_type == "VR")]

## Adjust survey data

surveys <- combined_data[source_type == "Survey"]
survey_scalars <- fread(paste0(j_dir, "survey_scalars.csv"))

surveys <- merge(
  surveys,
  survey_scalars[, c("nid", "ihme_loc_id", "scalar")],
  by = c("nid", "ihme_loc_id"),
  all.x = TRUE
)

survey_scalars_nat <- survey_scalars[ihme_loc_id %in% countries$ihme_loc_id]
survey_scalars_nat[, decade := plyr::round_any(year_start, 10, f = floor)]

survey_scalars_nat <- merge(
  survey_scalars_nat,
  locs[, c("ihme_loc_id", "region_name")],
  by = "ihme_loc_id",
  all.x = TRUE
)

survey_scalars_nat[, scalar_region_decade := mean(scalar), by = c("region_name", "decade")]
survey_scalars_nat[, scalar_global_decade := mean(scalar), by = "decade"]

survey_scalars_reg <- unique(survey_scalars_nat[, c("region_name", "decade", "scalar_region_decade")])
survey_scalars_glob <- unique(survey_scalars_nat[, c("decade", "scalar_global_decade")])

surveys_annual <- c( # no adjustment needed (annual data)
  9929, 58231, 234353, 281538, 417841, 417859:417866, 506170
)

surveys_scalar_missing <- surveys[is.na(scalar) & !(nid %in% surveys_annual)]
surveys_scalar_missing[, scalar := NULL]
surveys_scalar_missing[, year_id := as.numeric(year_id)]
surveys_scalar_missing[, decade := plyr::round_any(year_id, 10, f = floor)]

surveys_scalar_missing <- merge(
  surveys_scalar_missing,
  locs[, c("ihme_loc_id", "region_name")],
  by = "ihme_loc_id",
  all.x = TRUE
)

surveys_scalar_missing <- merge( # adjust by average scalar for region in decade, if possible
  surveys_scalar_missing,
  survey_scalars_reg,
  by = c("region_name", "decade"),
  all.x = TRUE
)

surveys_scalar_missing <- merge( # adjust by average scalar for decade, if possible
  surveys_scalar_missing,
  survey_scalars_glob,
  by = "decade",
  all.x = TRUE
)

surveys_scalar_missing[, scalar := ifelse(!is.na(scalar_region_decade), scalar_region_decade, scalar_global_decade)]

surveys_scalar_missing[, `:=` (decade = NULL, region_name = NULL, scalar_region_decade = NULL, scalar_global_decade = NULL)]

global_scalar <- mean(survey_scalars_nat$scalar) # use global scalar if still missing (nids 5090, 8444, 27301)
surveys_scalar_missing[is.na(scalar), scalar := global_scalar]

surveys <- rbind(surveys[!is.na(scalar) | nid %in% surveys_annual], surveys_scalar_missing)

surveys[!is.na(scalar), stillbirths := stillbirths / scalar]
surveys[!is.na(scalar), sbr := stillbirths / total_births]
surveys[, scalar := NULL]

combined_data <- rbind(surveys, combined_data[source_type != "Survey"])

## Additional testing via assertable

expected_colnames <- c(
  "year", "year_id", "ihme_loc_id", "total_births", "sbr", "lower", "upper",
  "sex_id", "age_group_id", "source_type", "nid", "std_def"
)
assertable::assert_colnames(
  combined_data,
  expected_colnames,
  only_colnames = FALSE
)

not_na_colnames <- c(
  "year", "ihme_loc_id", "sex_id", "age_group_id", "source_type", "nid"
)
assertable::assert_values(
  combined_data,
  not_na_colnames,
  test = "not_na"
)

not_negative_colnames <- c("total_births", "sbr")
assertable::assert_values(
  combined_data,
  not_negative_colnames,
  test = "gte",
  test_val = 0,
  na.rm = TRUE
)

assertable::assert_values(
  combined_data,
  "age_group_id",
  test = "equal",
  test_val = 22
)
assertable::assert_values(
  combined_data,
  "sex_id",
  test = "equal",
  test_val = 3
)

assertable::assert_values(
  combined_data,
  c("year_id", "year"),
  test = "lte",
  test_val = year_end,
  na.rm = TRUE
)

################
## Clean Data ##
################

data <- copy(combined_data)

data <- data[year_id >= 1950]

data[sbr > 0.5, sbr := sbr/1000]
data <- data[!is.na(sbr)]

##############################
## Create source categories ##
##############################

if (nrow(data[!is.na(sbr) & is.na(source_type),]) > 0) stop("missing source_type for an observation with data")

# Deal with source classifications here

data[, data_name := source_type]
data[!is.na(ihme_loc_id) & !is.na(data_name), loc_source := paste0(ihme_loc_id, "-", data_name)]

data[source_type %in% c("Standard DHS", "RHS", "MICS"), source_type := "Survey"]

data[is.na(source), source := "Custom"]

#########################################
## Resolve any issues with definitions ##
#########################################

## For any definitions that aren't already mapped to a standard, change here:
# >= 28 weeks                         1        28_weeks
# >= 26 weeks                         2        26_weeks
# >= 24 weeks                         3        24_weeks
# >= 22 weeks                         4        22_weeks
# >= 20 weeks                         5        20_weeks
# >= 12 weeks                         6        12_weeks
# >= 1000 grams                       7        1000_grams
# >= 500 grams                        8        500_grams
# other                               9        other
# not defined                         10       not defined
# >= 22 weeks OR >= 500 grams         11       22_weeks_OR_500_grams
# >= 28 weeks OR >= 1000 grams        12       28_weeks_OR_1000_grams
# >= 22 weeks AND >= 500 grams        13       22_weeks_AND_500_grams
# >= 28 weeks AND >= 1000 grams       14       28_weeks_AND_1000_grams

data[nid == 462091, definition := "a baby born with no signs of life at or after 28 weeks of gestation or as the delivery of a dead foetus whose birth weight is more than 500g"]

# 20 weeks

data[definition == "birth taking place after 5th month of pregnancy with no signs of life", std_def := "20_weeks"]
data[definition == "delivery of a dead fetus (without pulse) at GA >= 20 weeks", std_def := "20_weeks"]
data[definition == "fetal death at 20 weeks of gestation or more", std_def := "20_weeks"]
data[definition == "fetal death at 20 weeks of gestation or more; only fetal deaths of 500 grams or more", std_def := "20_weeks"]
data[definition == "fetal death refers to the intrauterine death of a fetus prior to delivery and  a presumed period of GA >=20 weeks", std_def := "20_weeks"]
data[definition == "GA >= 20 weeks or if GA unknown, BW >= 500 g", std_def := "20_weeks"]

# 28 weeks

data[definition == "28 weeks", std_def := "28_weeks"]
data[definition == "28_weeks", std_def := "28_weeks"]
data[definition == "all non-live birth terminations with pregnancy durations of 7 to 9 months", std_def := "28_weeks"]
data[definition == "baby born with no signs of life after 28 completed weeks gestation", std_def := "28_weeks"]
data[definition == "death of fetus in pregnancies of seven months or more", std_def := "28_weeks"]
data[definition == "GA>= 28 weeks, used ICD-10", std_def := "28_weeks"]
data[definition == "stillbirth or intra-uterine fetal death, defined by WHO as fetal death after 28 weeks gestation", std_def := "28_weeks"]
data[definition == paste0("The Stillbirth (Definition) Act 1992 redefined a stillbirth, from 1 October 1992, as a child which had issued forth from ",
                          "its mother after the 24th week of pregnancy and which did not breath or show any other sign of life. Prior to 1 October 1992 ",
                          "the statistics related to events occurring after the 28th week of pregnancy.") & year_id == 1992, std_def := "28_weeks"]

# 1000 grams

data[(is.na(definition) | definition == "") & std_def == "1000g", std_def := "1000_grams"]
data[is.na(definition) & std_def == "1000_grams", definition := "1000_grams"]
data[definition == "define stillbirth in accordance with the WHO criteria as data on vital status and birthweight were routinely recorded,
                    i.e. a newborn at or above 1000 g showing no vital signs immediately after birth", std_def := "1000_grams"]

# 22 weeks OR 500 grams

data[definition == "death of a fetus BW >= 500 g or if BW not available, GA >= 22 weeks", std_def := "22_weeks_OR_500_grams"]

# 28 weeks OR 1000 grams

data[definition == "BW >= 1000 g, GA>= 28 weeks, or a body length >= 35 cm", std_def := "28_weeks_OR_1000_grams"]
data[definition == "late fetal death at GA >=28 weeks or BW > 1000 g", std_def := "28_weeks_OR_1000_grams"]

# 28 weeks AND 1000 grams

data[definition == "BW>= 1000 g and a GA >= 28 weeks", std_def := "28_weeks_AND_1000_grams"]
data[definition == "delivered without evidence of life at GA >28 weeks and with a BW >=1000 g", std_def := "28_weeks_AND_1000_grams"]

data[definition == "GA 22 to 26 weeks", std_def := "other"]
data[definition == paste0("only looking at infants with a GA >=22 and <32 weeks; foetal mortality, the death is indicated by the fact that after such separation ",
                          "(expulsion or extraction from the mother) there are no signs of life"), std_def := "other"]
data[definition == "singleton births; GA 33 to 42 weeks", std_def := "other"]
data[definition == "singleton births; no mention of GA range however screened at GA 30-34 weeks", std_def := "other"]
data[definition %like% paste0("The stillbirths were classified according to whether they had a birthweight of greater than or lesser than 2.0 kg. ",
                              "An infant born in good condition in Kirakira with a birthweight of more than 2.0 kg is likely to be able to feed and survive, ",
                              "whilst infants with a birthweight of less than 2.0 kg are unlikely to survive because of a lack of neonatal intensive care equipment and skills."), std_def := "other"]

# not defined

data[is.na(definition) & !is.na(sbr), definition := "not defined"]
data[definition == "" & is.na(std_def), std_def := "not defined"]
data[definition == paste0("a child was considered to be stillborn or to have died in childbirth if it was at least 35 cm long and had neither used ",
                          "natural pulmonary respiration nor had his heart beat or the umbilical cord pulsed"), std_def := "not_defined"]
data[definition == "breathing is not evident at delivery" | definition == "breathing is not evident at time of delivery", std_def := "not defined"]
data[definition == paste0("death prior to the complete expulsion or extraction from its mother of a product of conception, irrespective of the duration of pregnancy; ",
                          "the death is indicated by the fact that after such separation the foetus does not breathe or show any other evidence of life, such as the beating of the heart, ",
                          "pulsation of the umbilical cord, or definite movement of the voluntary muscles"), std_def := "not defined"]
data[definition == "fetal deaths, singleton births", std_def := "not defined"]
data[definition == paste0("The fetus does not breath, or show evidence of life such as beating of the heart, pulsation of the umbilical cord or definite movement of voluntary muscles after ",
                          "its separation from its mother"), std_def := "not defined"]
data[definition == paste0("Since 2001 the definition of stiilbirth has been changed per recommendations of WHO for national statistics: stillborn children are children of gestational age ≥22 weeks ",
                          "or birth weight of 500 g or more. Comparisons with previous years are therefore not possible.") & year_id < 2001, std_def := "not defined"]
data[definition == paste0("Since 2001 the definition of a stillborn was changed, according to recommendations of WHO for national statistics and a stillborn child has been considered a child with ",
                          "≥22 weeks gestational age and birth waiting 500 g or more and comparisons with previous years could not be possible.") & year_id < 2001, std_def := "not defined"]
data[definition == paste0("Stillbirth is the death of a product of conception before its complete expulsion or extraction from its mother irrespective of the duration of pregnancy. ",
                          "After such separation, the fetus does not breathe or show any evidence of life, such as beating of the heart, pulsation of the umbilical cord, ",
                          "or definite movement of muscles."), std_def := "not defined"]
data[definition == "not_defined" & is.na(std_def), std_def := "not defined"]
data[definition == "not defined" & is.na(std_def), std_def := "not defined"]
data[definition == "Unknown" & is.na(std_def), std_def := "not defined"]

# Test that inclusion criteria makes sense given preference

if (nrow(data[sb_inclusion_preference %in% c("ga or bw", "either ga or bw", "ga and bw", "both ga and bw", "both bw and ga") &
              (is.na(sb_inclusion_ga) | is.na(sb_inclusion_bw))])) stop("there's data missing inclusion criteria values.")

# Make fixes to std_def column

data[std_def == ">=22 weeks", std_def := "22_weeks"]
data[std_def == "Not defined", std_def := "not defined"]
data[std_def == "not_defined", std_def := "not defined"]
data[std_def == "", std_def := "not defined"]

data[sb_inclusion_ga == 21, sb_inclusion_ga := 20]
data[sb_inclusion_ga == 23, sb_inclusion_ga := 22]
data[sb_inclusion_ga == 25, sb_inclusion_ga := 24]
data[sb_inclusion_ga == 29, sb_inclusion_ga := 28]

data[sb_inclusion_ga == 20, std_def := "20_weeks"]
data[sb_inclusion_ga == 24, std_def := "24_weeks"]
data[sb_inclusion_ga == 26, std_def := "26_weeks"]

data[sb_inclusion_ga == 22 & is.na(sb_inclusion_bw), std_def := "22_weeks"]
data[sb_inclusion_ga == 28 & is.na(sb_inclusion_bw), std_def := "28_weeks"]
data[sb_inclusion_bw == 500 & is.na(sb_inclusion_ga), std_def := "500_grams"]
data[sb_inclusion_bw == 1000 & is.na(sb_inclusion_ga), std_def := "1000_grams"]

data[sb_inclusion_ga == 22 & sb_inclusion_bw == 500 & sb_inclusion_preference %in% c("ga or bw", "either ga or bw"), std_def := "22_weeks_OR_500_grams"]
data[sb_inclusion_ga == 22 & sb_inclusion_bw == 500 & sb_inclusion_preference %in% c("ga and bw", "both ga and bw", "both bw and ga"), std_def := "22_weeks_AND_500_grams"]

data[sb_inclusion_ga == 28 & sb_inclusion_bw == 1000 & sb_inclusion_preference %in% c("ga or bw", "either ga or bw"), std_def := "28_weeks_OR_1000_grams"]
data[sb_inclusion_ga == 28 & sb_inclusion_bw == 1000 & sb_inclusion_preference %in% c("ga and bw", "both ga and bw", "both bw and ga"), std_def := "28_weeks_AND_1000_grams"]

data[sb_inclusion_preference == "unspecified" & is.na(std_def), std_def := "not defined"]
data[sb_inclusion_preference == "" & is.na(std_def), std_def := "not defined"]

assertable::assert_values(
  data,
  colnames = c("std_def"),
  test = "not_na"
)

# Make sure all definitions are less than 500 characters

if (nrow(data[nchar(definition) >= 500]) > 0) stop("some definitions are too long for the database")

#######################################
## Reassign composite vr definitions ##
#######################################

# Keep "Custom" definition over DYB

data[ihme_loc_id == "ARM" & source == "DYB", std_def := "22_weeks_OR_500_grams"] # values match Armenia Demographic Handbook
data[ihme_loc_id == "AUT" & source == "DYB", std_def := "500_grams"] # values match Austria Health Statistics Report
data[ihme_loc_id == "BHS" & source == "DYB", std_def := "22_weeks"] # values match Bahamas Vital Statistics Report
data[ihme_loc_id == "CUB" & source == "DYB" & year_id > 2000, std_def := "500_grams"] # values match Cuba Health Statistics Yearbook
data[ihme_loc_id == "DEU" & source == "DYB", std_def := "500_grams"] # values match LSEIG
data[ihme_loc_id == "GEO" & source == "DYB", std_def := "22_weeks"] # values match Demographic Situation in Georgia
data[ihme_loc_id == "HRV" & source == "DYB" & year_id > 2000, std_def := "22_weeks_OR_500_grams"] # values match Croatia Statistical Yearbook
data[ihme_loc_id == "LVA" & source == "DYB", std_def := "22_weeks"] # values match Latvia Number of Live Births and Stillbirths by Sex

data[ihme_loc_id == "GBR" & source == "DYB", std_def := "24_weeks"] # England, Scotland, Wales, and Northern Ireland all report this definition

# Keep "Custom" definition over WHO_HFA

data[ihme_loc_id == "LVA" & source == "WHO_HFA", std_def := "22_weeks"] # values match Latvia Number of Live Births and Stillbirths by Sex table

#############################################
## Reassign definitions when "not defined" ##
#############################################

# Save version of data before reassigning "not defined"

readr::write_csv(
  data,
  "FILEPATH"
)

# Read in and save version of expected reassignments to keep track of changes over time

new_defs <- fread(paste0(j_dir, "FILEPATH"))
readr::write_csv(
  new_defs,
  "FILEPATH"
)

# Clean reassignments file

std_defs <- get_mort_ids("std_def")
setnames(std_defs, "std_def_short", "std_def_new")

new_defs <- merge(
  new_defs,
  std_defs[, c("std_def_id", "std_def_new")],
  by = "std_def_id",
  all.x = TRUE
)
new_defs <- new_defs[, c("nid", "ihme_loc_id", "std_def_new")]

# Perform reassignment of definitions

not_defined_yes <- data[std_def %in% c("not defined", "other")]
not_defined_no <- data[!(std_def %in% c("not defined", "other"))]

not_defined_yes_updated <- merge(
  not_defined_yes,
  new_defs,
  by = c("nid", "ihme_loc_id"),
  all.x = TRUE
)

not_defined_yes_updated[!is.na(std_def_new), std_def := std_def_new]
not_defined_yes_updated$std_def_new <- NULL

data <- unique(rbind(not_defined_no, not_defined_yes_updated))

###############################
## Fixes to data definitions ##
###############################

data[ihme_loc_id == "FRA" & year_id >= 2002 & source == "Custom" & nid == 240404, std_def := "500_grams"]
data[ihme_loc_id == "FRA" & year_id >= 2002 & source == "DYB", std_def := "500_grams"]

#####################################
## Adjust vr based on completeness ##
#####################################

# For SBR:
#   1. Read in completeness file
#   2. Split data by type (vr and survey) and merge on completeness
#   3. Bind data together and subset to data without completeness value
#   4. Add completeness value for "all" data type to data
#   5. Add completeness value for india subnationals missing value by assigning national value
#   6. Replace complateness value for philippines subnationals from vr to all
#   7. Recalculate SBR

data[, year_id := as.numeric(year_id)]

# 1. Read in completeness file

comp <- fread("FILEPATH")

comp <- comp[comp != 0] # drop location-yrs when completeness is 0 (can't divide by 0)

comp_vr <- comp[source_type_id == 1]
comp_survey <- comp[source_type_id == 58]
comp_all <- comp[source_type_id == 57]

# 2. Split data by type (vr and survey) and merge on completeness

data_vr <- data[source_type %in% c("Statistical Report", "VR", "Sample Registration")]

data_vr <- merge(
  data_vr,
  comp_vr[, !c("sex_id", "source_type_id")],
  by = c("location_id", "year_id"),
  all.x = TRUE
)

data_survey <- data[source_type %in% c("Survey")]

data_survey <- merge(
  data_survey,
  comp_survey[, !c("sex_id", "source_type_id")],
  by = c("location_id", "year_id"),
  all.x = TRUE
)

data_other <- data[!(source_type %in% c("Statistical Report", "VR", "Sample Registration", "Survey"))]

# 3. Bind data together and subset to data without completeness value

data_comp <- rbind(
  data_vr,
  data_survey,
  data_other,
  fill = TRUE
)

data_comp_yes <- data_comp[!is.na(comp)]
data_comp_no <- data_comp[is.na(comp)]

# 4. Add completeness value for "all" data type to data

data_comp_no <- merge(
  data_comp_no[, !c("comp")],
  comp_all[, !c("sex_id", "source_type_id")],
  by = c("location_id", "year_id"),
  all.x = TRUE
)

data_comp <- rbind(data_comp_yes, data_comp_no)

data_comp_yes_2 <- data_comp[!is.na(comp)]
data_comp_no_2 <- data_comp[is.na(comp)]

data_comp_no_2_ind <- merge(
  data_comp_no_2[location_id %in% locs[ihme_loc_id %like% "IND"]$location_id, !c("comp")],
  comp_vr[location_id == 163, !c("location_id", "sex_id", "source_type_id")],
  by = "year_id",
  all.x = TRUE
)

data_comp <- rbind(
  data_comp_yes_2,
  data_comp_no_2[!(location_id %in% locs[ihme_loc_id %like% "IND"]$location_id)],
  data_comp_no_2_ind
)

# 6. Replace completeness values for philippines subnationals from vr to all

data_comp_phl_sub <- data_comp[ihme_loc_id %like% "PHL_"]
data_comp_no_phl_sub <- data_comp[!(ihme_loc_id %like% "PHL_")]

data_comp_phl_sub <- merge(
  data_comp_phl_sub[, !c("comp")],
  comp_all[, !c("sex_id", "source_type_id")],
  by = c("location_id", "year_id"),
  all.x = TRUE
)

data_comp <- rbind(data_comp_phl_sub, data_comp_no_phl_sub)

assertable::assert_values(data_comp, colnames = "comp", test = "not_na")

# 7. Recalculate SBR

data_comp[, sbr_unadj := sbr]
data_comp[, sbr := sbr / comp]

# Compare

adjustment <- data_comp[sbr_unadj != sbr, c("ihme_loc_id", "year_id", "std_def",
                                            "nid", "data_type", "comp",
                                            "sbr_unadj", "sbr")]
adjustment[, diff := sbr - sbr_unadj]

adjustment <- adjustment[order(adjustment$ihme_loc_id, adjustment$year_id),]

readr::write_csv(
  adjustment,
  "FILEPATH"
)

########################
## Write prepped file ##
########################

readr::write_csv(
  data_comp,
  "FILEPATH"
)
