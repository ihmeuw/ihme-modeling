##```````````````````````````````````````````````````````````````````````````````
#  This script is to prepare the data for MR-BRT from the extraction template.   #
#  This code should be adopted according to the need for each risk-outcome pair   #
#----------------------------------------------------------------------------------#


## Clean the environment
rm(list = ls())

## Load libraries
library(data.table)
library(tidyverse)
library(ggplot2)
library(openxlsx)
library(purrr)
source("FILEPATH/get_location_metadata.R")

## Edit the filepath
data_dir <- "FIlEPATH"

## Create covariates to assess the study design characteristics.
dt$representative <- ifelse(dt$rep_geography == 1, 0, 1)
dt$exp_assess_level[dt$exp_assess_level == "At the individual "] <- "At the individual"
dt$exposure_1 <- ifelse(dt$exp_assess_level == "At the individual", 0, 1)
dt$exposure_2 <- ifelse(dt$exp_method_1 == "Self-report (human/environment)", 0, 1)
dt$exposure_3 <- ifelse(dt$exp_assess_period == "only at baseline", 1, 0)
dt$outcome_1 <- ifelse(dt$outcome_assess_1 == "Self-report", 1, 0)
dt$outcome_2 <- "0"
dt$confounder_1 <- ifelse(dt$design %in% c("Prospective cohort", "prospective cohort", "case-cohort", "Nested case-control"), 1, 0)
dt$cov_incidence_only <- ifelse(dt$outcome_type %in% c("Incidence"), 1, 0)
dt$cov_mortality_only <- ifelse(dt$outcome_type %in% c("Mortality"), 1, 0)
dt$cov_incidence_mortality_together <- ifelse(dt$outcome_type %in% c("Incidence & Mortality", "Incidence, mortality", "Incidence and mortality"), 1, 0)
dt$cov_odds_ratio <- ifelse(dt$effect_size_measure == "Odds ratio (OR)", 1, 0)

## Adjust certain variables
dt <- dt %>%
  mutate(outcome_2 = 0, reverse_causation = 1, washout_years = NA, seq = NA, selection_bias = NA)

## Make the effect size and SE variables as numeric
dt$effect_size <- as.numeric(dt$effect_size)
dt$upper <- as.numeric(dt$upper)
dt$lower <- as.numeric(dt$lower)

## Create the ln_effect and ln_se variables
dt <- dt %>%
  mutate(ln_effect = log(effect_size), ln_se = (log(upper) - log(lower)) / 3.92)

## create a bias covariate for the follow-up duration
dt$value_of_duration_fup <- as.numeric(dt$value_of_duration_fup)
dt <- dt[, cov_follow_up := ifelse(value_of_duration_fup > 10, 1, 0)]

## Rename the effect size and dose response columns as required by the BoP pipeline
setnames(dt, old = c("ln_effect", "ln_se", "b_0", "b_1", "a_0", "a_1"), new = c("ln_rr", "ln_rr_se", "alt_risk_lower", "alt_risk_upper", "ref_risk_lower", "ref_risk_upper"))

## Convert selected columns to numeric
columns_to_convert <- c("ln_rr", "ln_rr_se", "ref_risk_lower", "ref_risk_upper", "alt_risk_lower", "alt_risk_upper")
dt[, (columns_to_convert) := lapply(.SD, as.numeric), .SDcols = columns_to_convert]

## Rename the bias covariates according to BoP guideline: 

names(dt) <- gsub("^(cofounder|confounder|confounders)", "cov", names(dt))  ### list all potential names used in the extraction template for confounders columns

dt <- dt[, measure := "relrisk"]

## Prepare the data for bundle upload
dt$sex <- ifelse(dt$percent_male == 1, "Male", ifelse(dt$percent_male == 0, "Female", "Both"))
dt$cov_exposure_definition <- ifelse(dt$Exposure_definition_reported == 1, 1, 0)
dt$cov_outcome_def <- ifelse(dt$outcome_mapping == "aggregate", 0, 1)
dt$cov_outcome_def[is.na(dt$cov_outcome_def)] <- 0  ## Set the missing values to 0

## Format the dataset for upload
dt$design[dt$design == "Prospective cohort"] <- "prospective cohort"
dt$design[dt$design == "case-cohort"] <- "case-cohort"
dt$design[dt$design == "Nested case-control"] <- "nested case-control"
dt$effect_size_measure[dt$effect_size_measure == "Hazard ratio (HR)"] <- "hazard ratio"
dt$effect_size_measure[dt$effect_size_measure == "Relative risk (RR)"] <- "relative risk"
dt$effect_size_measure[dt$effect_size_measure == "Odds ratio (OR)"] <- "odds ratio"
setnames(dt, c("year_end_study", "year_start_study", "effect_size"), c("year_end", "year_start", "mean"))

## Get the location data
loc <- get_location_metadata(location_set_id = 35, release_id = 9)
loc$location_id <- as.numeric(loc$location_id)
dt$location_id <- as.numeric(dt$location_id)

## Merge
dt_final <- merge(dt, loc, by = c("location_id", "location_name"), all.x = TRUE)

## Save the dataset
write_csv(dt_final, "FILEPATH/FILENAME.csv")

