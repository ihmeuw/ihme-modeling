# This code estimates the relative risk (RR) for the association between HIV and iNTS infection.
# We use the RR estimates produced here, plus HIV prevalence estimates, to calculate the population 
# attributable fraction (PAF) of iNTS disease due to HIV; and then use that PAF to split iNTS cases into
# those that are HIV attributable vs those that are not.  This allows us to apply different case-fatality
# estimates to the two groups (case fatality is much higher among those with HIV), and reattribute the HIV-
# attributable iNTS deaths to HIV in the formal GBD CoD pipeline, as that is the underlying cause of death.
# Most of the code here is data-wrangling -- primarily to reconcile the varying age ranges in the source data
# with the fixed age-ranges of GBD age-groups.  The actual RR estimation is done in the last few lines of code
# using a negative binomal GLM.
#
# Author: AUTHOR
# Created: 06 June 2018
# Last modified: 10 March 2024
#
# The code was updated in 3/2024 as part of in-cycle cause updates, to improve robustness, readability, and efficiency.
# I made no changes in the fundamental methodological approach.
#
# Libraries: data.table, dplyr, MASS, and openxlsx; ggplot2 for plotting only; sources GBD shared functions 
# Input data: this reads in the extraction sheet from the systematic review of scientific lit with case-fatality data
# Outputs: this saves a .csv file with the RR estimates for every GBD location, year, age, and sex to be used by enteric_splity.py


rm(list = ls())

library(data.table)
library(dplyr)
library(ggplot2)
library(MASS)
library(openxlsx, lib.loc = 'FILEPATH')

SHARED_FUN_DIR <- 'FILEPATH'

source(file.path(SHARED_FUN_DIR, 'get_age_metadata.R'))
source(file.path(SHARED_FUN_DIR, 'get_covariate_estimates.R'))
source(file.path(SHARED_FUN_DIR, 'get_location_metadata.R'))
source(file.path(SHARED_FUN_DIR, 'get_population.R'))


user <- Sys.getenv("USER")

# Set the release number
RELEASE <- 16

# Set up directories and file names
IN_DIR <- file.path('FILEPATH', paste0('release_', RELEASE), 'data')  
OUT_DIR <- file.path('FILEPATH', 'ints', paste0('release_', RELEASE), 'inputs')
DATA_FILE <- FILEPATH

# Get age and location metadata
age_meta <- get_age_metadata(release_id = RELEASE
                             )[, .(age_group_id, age_group_years_start, 
                                   age_group_years_end)]
loc_meta <- get_location_metadata(location_set_id = 35, release_id = RELEASE)

# Load extraction sheet with HIV RR data #
# variable descriptions in row 2, we'll treat row 2 as col names 
# (skipping that row leads to dropped cols), and read in col names below
data <- readWorkbook(DATA_FILE, sheet = 'extraction', colNames = TRUE, 
                     skipEmptyCols = FALSE, startRow = 2)
setDT(data)

# Now read in again to get only column names
names(data) <- as.character(readWorkbook(DATA_FILE, sheet = 'extraction', 
                                         rows = 1, colNames = FALSE))

# Retain only rows with HIV RR data
data <- data[measure == 'relrisk' & coinfection == 'hiv', ]

# Citation data stored across two fields, combine here (and convert values to integer)
data[, nid := as.integer(nid)][is.na(nid), nid := as.integer(field_citation_value)]

# Keep only needed columns
data <- data[, .(nid, location_id, year_start, year_end, sex, age_start, 
                 age_end, age_demographer, mean, lower, upper)]



# We need to merge covariate estimates with input data. Since covariate estimates exist for GBD age_groups, and 
# input data can be for any possible age ranges, we need to approximate the proportion of people represented by each
# data point in each GBD age group. We'll do this by finding the proportion of the input data point age range that
# overlaps with each GBD age group, and using the product of that proportion and the corresponding population 
# as a weight to take a weighted average across groups.

# Cross join input data with age/sex metadata to get all combinations of data and age/sex groups
age_sex <- rbind(copy(age_meta)[, sex_id := 1], 
                 copy(age_meta)[, sex_id := 2])[, merge := 1]
data <- merge(data[, `:=` (seq = 1:.N, merge = 1)], age_sex, by = 'merge', 
              all = T, allow.cartesian = T)[, merge := NULL]

# Convert age_end to age_end + a day under 1 year if age_demographer == 1
data[, age_end := age_end + (age_demographer * 364/365)] 

# Calculate the overlap between the input data age range and the GBD age group
data[, overlap_start := pmax(age_start, age_group_years_start)]
data[, overlap_end := pmin(age_end, age_group_years_end)]
data[, overlap := pmax(0, overlap_end - overlap_start)]

# Calculate the proportion of the input data age range that overlaps with the GBD age group
data[, age_group_duration := age_group_years_end - age_group_years_start]
data[, pr_in_age_group := overlap / age_group_duration]

# Reconcile sex-specific data (we made male and female rows for each data point, 
# so we want to drop rows where source was sex-specific and we have a mismatch)
data <- data[sex == 'Both' | (sex == 'Male' & sex_id == 1) |
               (sex == 'Female' & sex_id == 2), ]

# We'll use the mid-point year of the study as the year_id for merging with population & covariate data
data[, year_id := as.integer(floor((year_start + year_end) / 2))]

# Get population data
pop <- get_population(location_id = unique(data$location_id), age_group_id = -1, 
                      sex_id = 1:3, 
                      year_id = min(data$year_start):max(data$year_end), 
                      release_id = RELEASE)[, run_id := NULL]

data <- merge(data, pop, 
              by = c('location_id', 'year_id', 'age_group_id', 'sex_id'), 
              all.x = T)

# Calculate weight & drop intermediate variable used to calculate it that are no longer needed
data[, weight := population * pr_in_age_group]
data[, c('age_group_years_start', 'age_group_years_end', 'age_group_duration', 
         'pr_in_age_group', 'overlap_start', 'overlap_end', 'overlap', 
         'population', 'age_demographer') := NULL]


# Load covariate data (load for all locations, years, ages, sexes, to use as a prediction table after modelling)
cov <- get_covariate_estimates(486, release_id = RELEASE)
cov_name <- cov$covariate_name_short[1] # get the covariate name (use to rename value variables)
cov[, c('model_version_id', 'covariate_name_short', 'covariate_id', 
        'lower_value', 'upper_value', 'sex') := NULL] # drop unneeded columns
setnames(cov, 'mean_value', cov_name) # rename value variable

# Merge the covariate values with the data, and get weighted means across age groups for each data point
data <- merge(data, cov, by = c('location_id', 'year_id', 'age_group_id', 'sex_id'), all.x = TRUE)
data <- data[, .(SEV_scalar_diarrhea = weighted.mean(SEV_scalar_diarrhea, w = weight)), 
             by = c('seq', 'nid', 'location_id', 'year_id', 'age_start', 
                    'age_end', 'sex', 'mean', 'lower', 'upper')]



# Visually inspect association between diarrhea SEVs and HIV RRs #
data %>% ggplot(aes(x = SEV_scalar_diarrhea, y = log(mean + 1))) + 
  geom_smooth(method = 'lm') + geom_point() + theme_minimal()


# Create full dataset for modelling and prediction #
cov <- merge(cov, age_meta, by = 'age_group_id', all.x = T)
full <- rbind(data, cov, fill = T)
full[, reGroup := as.factor(ifelse(is.na(nid) == F, as.character(nid), 
                                   'prediction'))]


# Find mid-points of age groups #
full[, age_mid := (age_start + age_end) / 2]
full[is.na(age_mid), age_mid := (age_group_years_start + age_group_years_end) / 2]
full[age_mid <= 3, age_mid := 3]   # truncating to avoid extreme extrapolations to youngest age_groups

# the following replacements were either extracted or estimated from articles
full[nid == 317961, age_mid := 4.6]
full[nid == 317971, age_mid := 32] 
full[nid == 317972, age_mid := 36] 
full[nid == 317977 & age_start < 10, age_mid := 15/12] 
full[nid == 317977 & age_start > 10, age_mid := 30]
full[nid == 318009, age_mid := 37]


# Run the model to predict HIV RRs from diarrhea SEVs and age #
# We want to ensure that all RR estimates are greater than 1.0 (i.e. disallow the possibility of protective effects)
# To do this, we're actually modelling the excess risk (RR - 1) rather than the relative risk, using a negative binomial model.
# This ensures that the predicted RRs will be greater than 1.0 (i.e. exp(x) must be > 0 & exp(x)+1 > 1.0)

(mdl <- glm.nb(mean-1 ~ log(SEV_scalar_diarrhea) + log(age_mid), data = full))

preds <- predict(mdl, newdata = full, se.fit = T)

preds_out <- cbind(full, 
                   data.table(log_hiv_excess_risk = log(as.numeric(preds$fit)), 
                              log_hiv_excess_risk_se = as.numeric(preds$se.fit)))

preds_out <- preds_out[reGroup == 'prediction', 
                       ][, .(location_id, year_id, age_group_id, sex_id, 
                             log_hiv_excess_risk, log_hiv_excess_risk_se)]


# Save predictions to file #
write.csv(preds_out, file.path(OUT_DIR, 'ints_hiv_rr_estimates.csv'), row.names = F)
