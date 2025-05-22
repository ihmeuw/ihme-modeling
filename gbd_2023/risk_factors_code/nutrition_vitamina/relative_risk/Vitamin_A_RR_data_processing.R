# -------------------------------------
# Title: "Prep Vitamin A RR data for BoP model"
# Authors:
#  Date:
#
# Goal: Read the vitamin A extraction data and format it into a structure 
#       that is ready for the Burden of Proof model.
# Input: Vitamin A extraction data
# Output: BoP model-ready formatted data
#
# Clear the workspace
rm(list = ls())

# Load the required libraries
library(data.table)
library(tidyverse)
library(ggplot2)
library(openxlsx)

# Source necessary functions from a predefined library path
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/validate_input_sheet.R")
source("FILEPATH/get_draws.R")

# Retrieve operating system information and the current user
os <- Sys.info()[1]
user <- Sys.info()[7]

# Define the data directory path
data_dir <- "FILEPATH/VA_RR_extracted_data_version.csv"

# Read the data into a data.table
dt <- fread(data_dir)

# Add a sequential identifier
dt[, seq := .I]

# Make a copy of the data table for later use
dt_copy <- copy(dt)

# Exclude certain studies based on 'nid' values for specific reasons:
# Reasons include evaluating adverse effects of vitamin A supplementation and studies on diarrhea as a symptom of specific diseases
excluded_nids <- c(402772, 165676, 165682)
dt <- dt[!nid %in% excluded_nids]

# Remove rows with missing 'mean' values
# Note: Initial comments mentioned 'log_se' or 'log_effect_size', but the code checks for 'mean'
dt <- dt[!is.na(mean)]

# Convert follow-up duration to months for consistency
dt[duration_fup_units == "years", value_of_duration_fup_months := duration_fup * 12]
dt[duration_fup_units == "weeks", value_of_duration_fup_months := duration_fup / 4.3]
dt[duration_fup_units == "days", value_of_duration_fup_months := duration_fup / 30]
dt[duration_fup_units == "months", value_of_duration_fup_months := duration_fup / 30]

# Rename columns to more descriptive names
new_col_names <- c("include_infants_below_6_months", "include_infants_below_12_months",
                   "supplementation_interval", "VAS_alone_vs_other",
                   "duration_intervention", "outcome_definition_reported",
                   "incidence", "method_outcome_assessed", 
                   "intervention_type", "mortality")
old_col_names <- c("bc_6", "bc_12", "bc_supp_interval", "bc_supp_type",
                   "bc_duration_covered", "bc_diarrhea_definition",
                   "bc_incidence", "bc_method_outcome_assessed",
                   "bc_intervention_type", "bc_mortality")
setnames(dt, old_col_names, new_col_names)

# Add required variables for the bundle upload
dt[, standard_error := (upper - lower) / 3.92]
dt[, source_type := "Clinical trial"]
dt[, input_type := NA]
setnames(dt, c("age_start_study", "age_end_study"), c("age_start", "age_end"))

# Fix age start and age end values for some nids 
dt$age_start[dt$age_start == "182/365"] <- 0.498
dt[, age_start := ifelse(nid == 94493 & outcome == "Measles" & is.na(age_start), 0, age_start)]
dt[, age_end := ifelse(nid == 94493 & outcome == "Measles" & is.na(age_end), 10, age_end)]
dt[, age_start := ifelse(nid == 94516 & outcome == "Diarrhea" & is.na(age_start), 0, age_start)]
dt[, age_end := ifelse(nid == 94516 & outcome == "Diarrhea" & is.na(age_end), 10, age_end)]
dt[, age_start := ifelse(nid == 370214 & outcome == "Diarrhea" & is.na(age_start), 0, age_start)]
dt[, age_end := ifelse(nid == 370214 & outcome == "Diarrhea" & is.na(age_end), 10, age_end)]

# Convert protective RR into harmful RR
dt[, `:=`(RR_a1 = 1 / mean, RR_u1 = 1 / lower, RR_l1 = 1 / upper)]

# Log-transform RR and standard error
dt[, `:=`(ln_rr = log(RR_a1), ln_rr_se = (log(RR_u1) - log(RR_l1)) / 3.92)]

# Estimate prevalence of background vitamin A deficiency for trials without reported prevalence
# (The following script block simulates gamma distributions to estimate vitamin A deficiency prevalence for specific nids)

# nid_108889


mean = 1.443 #   
SD = 0.39
var = SD^2

a= mean^2/var 
b=  mean/var 

nid_108889 <- rgamma(1000, a, b)


#nid  165686

mean = 0.67
SD = 0.242
var =SD^2
a = mean^2/var
b = mean/var

nid_165686 <- rgamma(1000, a, b)


#nid 370219

mean = 1.145
SD = 0.285
var =0.285^2
a = mean^2/var
b = mean/var

nid_370219 <- rgamma(1000, a, b)


# Compile the data
gamma <- as.data.frame(cbind(nid_108889, nid_165686, nid_370219))

# Estimate the prevalence of vitamin A deficiency


## Estimate the prevalence of vitamin A deficiency:

## nid  108889

prop_108889 <- sum(gamma$nid_108889 < 0.70)/1000 *100 
rr_va_108889 <- dt[nid ==108889,]


## replace the na values of VAD with the gamma estimated prevalence of vitamin A deficiency

rr_va_108889 <- rr_va_108889 %>% mutate(VAD = ifelse(is.na(VAD), 0.063, VAD))


## nid 165686

prop_165686 <- sum(gamma$nid_165686 < 0.70)/1000 *100  # 62.3
rr_va_165686 <- dt[nid ==165686,]


# replace the na values of VAD with the gamma estimated prevalence of vitamin A deficiency

rr_va_165686 <- rr_va_165686 %>% mutate(VAD = ifelse(is.na(VAD), 0.623, VAD))


## nid  370219

prop_370219 <- sum(gamma$nid_370219 < 0.70)/1000 *100  # 4.8
rr_va_370219 <- dt[nid ==370219,]


## replace the na values of VAD with the gamma estimated prevalence of vitamin A deficiency

rr_va_370219 <- rr_va_370219 %>% mutate(VAD = ifelse(is.na(VAD), 0.048, VAD))


## rbind the gamma estimated prevalence of deficiency with the orginal extractonsheet

rr_va1 = dt[!nid %in% c(108889,165686, 370219), ]
rr_va2 = rbind(rr_va1, rr_va_370219, rr_va_108889, rr_va_108889)

## For studies with no data on prevalence of vitamin A deficiency, use data from the GBD 2021 estimates matched with the year of survey.

## If it was before year 1990, we used GBD 2021 estimates of year_id 1990.

## Getting GBD 2021 estimates

## Arguments

location_id =c(522, 4873, 4875, 16) # location where we could not find prevalence of vitamin A deficiecy data from the study
year_id = c(1991) # Years of interest. Most of the VAS studies were conducted in early 90's. 


dt_g <- get_draws(gbd_id_type = "modelable_entity_id",
                  gbd_id = 2510,
                  release_id = 9,
                  year_id=year_id,
                  location_id = location_id,
                  sex_id= 3,
                  age_group_id = 5,
                  source = "epi")


draws <- paste0("draw_", 0:999)


dt_g[, "mean" := apply(.SD, 1, mean, na.rm = T), .SDcols=paste0(draws)]
dt_g  <-   dt_g[, .(location_id, year_id, age_group_id, sex_id, mean)]


## assign  the GBD 2021 estimates to the studies with no data on prevalence of vitamin A deficiency

rr_va2$VAD[rr_va2$VAD =="522"] <- dt_g[location_id ==522,]$mean
rr_va2$VAD[rr_va2$VAD =="4873"] <- dt_g[location_id ==4873,]$mean
rr_va2$VAD[rr_va2$VAD =="4875"] <- dt_g[location_id ==4875,]$mean
rr_va2$VAD[rr_va2$VAD =="16"] <- dt_g[location_id ==16,]$mean


## save data

dt = rr_va2


## Create a bias covariate to account for the effect of background prevalence of deficiency

dt$cov_vad_50 <- ifelse(dt$VAD > 0.50, 1, 0)  # Dichotomize studies based on 50% prevalence of vitamin A deficiency
dt$cov_vad_30 <- ifelse(dt$VAD > 0.30, 1, 0)  # Dichotomize studies based on 30% prevalence of vitamin A deficiency
dt$cov_vad_20 <- ifelse(dt$VAD > 0.20, 1, 0)  # Dichotomize studies based on 20% prevalence of vitamin A deficiency


## Save the cleaned and renamed data

output_file_path <- "FILEPATH/VAD_RR_bundle_data.xlsx"
write.xlsx(dt, output_file_path, sheetName = "extraction")

