
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: Sex-split and crosswalk heart failure data. 
##

rm(list = ls())
date <- gsub("-", "_", Sys.Date())
pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"
bundle_id <- "VALUE"
bundle_version_id <- "VALUE"
folder_root <-  "FILEPATH"

plot <- F
save_age_version <- F
write_file <- "FILEPATH"

gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

## Re-extracted definitions
self_report_nid <- 108934

## Outlier decisions
source(paste0(central, "get_location_metadata.R"))
locs <- get_location_metadata(location_set_id = "VALUE", gbd_round_id = "VALUE", decomp_step = "VALUE")
outlier_choices <- c(518, # Tibet, all zeros
                     37, # Kyrgyzstan, all zeros
                     4742, # Papua, many zeros
                     4733, # North Sulawesi, all zeros 
                     4862, # Meghalaya, many zeros
                     98, # Chile, too low
                     164, # Nepal, too low
                     142, # Iran, many zeros
                     59, # Latvia, too low
                     60, # Lithuania, too high, pulling up the region
                     102, # USA, too high
                     135, # Brazil
                     130, # Mexico
                     6, # China
                     11, # Indonesia
                     16, # Philippines
                     151, # Qatar
                     193, # Botswana
                     122, # Ecuador
                     97, # Argentina
                     35, # Georgia
                     163, # India
                     144, # India
                     91) # Portugal, too low compared to literature data in region

outlier_ids <- locs[location_id %in% outlier_choices | parent_id %in% outlier_choices, location_id]

###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))

source("/FILEPATH/master_mrbrt_crosswalk_func.R") 
source("/FILEPATH/age_split.R")


###### Pull in data
#################################################################################

data <- get_bundle_version(bundle_version_id, fetch = "all")

## Don't use the outliers or group review data points.
data <- data[is_outlier==0 & input_type != "group_review",]

## Oldest GB CI data
gb <- locs[parent_id==4749,location_id]
data <- data[!(location_id%in%gb&age_start==95)]

## Get rid of self-report data
data <- data[!(nid %in% self_report_nid)]

## Assign clinical data types.
data[clinical_data_type %in% c("inpatient"), cv_inpatient := 1]
data[is.na(cv_inpatient), cv_inpatient := 0]

## Assign ICD-coded data to "inpatient"
data[nid == 285933, cv_inpatient := 1]

## Age restrictions for HF in clincial informatics data
data <- data[!(clinical_data_type != "" & age_start < 40)]

## Outlier CI data
data[clinical_data_type == "inpatient" & !(grepl("Truven", field_citation_value)) & location_id %in% outlier_ids, is_outlier := 1]
data <- data[!(is_outlier == 1)]

## Drop data where mean = 0, fix SEs
data <- data[mean != 0]
data[standard_error > 1, standard_error := 1]

## Drop 2000 MarketScan data
data <- data[!(field_citation_value %like% "Truven" & year_start == 2000)]

data_all_types <- copy(data)

###### Crosswalk prevalence data
#################################################################################

measure <- "prevalence"
prevalence_data <- data[measure == "prevalence"]

crosswalk_holder <- master_mrbrt_crosswalk(
  model_abbr = "imp_hf", 
  data = prevalence_data, 
  storage_folder = "/FILEPATH/",
  alternate_defs = c("cv_inpatient"), 
  xw_measure = "prevalence", 
  decomp_step = "VALUE", 
  gbd_round_id = "VALUE", 
  logit_transform = T, 
  addl_x_covs = list("age_scaled"),
  age_overlap = 5,
  year_overlap = 5,
  age_range = 15,
  year_range = 15,
  remove_x_intercept = F
)

data <- crosswalk_holder$data
write.csv(data, paste0(write_file, "pre_age_split_", date, "_", measure, ".csv"))

if (save_age_version) {
  
  age_specific_data <- copy(data)[(age_end - age_start) <= 25]
  age_specific_data$crosswalk_parent_seq <- age_specific_data$seq
  age_specific_data$seq <- NA
  
  age_specific_data[location_id %in% c("VALUE"), `:=` (location_id="VALUE", location_name="VALUE", ihme_loc_id="VALUE", 
                                               note_modeler=VALUE)]
  age_specific_data[standard_error > 1, standard_error := 1]
  age_specific_data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid") := NA]
  age_specific_data[, unit_value_as_published := 1]
  age_specific_data[, c("cv_inpatient") := NULL]
  
  write.xlsx(age_specific_data, paste0(write_file, "for_age_split_model_", date, "_", measure, ".xlsx"), sheetName="extraction")
  
  save_crosswalk_version(bundle_version_id = "VALUE", 
                         data_filepath = "FILEPATH", 
                         description = "VALUE")
  
}


###### Sex-split incidence data
#################################################################################

measure <- "incidence"
incidence_data <- copy(data_all_types)[measure == "incidence"]

crosswalk_holder <- master_mrbrt_crosswalk(
  model_abbr = "imp_hf", 
  data = incidence_data, 
  storage_folder = "FILEPATH",
  xw_measure = "incidence", 
  sex_split_only = T,
  decomp_step = "VALUE", 
  gbd_round_id = "VALUE", 
  logit_transform = T, 
  addl_x_covs = c("male", "age_scaled"),
  age_overlap = 8,
  year_overlap = 8,
  age_range = 15,
  year_range = 15,
  sex_age_overlap = 5,
  sex_year_overlap = 5,
  sex_age_range = 10,
  sex_year_range = 10
)

data <- crosswalk_holder$sex_specific_data

write.csv(data, paste0(write_file, "pre_age_split_", date, "_", measure, ".csv"))

if (save_age_version) {
  
  age_specific_data <- copy(data)[(age_end - age_start) <= 25]
  age_specific_data$crosswalk_parent_seq <- age_specific_data$seq
  age_specific_data$seq <- NA
  
  age_specific_data[location_id %in% c("VALUE"), `:=` (location_id="VALUE", location_name="VALUE", ihme_loc_id="VALUE", 
                                                         note_modeler="VALUE")]
  age_specific_data[standard_error > 1, standard_error := 1]
  age_specific_data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid") := NA]
  age_specific_data[, unit_value_as_published := 1]
  age_specific_data[, c("cv_inpatient") := NULL]
  
  write.xlsx(age_specific_data, paste0(write_file, "for_age_split_model_", date, "_", measure, ".csv"), sheetName="extraction")
  
  save_crosswalk_version(bundle_version_id = bundle_version_id, 
                         data_filepath = "FILEPATH", 
                         description = "VALUE")
  
}


###### Sex-split mtwith data
#################################################################################

measure <- "mtwith"
mtwith <- copy(data_all_types)[measure == "mtwith"]

## Turn CFR into mtwith
cfr <- copy(data_all_types)[measure == "cfr" & mean < 1]
cfr[measure == "cfr" & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
cfr[measure == "cfr", mean := -(log(1-mean))]
z <- qnorm(0.975)
cfr[measure == "cfr", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
cfr[measure == "cfr", `:=` (upper = NA, lower = NA)]
cfr <- cfr[!is.nan(standard_error)]

## Update
cfr[measure == "cfr", `:=` (measure = "mtwith", note_modeler = paste0(note_modeler, " | transformed from cfr"))]

## Pull all mtwith together
mtwith <- rbind(mtwith, cfr)

crosswalk_holder <- master_mrbrt_crosswalk(
  model_abbr = "imp_hf", 
  data = mtwith, 
  storage_folder = "FILEPATH",
  xw_measure = "mtwith", 
  sex_split_only = T,
  decomp_step = "VALUE", 
  gbd_round_id = "VALUE", 
  logit_transform = T, 
  addl_x_covs = c("male", "age_scaled"),
  age_overlap = 8,
  year_overlap = 8,
  age_range = 15,
  year_range = 15
)

data <- crosswalk_holder$sex_specific_data

write.csv(data, paste0(write_file, "pre_age_split_", date, "_", measure, ".csv"))

if (save_age_version) {
  
  age_specific_data <- copy(data)[(age_end - age_start) <= 25]
  age_specific_data$crosswalk_parent_seq <- age_specific_data$seq
  age_specific_data$seq <- NA
  
  age_specific_data[location_id %in% c("VALUE"), `:=` (location_id="VALUE", location_name="VALUE", ihme_loc_id="VALUE", 
                                                             note_modeler="VALUE")]
  age_specific_data[standard_error > 1, standard_error := 1]
  age_specific_data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid") := NA]
  age_specific_data[, unit_value_as_published := 1]
  age_specific_data[, c("cv_inpatient") := NULL]
  age_specific_data[, measure := "incidence"]
  
  write.xlsx(age_specific_data, paste0(write_file, "for_age_split_model_", date, "_", measure, ".csv"), sheetName="extraction")
  
  save_crosswalk_version(bundle_version_id = bundle_version_id, 
                         data_filepath = "FILEPATH", 
                         description = "VALUE")
  
}


###### Sex-split mtstandard data
#################################################################################

measure <- "mtstandard"
mtstandard <- copy(data_all_types)[measure == "mtstandard"]

crosswalk_holder <- master_mrbrt_crosswalk(
  model_abbr = "imp_hf", 
  data = mtstandard, 
  storage_folder = "FILEPATH",
  xw_measure = "mtstandard", 
  sex_split_only = T,
  decomp_step = "VALUE", 
  gbd_round_id = "VALUE", 
  logit_transform = T, 
  addl_x_covs = c("male", "age_scaled"),
  age_overlap = 8,
  year_overlap = 8,
  age_range = 15,
  year_range = 15
)

data <- crosswalk_holder$sex_specific_data

write.csv(data, paste0(write_file, "pre_age_split_", date, "_", measure, ".csv"))

