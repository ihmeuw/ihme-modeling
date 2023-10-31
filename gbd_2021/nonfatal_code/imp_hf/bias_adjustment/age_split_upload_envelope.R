
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: Age-split post-crosswalk HF data, save as crosswalk version
##

date <- gsub("-", "_", Sys.Date())
pacman::p_load(data.table, ggplot2, doBy)


###### Paths, args
#################################################################################

central <- "FILEPATH"

gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

bundle_version_id <- "VALUE"

crosswalk_path <- "FILEPATH"
crosswalk_date <- "VALUE"

prev_mvid <- "VALUE"
inc_mvid <- "VALUE"
mtwith_mvid <- "VALUE"
  

###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))

source("/FILEPATH/model_helper_functions.R")
source("/FILEPATH/master_mrbrt_crosswalk_func.R")
source("/FILEPATH/age_split.R")

###### Pull in data, age-split
#################################################################################

prevalence_data <- fread(paste0(crosswalk_path, "pre_age_split_", crosswalk_date, "_prevalence.csv"))
prevalence_data[grepl("crosswalked|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
prevalence_data[!is.na(crosswalk_parent_seq), seq := NA]
prevalence_data[, c("reference", "age_mid", "year_mid") := NULL]

prevalence_data[nid == 285933, is_outlier := 1]

## Using updated age-splitting technique to break into 10-year age groups
prevalence_data_agesplit <- age_split(df = prevalence_data, 
                                      model_id = "VALUE",
                                      decomp_step = decomp_step, 
                                      gbd_round_id = gbd_round_id, 
                                      measure = "prevalence", 
                                      global_age_pattern = T,
                                      model_version_id = prev_mvid, 
                                      gbd_age_groups = F,
                                      age_bin_size = 10)


incidence_data <- fread(paste0(crosswalk_path, "pre_age_split_", crosswalk_date, "_incidence.csv"))
incidence_data[grepl("crosswalked|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
incidence_data[!is.na(crosswalk_parent_seq), seq := NA]

## Splitting into age 20+ because of poor fit in 10-14 age group
incidence_data_agesplit <- age_split(df = incidence_data, 
                                      model_id = "VALUE",
                                      decomp_step = decomp_step, 
                                      gbd_round_id = gbd_round_id, 
                                      measure = "incidence", 
                                      global_age_pattern = T,
                                      model_version_id = inc_mvid, 
                                      gbd_age_groups = F, 
                                      age_bin_size = 10)

mtwith_data <- fread(paste0(crosswalk_path, "pre_age_split_", crosswalk_date, "_mtwith.csv"))
mtwith_data[grepl("crosswalked|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
mtwith_data[!is.na(crosswalk_parent_seq), seq := NA]

## Splitting into age 20+ because of poor fit in 10-14 age group
mtwith_data_agesplit <- age_split(df = mtwith_data, 
                                      model_id = "VALUE",
                                      decomp_step = decomp_step, 
                                      gbd_round_id = gbd_round_id, 
                                      measure = "incidence", 
                                      global_age_pattern = T,
                                      model_version_id = mtwith_mvid,
                                      gbd_age_groups = F,
                                      age_bin_size = 10)

mtstandard_data <- fread(paste0(crosswalk_path, "pre_age_split_", crosswalk_date, "_mtstandard.csv"))
mtstandard_data[grepl("crosswalked|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
mtstandard_data[!is.na(crosswalk_parent_seq), seq := NA]


data <- rbindlist(list(prevalence_data_agesplit, incidence_data_agesplit, mtwith_data_agesplit, mtstandard_data))

### REMOVE BRAZIL DATA POINT ###
data[nid == 439196, is_outlier := 1]

## REMOVE CHINA MTWITH DATA POINT
china_id <- locations[location_name=="China",location_id]
locs <- locations[parent_id==china_id|location_id==china_id,location_id]
data[location_id%in%locs&measure=="mtwith"&note_modeler%like%"age-split using Dismod",is_outlier:=1]

### REMOVE VIETNAM CLINICAL INF DATA ###
data[location_id == 20 & clinical_data_type == "inpatient", is_outlier := 1]

###### MAD filter
#################################################################################

mad_p <- summaryBy(data = data[measure == "prevalence"], mean ~ age_start + sex, FUN = mad.stats)
mad_p$mad.upper <- with(mad_p, ifelse(age_start<80, mean.median + 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median + 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median + 4*mean.mad, NA))))
mad_p$mad.lower <- with(mad_p, ifelse(age_start<80, mean.median - 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median - 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median - 4*mean.mad, NA))))
mad_p$measure <- "prevalence"
data <- merge(data, mad_p, by=c("age_start", "sex", "measure"), all.x = T)
data[measure == "prevalence" & (mean > mad.upper | mean < mad.lower), 
     `:=` (is_outlier = 1, note_modeler = paste0(note_modeler, " | MAD filtered"))]
data[, c("mad.upper", "mad.lower") := NULL]

mad_i <- summaryBy(data = data[measure == "incidence"], mean ~ age_start + sex, FUN = mad.stats)
mad_i$mad.upper <- with(mad_i, ifelse(age_start<80, mean.median + 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median + 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median + 4*mean.mad, NA))))
mad_i$mad.lower <- with(mad_i, ifelse(age_start<80, mean.median - 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median - 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median - 4*mean.mad, NA))))
mad_i$measure <- "incidence"
data <- merge(data, mad_i, by=c("age_start", "sex", "measure"), all.x = T)
data[measure == "incidence" & (mean > mad.upper | mean < mad.lower), 
     `:=` (is_outlier = 1, note_modeler = paste0(note_modeler, " | MAD filtered"))]
data[, c("mad.upper", "mad.lower") := NULL]

mad_m <- summaryBy(data = data[measure == "mtwith"], mean ~ age_start + sex, FUN = mad.stats)
mad_m$mad.upper <- with(mad_m, ifelse(age_start<80, mean.median + 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median + 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median + 4*mean.mad, NA))))
mad_m$mad.lower <- with(mad_m, ifelse(age_start<80, mean.median - 5*mean.mad, 
                                      ifelse(age_start>=80 & age_start<90, mean.median - 5*mean.mad, 
                                             ifelse(age_start>=90, mean.median - 4*mean.mad, NA))))
mad_m$measure <- "mtwith"
data <- merge(data, mad_m, by=c("age_start", "sex", "measure"), all.x = T)
data[measure == "mtwith" & (mean > mad.upper | mean < mad.lower), 
     `:=` (is_outlier = 1, note_modeler = paste0(note_modeler, " | MAD filtered"))]
data[, c("mad.upper", "mad.lower") := NULL]

data[, names(data)[grepl("mean.", names(data))] := NULL]
data[cases < 0, `:=` (cases = NA, sample_size = NA)]

mad_filtering <- rbind(mad_p, mad_i, mad_m)
write.csv(mad_filtering, paste0(crosswalk_path, "mad_filtering_", date, ".csv"))


###### Save as crosswalk version
#################################################################################

data[location_id %in% c("VALUE"), `:=` (location_id="VALUE", location_name="VALUE", ihme_loc_id="VALUE", 
                                                           note_modeler="VALUE")]

data[standard_error > 1, standard_error := 1]
data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid") := NA]
data[, unit_value_as_published := 1]
data[, c("cv_inpatient") := NULL]

write.xlsx(data, paste0("/FILEPATH/crosswalked_agesplit_", date, ".xlsx"), sheetName = "extraction")
save_crosswalk_version(bundle_version_id = bundle_version_id, 
                       data_filepath = paste0("/FILEPATH/crosswalked_agesplit_", date, ".xlsx"), 
                       description = "VALUE")

















