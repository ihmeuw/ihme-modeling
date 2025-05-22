
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: Age-split and upload
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"
write_file <- "FILEPATH"
crosswalk_date <- "VALUE"
mvid <- "VALUE"


###### Functions
#################################################################################

for (i in list.files(central)) source(paste0(central, i))
source("FILEPATH/age_split.R")

prevalence_data <- fread(paste0(write_file, "pre_age_split_", crosswalk_date, "_prevalence_no_icd.csv"))
prevalence_data[grepl("crosswalked|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
prevalence_data[!is.na(crosswalk_parent_seq), seq := NA]
prevalence_data[, c("reference", "age_mid", "year_mid") := NULL]
prevalence_data$V1 <- NULL
prevalence_data$lower <- NA
prevalence_data$upper <- NA

## Using updated age-splitting technique to break into 10-year age groups. First 5, then 10, then incl <1 case
prevalence_data_agesplit <- age_split(df = prevalence_data, 
                                      model_id = "VALUE",
                                      decomp_step = "VALUE", 
                                      gbd_round_id = "VALUE", 
                                      measure = "prevalence", 
                                      global_age_pattern = T,
                                      model_version_id = mvid, 
                                      gbd_age_groups = F,
                                      drop_under_1_case = T,
                                      age_bin_size = 5)
prevalence_data_agesplit$lower <- NA
prevalence_data_agesplit$upper <- NA

prevalence_data_agesplit2 <- age_split(df = prevalence_data_agesplit[age_end-age_start>25], 
                                      model_id = "VALUE",
                                      decomp_step = "VALUE", 
                                      gbd_round_id = "VALUE", 
                                      measure = "prevalence", 
                                      global_age_pattern = T,
                                      model_version_id = mvid, 
                                      gbd_age_groups = F,
                                      drop_under_1_case = F,
                                      age_bin_size = 10)

prevalence_data_agesplit <- rbind(prevalence_data_agesplit[age_end-age_start<25],prevalence_data_agesplit2)

incidence_data <- fread(paste0(write_file, "pre_age_split_", crosswalk_date, "_incidence.csv"))
incidence_data[grepl("crosswalked|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
incidence_data[!is.na(crosswalk_parent_seq), seq := NA]
incidence_data[, c("reference", "age_mid", "year_mid") := NULL]
incidence_data$V1 <- NULL

incidence_data_agesplit <- age_split(df = incidence_data, 
                                      model_id = "VALUE",
                                      decomp_step = "VALUE", 
                                      gbd_round_id = "VALUE", 
                                      measure = "incidence", 
                                      global_age_pattern = T,
                                      model_version_id = mvid, 
                                      gbd_age_groups = F,
                                      drop_under_1_case = T,
                                      age_bin_size = 10)

incidence_data_agesplit2 <- age_split(df = incidence_data_agesplit[age_end-age_start>25], 
                                     model_id = "VALUE",
                                     decomp_step = "VALUE", 
                                     gbd_round_id = "VALUE", 
                                     measure = "incidence", 
                                     global_age_pattern = T,
                                     model_version_id = mvid, 
                                     gbd_age_groups = F,
                                     drop_under_1_case = F,
                                     age_bin_size = 10)
incidence_data_agesplit <- rbind(incidence_data_agesplit[age_end-age_start<25],incidence_data_agesplit2)


mtwith_data <- fread(paste0(write_file, "pre_age_split_", crosswalk_date, "_mtwith.csv"))
mtwith_data[grepl("crosswalked|sex split with female/male ratio", note_modeler), crosswalk_parent_seq := seq]
mtwith_data[!is.na(crosswalk_parent_seq), seq := NA]
mtwith_data[, c("reference", "age_mid", "year_mid") := NULL]
mtwith_data[, c("cases", "sample_size") := NA]
mtwith_data$V1 <- NULL

mtwith_data_agesplit <- age_split(df = mtwith_data, 
                                     model_id = "VALUE",
                                     decomp_step = "VALUE", 
                                     gbd_round_id = "VALUE", 
                                     measure = "mtwith", 
                                     global_age_pattern = T,
                                     model_version_id = mvid, 
                                     gbd_age_groups = F,
                                     drop_under_1_case = T,
                                     age_bin_size = 10)

mtwith_data_agesplit2 <- age_split(df = mtwith_data_agesplit[age_end-age_start>25], 
                                  model_id = "VALUE",
                                  decomp_step = "VALUE", 
                                  gbd_round_id = "VALUE", 
                                  measure = "mtwith", 
                                  global_age_pattern = T,
                                  model_version_id = mvid, 
                                  gbd_age_groups = F,
                                  drop_under_1_case = F,
                                  age_bin_size = 10)
mtwith_data_agesplit <- rbind(mtwith_data_agesplit[age_end-age_start<25],mtwith_data_agesplit2)
mtwith_data_agesplit <- mtwith_data_agesplit[mean<1]
mtwith_data_agesplit <- mtwith_data_agesplit[standard_error<=1]

data <- rbindlist(list(prevalence_data_agesplit, incidence_data_agesplit, mtwith_data_agesplit),use.names = T)
data <- data[!(location_name=="Australia"&measure%in%c("prevalence","incidence"))]
data <- data[!(location_name=="France"&measure%in%c("prevalence","incidence"))]

data <- data[!(cv_icd==1)]


data[standard_error > 1, standard_error := 1]
data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid", "seq") := NA]
data[, unit_value_as_published := 1]
#data[, c("cv_inpatient") := NULL]

write.csv(data, paste0("FILEPATH/pah_agesplit_", date, "_new2.csv"))
save_crosswalk_version(bundle_version_id = "VALUE", 
                       data_filepath = paste0("FILEPATH/pah_agesplit_", date, "_new2.xlsx"), 
                       description = "Sex- and age-split PAH data, lit only, no ICD, no AUS, no FRA, cor DMK")








