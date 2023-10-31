
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: Age-split NRVDs
##

rm(list=ls())
date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"

gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

mes <- data.table(
  
  me_names = c("aort", "mitral"),
  me_name_long = c("CAVD", "DMVD"),
  bundle_id = c("VALUES"),
  modelable_entity = c("VALUES"),
  bundle_version_id = c("VALUES"),
  acause = c("cvd_valvu_aort", "cvd_valvu_mitral"),
  age_split_meid = c("VALUES")
   
)

write_file <- paste0("/FILEPATH/")


###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))
source("/FILEPATH/age_split.R")


###### Age-split, save
#################################################################################

data <- fread(paste0(write_file, "all_age_", me, "_", date, ".csv"))
data[, c("reference", "age_mid", "year_mid") := NULL]
data[grepl("crosswalked|sex split with female/male ratio|duplicated from both-sex data", note_modeler), crosswalk_parent_seq := seq]

prevalence_data <- copy(data)[measure == "prevalence",]

prevalence_data_agesplit <- age_split(df = prevalence_data, 
                                      model_id = mes[me_names==me, modelable_entity],
                                      decomp_step = decomp_step, 
                                      gbd_round_id = gbd_round_id, 
                                      measure = "prevalence", 
                                      global_age_pattern = T,
                                      model_version_id = mes[me_names==me, age_split_meid])
prevalence_data_agesplit[!is.na(crosswalk_parent_seq), seq := NA]
prevalence_data_agesplit <- prevalence_data_agesplit[!(grepl("age-split using Dismod", note_modeler) & (age_start < 20))]

## Transform CFR into mtwith
cfr <- copy(data)[measure == "cfr" & mean < 1]
cfr[measure == "cfr" & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
cfr[measure == "cfr", mean := -(log(1-mean))]
z <- qnorm(0.975)
cfr[measure == "cfr", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
cfr[measure == "cfr", `:=` (upper = NA, lower = NA)]
cfr <- cfr[!is.nan(standard_error)]

## Update
cfr[measure == "cfr", `:=` (measure = "mtwith", note_modeler = paste0(note_modeler, " | transformed from cfr"))]

## Pull all mtwith together
mtwith <- rbind(copy(data)[measure == "mtwith",], cfr)

## Age-split mtwith
mtwith <- copy(data)[measure == "mtwith",]
mtwith_data_agesplit <- age_split(df = mtwith, 
                                  model_id = mes[me_names==me, modelable_entity],
                                  decomp_step = decomp_step, 
                                  gbd_round_id = gbd_round_id, 
                                  measure = "mtwith", 
                                  global_age_pattern = T,
                                  model_version_id = mes[me_names==me, age_split_meid])

mtwith_data_agesplit[!is.na(crosswalk_parent_seq), seq := NA]
mtwith_data_agesplit <- mtwith_data_agesplit[!(grepl("age-split using Dismod", note_modeler) & (age_start < 20))]


data <- rbindlist(list(prevalence_data_agesplit,mtwith_data_agesplit))

data[standard_error > 1, standard_error := 1]
data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid", "group", "group_review", "specificity") := NA]
data[, unit_value_as_published := 1]
data[, c("cv_claims", "cv_hospital", "cv_inpatient", "cv_marketscan_2000") := NULL]
data[!is.na(crosswalk_parent_seq), seq := NA]

write.xlsx(data, paste0(write_file, me, "_post_age_split_", date, ".xlsx"), sheetName = "extraction")
save_crosswalk_version(bundle_version_id = mes[me_names==me, bundle_version_id], 
                       data_filepath = paste0(write_file, me, "_post_age_split_", date, ".xlsx"), 
                       description = paste0("Age-split ", date, " with appropriate sex-splitting and all MCT data"))


