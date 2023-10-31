
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: NRVD processing for GBD 2020 
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"

mes <- data.table(
  
  me_names = c("aort", "mitral"),
  me_name_long = c("CAVD", "DMVD"),
  bundle_id = c("VALUES"),
  modelable_entity = c("VALUES"),
  bundle_version_id = c("VALUES"),
  acause = c("cvd_valvu_aort", "cvd_valvu_mitral"),
  age_split_meid = c("VALUES")
  
)

gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

plot <- T
save_age_version <- T

write_file <- paste0("FILEPATH")

###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))
source("/FILEPATH/master_mrbrt_crosswalk_func.R")


###### Pull down data
#################################################################################

save_bundle <- F
if (save_bundle) {
  hold <- save_bundle_version(bundle_id = mes[me_names==me, bundle_id], decomp_step = decomp_step, include_clinical = c("claims", "inpatient"), gbd_round_id = gbd_round_id)
  data <- as.data.table(get_bundle_version(hold$bundle_version_id, fetch = "all"))
} else {
  data <- as.data.table(get_bundle_version(mes[me_names==me, bundle_version_id], fetch = "all"))
}

data[, note_modeler := ""]

## Mark columns
data[, `:=` (cv_claims = ifelse(clinical_data_type=="claims"|field_citation_value%like%"Truven", 1, 0), 
             cv_inpatient = ifelse(clinical_data_type=="inpatient", 1, 0),
             cv_marketscan_2000 = ifelse(clinical_data_type=="claims" & grepl("2000", field_citation_value), 1, 0))]
data[cv_claims==1 & cv_marketscan_2000==1, cv_claims := 0]
data <- data[cv_marketscan_2000 == 0]
data[is.na(cv_hospital), cv_hospital := 0]
#data[, cv_inpatient_lit := ifelse(cv_hospital == 1, 1, 0)]
data[cv_hospital == 1, cv_inpatient := 1]
     
## Get rid of CI data below 40
data <- data[!((cv_inpatient ==1 | cv_claims == 1) & age_start < 40)]

locations <- as.data.table(get_location_metadata(location_set_id=35))
data <- merge(data, locations[, .(location_id, super_region_name, region_name, parent_id)], by="location_id")
data$note_modeler <- ""

## Aggregate Marketscan to national data points for matching
#msn <- copy(data)[cv_claims==1 | cv_marketscan_2000==1,]
msn <- copy(data)[grepl("Truven", field_citation_value)]
msn[, agg_cases := sum(cases), by=c("age_start", "age_end", "year_start", "year_end", "sex")]
msn[, agg_ss := sum(sample_size), by=c("age_start", "age_end", "year_start", "year_end", "sex")]
msn[, agg_mean := agg_cases/agg_ss]
z <- qnorm(0.975)
msn[measure == "prevalence", agg_se := sqrt(agg_mean*(1-agg_mean)/agg_ss + z^2/(4*agg_ss^2))]
msn[measure == "incidence" & cases < 5, agg_se := ((5-agg_mean*agg_ss)/agg_ss+agg_mean*agg_ss*sqrt(5/agg_ss^2))/5]
msn[measure == "incidence" & cases >= 5, agg_se := sqrt(agg_mean/agg_ss)]
msn[, agg_location_id := 102]
msn[, agg_location_name := "United States of America"]
msn[, c("location_name", "location_id", "mean", "upper", "lower", "standard_error", "cases", "sample_size", "effective_sample_size", "seq", "origin_seq") := NA]
msn <- unique(msn)

msn[, `:=` (cases=agg_cases, standard_error=agg_se, sample_size=agg_ss, mean=agg_mean, location_id=agg_location_id, location_name=agg_location_name)]
n <- names(msn)[grepl("agg", names(msn))]
msn[, c(n) := NULL]

msn[, note_modeler := paste0(note_modeler, " | DATA POINT IS AGGREGATED ONLY EXISTS FOR MATCHING - REMOVE BEFORE UPLOAD - USERNAME")]

## Pull aggregated Marketscan data back onto the dataset
data <- rbind(data, msn)

data_all_types <- copy(data)
data <- copy(data_all_types)


###### Outlier hospital data
#################################################################################

data[mean>0, `:=` (med_val=median(mean), mad=median(abs(mean-median(mean)))), by="age_start"]

data[, extreme:=ifelse(mean<med_val-2*mad | mean>med_val+2*mad, 1, 0)]
data[, c("med_val", "mad"):=NULL]

## Outlier mean-0 CI data
data[mean == 0 & clinical_data_type == "inpatient", is_outlier := 1]
#data[extreme == 1 & clinical_data_type == "inpatient", is_outlier := 1]

if (me == "mitral") {
  
  ## Kenya inpatient data from the MOH, too low
  data[clinical_data_type == "inpatient" & location_id==180, is_outlier:=1]
  
  ## Romania inpatient data, too high
  data[clinical_data_type == "inpatient" & location_id==52, is_outlier:=1]
  
  ## Philippines inpatient data, too low
  phil_locs <- locations[parent_id == 16, location_id]
  data[clinical_data_type == "inpatient" & location_id %in% c(16, phil_locs), is_outlier:=1]
  
  ## Nepal inpatient data, all zeros
  data[clinical_data_type == "inpatient" & location_id==164, is_outlier:=1]
  
  ## All South Asia (India, Nepal), all zeros
  data[region_name=="South Asia" & clinical_data_type == "inpatient", is_outlier:=1]
  
  ## Brazil, too low
  bra_subnats <- locations[parent_id == 135, location_id]
  data[location_id %in% c(135, bra_subnats) & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Mexico, too low and many zeros
  mex_subnats <- locations[parent_id == 130, location_id]
  data[location_id %in% c(130, mex_subnats) & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Chile, too low
  data[location_id == 98 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Belgium, all zeros
  data[location_id == 76 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## China subnationals, start with outliering everything and add some in, too low
  china_locs <- locations[parent_id == 6, location_id]
  data[clinical_data_type == "inpatient" & location_id %in% c(6, china_locs), is_outlier := 1]
  data[clinical_data_type == "inpatient" & location_id == 491, is_outlier := 0] ## Anhui
  data[clinical_data_type == "inpatient" & location_id == 492, is_outlier := 0] ## Beijing
  data[clinical_data_type == "inpatient" & location_id == 494, is_outlier := 0] ## Fuijan
  data[clinical_data_type == "inpatient" & location_id == 496, is_outlier := 0] ## Guangdong
  data[clinical_data_type == "inpatient" & location_id == 497, is_outlier := 0] ## Guangxi
  data[clinical_data_type == "inpatient" & location_id == 499, is_outlier := 0] ## Hainan -- new for GBD 2020, prev has increased
  data[clinical_data_type == "inpatient" & location_id == 506, is_outlier := 0] ## Jiangsu
  data[clinical_data_type == "inpatient" & location_id == 507, is_outlier := 0] ## Jiangxi
  data[clinical_data_type == "inpatient" & location_id == 509, is_outlier := 0] ## Lianoning
  data[clinical_data_type == "inpatient" & location_id == 510, is_outlier := 0] ## Ningxia
  data[clinical_data_type == "inpatient" & location_id == 513, is_outlier := 0] ## Shangdong
  data[clinical_data_type == "inpatient" & location_id == 514, is_outlier := 0] ## Shanghai
  data[clinical_data_type == "inpatient" & location_id == 521, is_outlier := 0] ## Zhejiang
  
  ## Brazil, too low
  bra_subnats <- locations[parent_id == 135, location_id]
  data[clinical_data_type == "inpatient" & location_id %in% c(135, bra_subnats), is_outlier := 1]

  ## Ecuador, too low
  data[clinical_data_type == "inpatient" & location_id == 122, is_outlier := 1]
  
  ## Poland, too high
  data[clinical_data_type == "inpatient" & location_id == 51, is_outlier := 1]
  
  ## Iran, many zeros
  data[clinical_data_type == "inpatient" & location_id == 142, is_outlier := 1]
  
  ## Most recent year of Georgia data
  data[nid == 407536, is_outlier := 1]
  
  ## Botswana, many zeros
  data[clinical_data_type == "inpatient" & location_id == 193, is_outlier := 1]
  
  ## Cyprus, many zeros
  data[clinical_data_type == "inpatient" & location_id == 77, is_outlier := 1]
  
  
} else if (me == "aort") {
  
  ## Leaving in Georgia, Ecuador, Turkey
  ## Get rid of Belgium, Botswana
  
  ## India and Nepal, many zeros/too low
  data[region_name == "South Asia" & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Kenya
  data[clinical_data_type == "inpatient" & location_id == 180, is_outlier := 1]
  
  ## China subnationals, start with outliering everything and add some back in
  data[parent_id == 6 & clinical_data_type == "inpatient", is_outlier:=1]
  data[clinical_data_type == "inpatient" & location_id==492, is_outlier:=0] ## Beijing
  data[clinical_data_type == "inpatient" & location_id==496, is_outlier:=0] ## Guangdong
  data[clinical_data_type == "inpatient" & location_id==499, is_outlier:=0] ## Hainan - new for GBD 2020, prevalence has increased
  data[clinical_data_type == "inpatient" & location_id==506, is_outlier:=0] ## Jiangsu
  data[clinical_data_type == "inpatient" & location_id==507, is_outlier:=0] ## Jiangxi
  data[clinical_data_type == "inpatient" & location_id == 510 & age_start < 99, is_outlier := 0] ## Ningxia - new for GBD 2020 except for oldest age group
  data[clinical_data_type == "inpatient" & location_id==513, is_outlier:=0] ## Shandong
  data[clinical_data_type == "inpatient" & location_id==516, is_outlier:=0] ## Sichuan
  
  ## Brazil
  data[parent_id == 135 & clinical_data_type == "inpatient", is_outlier := 1]

  ## Mexico subnationals, start with outliering everything and add some back in
  data[parent_id == 130 & clinical_data_type == "inpatient", is_outlier := 1]
  data[location_id == 4651 & clinical_data_type == "inpatient", is_outlier := 0] ## Distrito Federal
  data[location_id == 4666 & clinical_data_type == "inpatient", is_outlier := 0] ## San Luis Potosi
  data[location_id == 4672 & clinical_data_type == "inpatient", is_outlier := 0] ## Veracruz de Ignacio de la Llave - new for GBD 2020, prevalence has increased and few 0s
  
  ## Jordan
  data[location_id == 144 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Iran
  data[location_id == 142 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Botswana, erratic pattern where >60 is 0 and <60 is too high
  data[location_id == 193 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Chile  
  data[location_id == 98 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Very high single data point in Maori NZ in 1998
  data[location_id == 44850 & clinical_data_type == "inpatient" & year_start == 1998, is_outlier := 1]
  
  ## Norway
  data[parent_id == 90 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## High data point in Malta
  data[location_id == 88 & clinical_data_type == "inpatient" & age_start >= 80, is_outlier := 1]
  
  ## Belgium
  data[location_id == 76 & clinical_data_type == "inpatient", is_outlier := 1]
  
  ## Philippines
  data[location_id == 16 & clinical_data_type == "inpatient", is_outlier := 1]
  
}

## Final cleanings
data$age_sd <- NULL
data[is.na(is_outlier), is_outlier := 0]
data <- data[is_outlier == 0]

###### Crosswalk prevalence data
#################################################################################

prev <- copy(data[measure=="prevalence",])
prev[mean>1, mean := 1]
prev <- prev[standard_error > 0]
prev[standard_error > 1, standard_error := 1]
xw_measure <- "prevalence"

if (me == "mitral") {
  crosswalk_holder <- master_mrbrt_crosswalk(data = prev,
                                             model_abbr = "cvd_valvu_mitral",
                                             alternate_defs = c("cv_inpatient", "cv_claims"),
                                             storage_folder = "FILEPATH",
                                             xw_measure = "prevalence",
                                             
                                             sex_split_only = F,
                                             
                                             age_overlap =10,
                                             age_range = 25,
                                             year_overlap = 10,
                                             year_range = 25,
                                             
                                             addl_x_covs = list("age_scaled"),
#                                             spline_covs = "age_scaled",
                                             
                                             knots = c(-1.673, -0.669, 0.33, 2.84),
                                             degree = 3,
                                             r_linear = T,
                                             l_linear = T,
                                             id_vars = "id_var",
                                             decomp_step = "VALUE", 
                                             gbd_round_id = "VALUE", 
                                             logit_transform = T,
                                             remove_x_intercept = F,
                                             trim_pct = 0.1)
} else {
  crosswalk_holder <- master_mrbrt_crosswalk(data = prev,
                                             model_abbr = "cvd_valvu_aort",
                                             alternate_defs = c("cv_inpatient", "cv_claims"),
                                             storage_folder = "FILEPATH",
                                             xw_measure = "prevalence",
                                             
                                             sex_split_only = F,
                                             
                                             age_overlap =10,
                                             age_range = 25,
                                             year_overlap = 10,
                                             year_range = 25,
                                             
                                             addl_x_covs = list("age_scaled"),
                                             #spline_covs = "age_scaled",
                                             
                                             knots = c(-1.666, -0.701, 0.586, 2.679),
                                             degree = 3,
                                             r_linear = T,
                                             l_linear = T,
                                             id_vars = "id_var",
                                             decomp_step = "VALUE", 
                                             gbd_round_id = "VALUE", 
                                             logit_transform = T,
                                             remove_x_intercept = F,
                                             trim_pct = 0.1)
}

prevalence_data <- crosswalk_holder$data
prevalence_data <- prevalence_data[!(note_modeler %like% "REMOVE BEFORE UPLOAD")]

write.csv(prevalence_data, paste0(write_file, "pre_age_split_", me, "_", date, "_", xw_measure, ".csv"))


###### Sex-split other measures
#################################################################################

data_m <- copy(data[measure == "cfr",])
data_f <- copy(data[measure=="cfr",])
data_m[, `:=` (sex="Male", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
data_f[, `:=` (sex="Female", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
cfr <- rbind(data_f, data_m)

data_m <- copy(data[measure == "mtwith" & sex == "Both",])
data_f <- copy(data[measure=="mtwith" & sex == "Both",])
data_m[, `:=` (sex="Male", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
data_f[, `:=` (sex="Female", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
mtwith <- rbind(data_f, data_m, fill=T)
mtwith <- rbind(copy(data)[measure=="mtwith"&sex!="Both"], mtwith)

all_data <- do.call("rbind", list(prevalence_data, mtwith, cfr, fill=T))
write.csv(all_data, paste0(write_file, "all_age_", me, "_", date, ".csv"))


###### Save age-splitting crosswalk versions
#################################################################################

age_specific_data <- copy(all_data)[age_end - age_start <= 25]
age_specific_data$crosswalk_parent_seq <- age_specific_data$seq
age_specific_data$seq <- NA
age_specific_data[standard_error > 1, standard_error := 1]
age_specific_data[, c("upper", "lower", "uncertainty_type_value", "underlying_nid") := NA]
age_specific_data[, c("cv_hospital", "cv_marketscan_2000", "cv_inpatient", "cv_claims") := NULL]
age_specific_data[, unit_value_as_published := 1]

save_age_version <- F
if (save_age_version) {
  for (meas in unique(all_data$measure)) {
    
    df <- copy(age_specific_data)[measure == meas]
    write.xlsx(df, paste0(write_file, "for_age_split_model_", me, "_", date, "_", meas, ".xlsx"), sheetName="extraction")
    
    save_crosswalk_version(bundle_version_id = mes[me_names==me, bundle_version_id], 
                           data_filepath = paste0(write_file, "for_age_split_model_", me, "_", date, "_", meas, ".xlsx"), 
                           description = paste0("Age-specific data, post-crosswalk, for age-split model ", me, " ", meas, " ", date))
    
    
  }
}





