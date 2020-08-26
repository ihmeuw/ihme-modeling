
##
## Purpose: NRVD crosswalks in MR-BRT. 
##          Save age-specific data as crosswalk versions for age splitting models. 
##          Includes outliering hospital data that isn't representative. 
##


date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"

mes <- data.table(me_name = c("aort", "mitral"), 
                  bundle_id = c(2987, 2993),
                  modelable_entity = c(16601, 16602),
                  modelable_entity_version = c(410420, 410426),
                  s2bundle_version = c(15551, 14927))

decomp_step <- "step2"
gbd_round_id <- 6

plot <- F

logit_transform <- T

folder_root <- "FILEPATH"


###### Functions
#################################################################################

vid <- mes[me_name==me, s2bundle_version]

## Central functions to upload/pull down data
source(paste0(central, 'save_crosswalk_version.R'))
source(paste0(central, 'get_bundle_version.R'))
source(paste0(central, 'save_bundle_version.R'))
source(paste0(central, 'get_model_results.R'))
source(paste0(central, 'get_age_metadata.R'))
source(paste0(central, 'get_location_metadata.R'))
source(paste0(central, 'get_bundle_data.R'))

## MR-BRT wrapper function
brt_code_root <- "FILEPATH"
source(paste0(brt_code_root, "mr_brt_functions.R"))
source('master_mrbrt_crosswalk_func.R')


###### Pull down data
#################################################################################

save_bundle <- F
if (save_bundle) {
  hold <- save_bundle_version(bundle_id = mes[me_name==me, bundle_id], decomp_step = decomp_step, include_clinical = T)
  data <- as.data.table(get_bundle_version(hold$bundle_version_id))
} else {
  data <- as.data.table(get_bundle_version(mes[me_name==me, s2bundle_version]))
}

data[, note_modeler := ""]

## Mark columns
data[, `:=` (cv_claims = ifelse(clinical_data_type=="claims", 1, 0), 
             cv_inpatient = ifelse(clinical_data_type=="inpatient", 1, 0),
             cv_marketscan_2000 = ifelse(clinical_data_type=="claims" & grepl("2000", field_citation_value), 1, 0))]
data[cv_claims==1 & cv_marketscan_2000==1, cv_claims := 0]

locations <- as.data.table(get_location_metadata(location_set_id=35))
data <- merge(data, locations[, .(location_id, super_region_name, region_name, parent_id)], by="location_id")

if (plot) data[, def := ifelse(cv_claims==1, "Claims", ifelse(cv_inpatient==1, "Inpatient", ifelse(cv_marketscan_2000==1, "Marketscan 2000", "Literature")))]

## Aggregate Marketscan to national data points for matching
msn <- copy(data)[cv_claims==1 | cv_marketscan_2000==1,]
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

msn[, note_modeler := paste0(note_modeler, " | DATA POINT IS AGGREGATED ONLY EXISTS FOR MATCHING - REMOVE BEFORE UPLOAD")]

## Pull aggregated Marketscan data back onto the dataset
data <- rbind(data, msn)

data_all_types <- copy(data)


###### Outlier hospital data 
#################################################################################

data[mean>0, `:=` (med_val=median(mean), mad=median(abs(mean-median(mean)))), by="age_start"]

data[, extreme:=ifelse(mean<med_val-2*mad | mean>med_val+2*mad, 1, 0)]
data[, c("med_val", "mad"):=NULL]

if (me == "mitral") {
  
  data[nid==133665 & year_start==1998 & location_id==180, is_outlier:=1]
  data[nid==3822 & year_start==2008 & location_id==52, is_outlier:=1]
  
  data[nid==292574 & year_start==2013 & location_id==16, is_outlier:=1]
  
  data[nid %in% c(292437, 292575) & location_id==164, is_outlier:=1]
  
  data[region_name=="South Asia" & cv_inpatient == 1, is_outlier:=1]
  data[nid==281819 & year_start==2013 & location_id==4862, is_outlier:=1]
  
  data[nid==104246 & year_start==1993 & location_id==4774, is_outlier:=1]
  data[nid==104246 & year_start==1993 & location_id==4772, is_outlier:=1]
  data[nid==104246 & year_start==1993 & location_id==4773, is_outlier:=1]
  data[nid==104246 & year_start==2013 & location_id==4773 & sex=="Male", is_outlier:=1]
  data[nid==104246 & year_start==1993 & location_id==4775, is_outlier:=1]
  data[location_id==4776 & cv_inpatient == 1, is_outlier:=1]
  
  data[region_name=="Tropical Latin America" & cv_inpatient == 1, is_outlier:=1]

  data[region_name=="Central Latin America" & cv_inpatient == 1, is_outlier:=1]

  data[region_name=="Southern Latin America" & cv_inpatient == 1, is_outlier:=1]
  data[cv_inpatient == 1 & location_id==491 & year_start==2013, is_outlier:=0] ##sy: Anhui
  data[cv_inpatient == 1 & location_id==492 & year_start==2013, is_outlier:=0] ##sy: Beijing
  data[cv_inpatient == 1 & location_id==494 & year_start==2013, is_outlier:=0] ##sy: Fuijan
  data[cv_inpatient == 1 & location_id==496 & year_start==2013, is_outlier:=0] ##sy: Guangdong
  data[cv_inpatient == 1 & location_id==497 & year_start==2013, is_outlier:=0] ##sy: Guanxi
  data[cv_inpatient == 1 & location_id==506 & year_start==2013, is_outlier:=0] ##sy: Jiangsu
  data[cv_inpatient == 1 & location_id==507 & year_start==2013, is_outlier:=0] ##sy: Jiangxi
  data[cv_inpatient == 1 & location_id==509 & year_start==2013, is_outlier:=0] ##sy: Lianoning
  data[cv_inpatient == 1 & location_id==510 & year_start==2013, is_outlier:=0] ##sy: Ningxia
  data[cv_inpatient == 1 & location_id==513 & year_start==2013, is_outlier:=0] ##sy: Shandong
  data[cv_inpatient == 1 & location_id==514 & year_start==2013, is_outlier:=0] ##sy: Shanghai
  data[cv_inpatient == 1 & location_id==521 & year_start==2013, is_outlier:=0] ##sy: Zhejiang
  
  data[region_name=="Andean Latin America" & cv_inpatient == 1, is_outlier:=1]
  data[nid==3822 & year_start==2008 & location_id==51, is_outlier:=1]
  data[nid==331084 & location_id==142, is_outlier:=1]
  
  data[nid==3822 & location_id==87 & year_start<2003, is_outlier:=1]
  
} else if (me == "aort") {
  
  data[nid==234738 & year_start==1993 & location_id==122 & age_start>=85, is_outlier:=1]
  data[nid==234738 & year_start==1998 & location_id==122 & age_start>=85 & sex=="Female", is_outlier:=1]
  data[nid==234738 & location_id==122, is_outlier:=1]
  
  data[region_name=="South Asia" & cv_inpatient == 1, is_outlier:=1]
  data[nid %in% c(292437, 292575) & location_id==164, is_outlier:=1]
  data[nid==281819 & year_start==2013 & location_id==4862, is_outlier:=1]
  data[nid==337129 & year_start==2013 & location_id==4856, is_outlier:=1]
  
  data[nid==133665 & year_start==1998 & location_id==180, is_outlier:=1]
  data[nid==284420 & year_start==2013 & location_id==499 & age_start>=90 & sex=="Female", is_outlier:=1]
  data[nid==284420 & year_start==2013 & location_id==510 & age_start>=90 & sex=="Female", is_outlier:=1]
  data[nid==284420 & year_start==2013 & location_id==505 & age_start>=90 & sex=="Female", is_outlier:=1]
  data[nid==284420 & year_start==2013 & location_id==518 & age_start==50 & sex=="Female", is_outlier:=1]
  data[nid==292574 & year_start==2013 & location_id==16, is_outlier:=1]
  data[region_name=="Tropical Latin America" & cv_inpatient == 1, is_outlier:=1]

  data[region_name=="Central Asia" & cv_inpatient == 1, is_outlier:=1]
  
  data[region_name=="Central Latin America" & cv_inpatient == 1, is_outlier:=1]
  data[location_id==4651 & year_start==2008 & cv_inpatient == 1, is_outlier:=0] ## Distrito Federal
  data[location_id==4666 & year_start==2008 & cv_inpatient == 1, is_outlier:=0] ## San Luis Potosi

  data[region_name=="North Africa and Middle East" & cv_inpatient == 1, is_outlier:=1]
  data[nid==317423 & year_start==2013 & location_id==144, is_outlier:=1]
  data[nid==331084 & location_id==142, is_outlier:=1]
  
  data[region_name=="Southern Latin America" & cv_inpatient == 1, is_outlier:=1]
  data[region_name=="East Asia" & cv_inpatient == 1, is_outlier:=1]
  data[cv_inpatient == 1 & location_id==492 & year_start==2013, is_outlier:=0] ## Beijing
  data[cv_inpatient == 1 & location_id==496 & year_start==2013, is_outlier:=0] ## Guangdong
  data[cv_inpatient == 1 & location_id==506 & year_start==2013, is_outlier:=0] ## Jiangsu
  data[cv_inpatient == 1 & location_id==507 & year_start==2013, is_outlier:=0] ## Jiangxi
  data[cv_inpatient == 1 & location_id==513 & year_start==2013, is_outlier:=0] ## Shandong
  data[cv_inpatient == 1 & location_id==516 & year_start==2013, is_outlier:=0] ## Sichuan
  
  data[nid==284439 & year_start==1998 & location_id==44850, is_outlier:=1]
  data[nid==234750 & year_start==2008 & parent_id==90, is_outlier:=1]

  
  data[nid==3822 & year_start==2003 & age_start>=80 & location_id==88, is_outlier:=1]
  
  data[nid==3822 & year_start==2008 & age_start>=80 & location_id==83 & sex=="Male", is_outlier:=1]
  
  data[nid==3822 & year_start==2008 & age_start>=80 & location_id==83 & sex=="Male", is_outlier:=1]
  
  data[nid==3822 & year_start==2008 & age_start==95 & location_id==52 & sex=="Female", is_outlier:=1]
  
}


###### Run crosswalk
#################################################################################

prev <- copy(data[measure=="prevalence",])
xw_measure <- "prevalence"

crosswalk_holder <- master_mrbrt_crosswalk(data = prev,
                                           model_abbr = "documentation_cvd_nrvd_aort",
                                           save_crosswalk_result_file = "FILEPATH",
                                           alternate_defs = c("cv_inpatient", "cv_claims", "cv_marketscan_2000"),
                                           folder_root = folder_root,
                                           xw_measure = "prevalence",
                                           
                                           sex_split_only = F,
                                           
                                           age_overlap = 5,
                                           age_range = 25,
                                           year_overlap = 5,
                                           year_range = 10,
                                           
                                           addl_x_covs = c("male", "age_scaled"),
                                           
                                           id_vars = "id_var",
                                           decomp_step = "step2", 
                                           logit_transform = T)

prev <- crosswalk_holder$data
prev[region_name %like% "United States" & cv_inpatient == 1, is_outlier:=1]
prev <- prev[!(note_modeler %like% "REMOVE BEFORE UPLOAD")]


if (me == "mitral") {
  data_m <- copy(data[measure == "cfr",])
  data_f <- copy(data[measure=="cfr",])
  data_m[, `:=` (sex="Male", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
  data_f[, `:=` (sex="Female", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
  cfr <- rbind(data_f, data_m)
  
  data_m <- copy(data[measure == "mtwith",])
  data_f <- copy(data[measure=="mtwith",])
  data_m[, `:=` (sex="Male", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
  data_f[, `:=` (sex="Female", note_modeler = paste0(note_modeler, " | duplicated from both-sex data"))]
  mtwith <- rbind(data_f, data_m, fill=T)
  
  all_data <- do.call("rbind", list(prev, mtwith, cfr, fill=T))
} else {
  
  mtwith <- copy(data[measure == "mtwith",])
  xw_measure <- "mtwith"

    crosswalk_holder <- master_mrbrt_crosswalk(
      
    save_crosswalk_result_file = "FILEPATH",
    
    model_abbr = "documentation_cvd_valvu_aort",
    data = mtwith,
    age_overlap = 25,
    year_overlap = 15,
    age_range = 5,
    year_range = 5,
    xw_measure = xw_measure,
    addl_x_covs = addl_x_covs,
    folder_root = folder_root,
    sex_split_only = T,
    subnat_to_nat = F,
    
    sex_covs = NULL,
    
    gbd_round_id = gbd_round_id,
    decomp_step = decomp_step,
    logit_transform = logit_transform,
    
    upload_crosswalk_version = F
  )
  

  mtwith <- crosswalk_holder$data
  all_data <- do.call("rbind", list(prev, mtwith, data[!(measure %in% c("mtwith", "prevalence"))], fill=T))
}

num_cols <- c("mean", "upper", "lower", "cases", "sample_size")
lapply(X = num_cols, FUN = check_class, df=all_data, class="numeric")

message("Writing to xlsx")
write.xlsx(all_data, file = "FILEPATH")

##sy: drop all data less than age 40 and all means that are 0
all_data[age_start<40 & cv_inpatient == 1, is_outlier:=1]
all_data[mean==0 & cv_inpatient == 1, is_outlier:=1]

age_data <- copy(all_data[age_end - age_start < 25,])
age_data[location_id %in% c(4622,4624,95), `:=` (location_id=4749, location_name="England", ihme_loc_id="GBR_4749", step2_location_year="UTLA data points reextracted as 'England' because of the location heirarchy.")]

age_data$underlying_nid <- NA
age_data$crosswalk_parent_seq <- age_data$seq
age_data$seq <- NA
#
age_data$group_review <- NA
age_data$specificity <- NA
age_data$group <- NA
#
age_data$upper <- NA
age_data$lower <- NA
age_data$uncertainty_type_value <- NA

age_data[standard_error>1, standard_error := 1]

#
age_data$sampling_type <- NA
age_data[, grep("cv_*", names(age_data)) := NULL]

write.xlsx(age_data, "FILEPATH", sheetName="extraction")
#
description <- "MR-BRT age pattern, 9/27/19"
save_crosswalk_version_result <- save_crosswalk_version(
  bundle_version_id=vid,
  data_filepath="FILEPATH",
  description=description)
#
print(sprintf('Request status: %s', save_crosswalk_version_result$request_status))
print(sprintf('Request ID: %s', save_crosswalk_version_result$request_id))
print(sprintf('Bundle version ID: %s', save_crosswalk_version_result$bundle_version_id))
#
message("crosswalk version uploaded.")
