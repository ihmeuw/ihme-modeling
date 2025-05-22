###############################################################################################################################################
# Purpose: data processing for ectopic input data
# adjust denominator, matchfinding, run mr-brt, apply adjustments, and age-split
###############################################################################################################################################

remove(list = ls())
library(writexl)
library(dplyr)
library(scales)
library(ggplot2)
library(gridExtra)
library(readr)
library(openxlsx)
library(readxl)

base_dir <- "FILEPATH"
functions <- c("get_bundle_data", "upload_bundle_data", "get_bundle_version",
               "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version","get_age_metadata",
               "get_location_metadata","get_draws","get_ids","get_population","get_covariate_estimates","get_model_results")

lapply(paste0(base_dir, functions, ".R"), source)


Sys.setenv("RETICULATE_PYTHON" = "FILEPATH/python") 
library(reticulate)
reticulate::use_python("FILEPATH/python")
mr <- import("mrtool")
cw <- import("crosswalk")


ectopic<-get_bundle_version(BVID)

###################################################################################################################################
############adjust the denominators in Scilit data
ectopic_scilit<-subset(ectopic, is.na(clinical_data_type))
ectopic_scilit$year_id<-round((ectopic_scilit$year_start+ectopic_scilit$year_end)/2)
ectopic_scilit$age_group<-paste(ectopic_scilit$age_start,ectopic_scilit$age_end,sep="-")

#calculate the following ratios to adjust denominators in Scilit sources
# of live births = # of women of reproductive age in extraction * asfr
# of live births = # of pregnancies in extraction * (# of live births/# of pregnancies in GBD dataset for the same location)
# of live births = # of deliveries * (# of live births/# of births in GBD dataset for the same location)
# of live births = # of births * (# of live births/# of births in GBD dataset for the same location)
# to get age_specific counts of births:
### of age specific birth = population *[(ASFR * 46/52) + (stillbirth_livebirth_ratio_20wks *ASFR * 26/52)] 
#assumptions: most of the studies treat 1 twin pregnancy= 1 delivery = 2 live births; the stillbirth-live birth ratio is the same across all ages

scilit_location_id<-ectopic_scilit$location_id
scilit_year_id<-ectopic_scilit$year_id

#if the denominator is women of reproductive age; 
asfr <- get_covariate_estimates(covariate_id=OBJECT, age_group_id = 'all', location_id=scilit_location_id, year_id=scilit_year_id, release_id=OBJECT, sex_id=OBJECT)
setnames(asfr, "mean_value", "asfr")
asfr<-asfr[, c("location_id","location_name", "year_id", "age_group_id", "asfr")]
WRA <- get_population(age_group_id='all', location_id=scilit_location_id, year_id=scilit_year_id, sex_id=OBJECT, with_ui=TRUE, release_id=OBJECT)
WRA<-WRA[, c("location_id", "year_id", "age_group_id", "population")]
asfr<-left_join(asfr,WRA,by=c("location_id", "year_id", "age_group_id"))
asfr<-subset(asfr,age_group_id >= 7 & age_group_id <= 15)
livebirth<-get_covariate_estimates(covariate_id=OBJECT, age_group_id = 'all', location_id=scilit_location_id, year_id=scilit_year_id, release_id=OBJECT)

calculate_weighted_asfr <- function(data, age_groups) {
  filtered_data <- data %>% filter(age_group_id %in% age_groups)
  weighted_asfr <- sum(filtered_data$asfr * filtered_data$population) / sum(filtered_data$population)
  return(weighted_asfr)
}

asfr_10_54 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(7, 8, 9,10,11,12,13,14,15)), .groups = 'drop')
asfr_10_54$age_group<-"10-54"
asfr_10_19 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(7, 8)), .groups = 'drop')
asfr_10_19$age_group<-"10-19"
asfr_15_40 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12)), .groups = 'drop')
asfr_15_40$age_group<-"15-40"
asfr_15_44 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
asfr_15_44$age_group<-"15-44"
asfr_15_49 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12,13,14)), .groups = 'drop')
asfr_15_49$age_group<-"15-49"
asfr_16_40 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12)), .groups = 'drop')
asfr_16_40$age_group<-"16-40"
asfr_16_45 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
asfr_16_45$age_group<-"16-45"
asfr_17_45 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
asfr_17_45$age_group<-"17-45"
asfr_17_46 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
asfr_17_46$age_group<-"17-46"
asfr_18_54 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(8,9,10,11,12,13,14,15)), .groups = 'drop')
asfr_18_54$age_group<-"18-54"
asfr_20_30 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(9,10)), .groups = 'drop')
asfr_20_30$age_group<-"20-30"
asfr_31_35 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(11)), .groups = 'drop')
asfr_31_35$age_group<-"31-35"
asfr_36_54 <- asfr %>%group_by(location_id, year_id) %>%summarize(asfr = calculate_weighted_asfr(cur_data(), c(12,13,14,15)), .groups = 'drop')
asfr_36_54$age_group<-"36-54"
asfr_aggregated <- rbind(asfr_10_54,asfr_10_19,asfr_15_40,asfr_15_44,asfr_15_49,asfr_16_40,asfr_16_45,asfr_17_45,asfr_17_46,asfr_18_54,asfr_20_30,asfr_31_35,asfr_36_54)
ectopic_scilit<-left_join(ectopic_scilit,asfr_aggregated, by=c("location_id","year_id","age_group"))

#if the denominator is pregnancy; 
pregnancy <- get_population(age_group_id='all', location_id=scilit_location_id, year_id=scilit_year_id, sex_id=OBJECT, with_ui=TRUE, release_id=OBJECT, population_group_id=OBJECT) #no data before 1990
setnames(pregnancy, "population", "population_preg")
pregnancy<-pregnancy[, c("location_id", "year_id", "age_group_id", "population_preg")]
live_birth_pregnancy<-left_join(asfr,pregnancy,by=c("location_id", "year_id", "age_group_id"))
live_birth_pregnancy<-live_birth_pregnancy%>%mutate(live_birth_pregnancy_ratio=population*asfr*46/52/population_preg)
live_birth_pregnancy<-live_birth_pregnancy[,c("location_id","year_id","age_group_id","live_birth_pregnancy_ratio","population_preg")]

calculate_weighted_live_birth_pregnancy <- function(data, age_groups) {
  filtered_data <- data %>% filter(age_group_id %in% age_groups)
  weighted_live_birth_pregnancy <- sum(filtered_data$live_birth_pregnancy_ratio * filtered_data$population_preg) / sum(filtered_data$population_preg)
  return(weighted_live_birth_pregnancy)
}

lbp_10_54 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(7, 8, 9,10,11,12,13,14,15)), .groups = 'drop')
lbp_10_54$age_group<-"10-54"
lbp_10_19 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(7, 8)), .groups = 'drop')
lbp_10_19$age_group<-"10-19"
lbp_15_40 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12)), .groups = 'drop')
lbp_15_40$age_group<-"15-40"
lbp_15_44 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbp_15_44$age_group<-"15-44"
lbp_15_49 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12,13,14)), .groups = 'drop')
lbp_15_49$age_group<-"15-49"
lbp_16_40 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12)), .groups = 'drop')
lbp_16_40$age_group<-"16-40"
lbp_16_45 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbp_16_45$age_group<-"16-45"
lbp_17_45 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbp_17_45$age_group<-"17-45"
lbp_17_46 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbp_17_46$age_group<-"17-46"
lbp_18_54 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(8,9,10,11,12,13,14,15)), .groups = 'drop')
lbp_18_54$age_group<-"18-54"
lbp_20_30 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(9,10)), .groups = 'drop')
lbp_20_30$age_group<-"20-30"
lbp_31_35 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(11)), .groups = 'drop')
lbp_31_35$age_group<-"31-35"
lbp_36_54 <- live_birth_pregnancy %>%group_by(location_id, year_id) %>%summarize(live_birth_pregnancy_ratio = calculate_weighted_live_birth_pregnancy(cur_data(), c(12,13,14,15)), .groups = 'drop')
lbp_36_54$age_group<-"36-54"
#age_group:10-19 10-54 15-40 15-44 15-49 16-40 17-45 17-46 18-54 20-30 31-35 36-54 
lbp_aggregated <- rbind(lbp_10_54,lbp_10_19,lbp_15_40,lbp_15_44,lbp_15_49,lbp_16_40,lbp_16_45,lbp_17_45,lbp_17_46,lbp_18_54,lbp_20_30,lbp_31_35,lbp_36_54)
ectopic_scilit<-left_join(ectopic_scilit,lbp_aggregated, by=c("location_id","year_id","age_group"))


#if the denominator is women of deliveries/births 
stillbirth_livebirth_ratio_20wks_1 <- read_csv( "FILEPATH/stillbirths_2024-01-16.csv")
stillbirth_livebirth_ratio_20wks_2 <- read_csv( "FILEPATH/stillbirths_2024-01-16_agg_locs.csv")
stillbirth_livebirth_ratio_20wks<-rbind(stillbirth_livebirth_ratio_20wks_1,stillbirth_livebirth_ratio_20wks_2)
setnames(stillbirth_livebirth_ratio_20wks, "mean_value", "stillbirth_livebirth_ratio")
stillbirth_livebirth_ratio_20wks<-stillbirth_livebirth_ratio_20wks[, c("location_id","year_id", "stillbirth_livebirth_ratio")]
stillbirth_livebirth_ratio_20wks<-subset(stillbirth_livebirth_ratio_20wks,location_id %in% scilit_location_id&year_id %in%scilit_year_id)
birth <- left_join(asfr, stillbirth_livebirth_ratio_20wks, by=c("location_id", "year_id"))
birth<-birth%>%mutate(live_birth_birth_ratio=(asfr*46/52*population)/((asfr*46/52+stillbirth_livebirth_ratio*asfr*26/52)*population),
                      birth=(asfr*46/52+stillbirth_livebirth_ratio*asfr*26/52)*population)
birth<-birth[,c("location_id","year_id","age_group_id","live_birth_birth_ratio","birth")]


calculate_weighted_live_birth_birth <- function(data, age_groups) {
  filtered_data <- data %>% filter(age_group_id %in% age_groups)
  weighted_live_birth_birth <- sum(filtered_data$live_birth_birth_ratio * filtered_data$birth) / sum(filtered_data$birth)
  return(weighted_live_birth_birth)
}

#age_group:10-19 10-54 15-40 15-44 15-49 16-40 17-45 17-46 18-54 20-30 31-35 36-54 
lbb_10_54 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(7, 8, 9,10,11,12,13,14,15)), .groups = 'drop')
lbb_10_54$age_group<-"10-54"
lbb_10_19 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(7, 8)), .groups = 'drop')
lbb_10_19$age_group<-"10-19"
lbb_15_40 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12)), .groups = 'drop')
lbb_15_40$age_group<-"15-40"
lbb_15_44 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbb_15_44$age_group<-"15-44"
lbb_15_49 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12,13,14)), .groups = 'drop')
lbb_15_49$age_group<-"15-49"
lbb_16_40 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12)), .groups = 'drop')
lbb_16_40$age_group<-"16-40"
lbb_16_45 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbb_16_45$age_group<-"16-45"
lbb_17_45 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbb_17_45$age_group<-"17-45"
lbb_17_46 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12,13)), .groups = 'drop')
lbb_17_46$age_group<-"17-46"
lbb_18_54 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(8,9,10,11,12,13,14,15)), .groups = 'drop')
lbb_18_54$age_group<-"18-54"
lbb_20_30 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(9,10)), .groups = 'drop')
lbb_20_30$age_group<-"20-30"
lbb_31_35 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(11)), .groups = 'drop')
lbb_31_35$age_group<-"31-35"
lbb_36_54 <- birth %>%group_by(location_id, year_id) %>%summarize(live_birth_birth_ratio = calculate_weighted_live_birth_birth(cur_data(), c(12,13,14,15)), .groups = 'drop')
lbb_36_54$age_group<-"36-54"
#age ranges:10-19 10-54 15-40 15-44 15-49 16-45 17-45 17-46 18-54 20-30 31-35 36-54 
lbb_aggregated <- rbind(lbb_10_54,lbb_10_19,lbb_15_40,lbb_15_44,lbb_15_49,lbb_16_40,lbb_16_45,lbb_17_45,lbb_17_46,lbb_18_54,lbb_20_30,lbb_31_35,lbb_36_54)
ectopic_scilit<-left_join(ectopic_scilit,lbb_aggregated, by=c("location_id","year_id","age_group"))


#adjust denominators
ectopic_scilit$sample_size_original<-ectopic_scilit$sample_size
ectopic_scilit$mean_original<-ectopic_scilit$mean
ectopic_scilit$upper_original<-ectopic_scilit$upper
ectopic_scilit$lower_original<-ectopic_scilit$lower
ectopic_scilit$standard_error_original<-ectopic_scilit$standard_error
ectopic_scilit<-ectopic_scilit%>%mutate(sample_size=ifelse(denominator_type=="pregnancies",sample_size_original*live_birth_pregnancy_ratio,
                                                           ifelse(denominator_type %in% c("births","deliveries"),sample_size_original*live_birth_birth_ratio,sample_size_original)),
                                        mean=ifelse(denominator_type %in% c("pregnancies","births","deliveries"), cases/sample_size,mean_original),
                                        standard_error=ifelse(denominator_type %in% c("pregnancies","births","deliveries") & cases < 5, ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5,
                                                              ifelse(denominator_type %in% c("pregnancies","births","deliveries") & cases >= 5,  sqrt(mean/sample_size),standard_error_original)),
                                        lower=ifelse(denominator_type %in% c("pregnancies","births","deliveries"), mean-1.96*standard_error,lower_original),
                                        upper=ifelse(denominator_type %in% c("pregnancies","births","deliveries"), mean+1.96*standard_error,upper_original))

#write.xlsx(ectopic_scilit, "FILEPATH/646_ectopic_extractions_bundle_BVID_scilit_denominator_adjusted.xlsx", sheetName = "extraction")
ectopic_scilit<-read.xlsx("FILEPATH/646_ectopic_extractions_bundle_BVID_scilit_denominator_adjusted.xlsx")


###################################################################################################################################
############match finding for CI data
#review the incidence trend in CI data 
ectopic_CI<-subset(ectopic,!is.na(clinical_data_type))
ectopic_CI$age<-(ectopic_CI$age_start+ectopic_CI$age_end)/2


#excluded CI data for matching and modeling
ectopic_CI<-ectopic_CI %>% mutate(ms_hcup=ifelse(nid==OBJECT |nid== OBJECT |nid== OBJECT |nid== OBJECT |nid== OBJECT |nid== OBJECT|nid== OBJECT|nid== OBJECT|nid==OBJECT |nid==OBJECT 
                                                 |nid==OBJECT,"MS", ifelse(nid== OBJECT | nid== OBJECT| nid== OBJECT| nid== OBJECT,"HCUP",ifelse(!is.na(clinical_data_type),"other",NA))))


#create a dataset exclude inpatient data that use the inpatient envelope
#Mark envelope use
envelope_new<-read.xlsx("FILEPATH/clinical_version_id_3_NIDs_uses_env.xlsx")
envelope_new<-envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid<-as.numeric(envelope_distinct_new$nid)
ectopic_CI<-left_join(ectopic_CI,envelope_distinct_new,by="nid")
ectopic_CI_exclude_envelope<-ectopic_CI
ectopic_CI_exclude_envelope <- ectopic_CI_exclude_envelope %>%
  mutate(is_outlier = ifelse(is.na(uses_env), 0, ifelse(uses_env == 1, 1, 0)))

#create a dataset to match reference and alt sources
ectopic_ms_hcup<-subset(ectopic_CI,ms_hcup %in% c("MS","HCUP"))
ectopic_ms_hcup$row_id <- row.names(ectopic_ms_hcup)

#create age and reference variable for match finding
ectopic_ms_hcup$age<-(ectopic_ms_hcup$age_start+ectopic_ms_hcup$age_end)/2
ectopic_ms_hcup$is_reference<-ifelse(ectopic_ms_hcup$clinical_data_type=="inpatient", 1, 0)

# create a dataset to match reference and alt sources
ectopic_ref<-subset(ectopic_ms_hcup, ectopic_ms_hcup$clinical_data_type=="inpatient")
ectopic_ref<-ectopic_ref[, c("nid", "input_type", "source_type", "location_id", "location_name" , "year_start" , "year_end" , "age_start" , "age_end",  "measure",
                             "mean" ,"group_review","standard_error",  "extractor", "is_outlier", "is_reference","age","field_citation_value","upper","lower")]
ectopic_ref<-plyr::rename(ectopic_ref, c("mean"="mean_ref", "standard_error"="standard_error_ref","nid"="nid_ref","field_citation_value"="field_citation_value_ref",
                                         "upper"="upper_ref","lower"="lower_ref"))
ectopic_ref$year_id_ref<-(ectopic_ref$year_start+ectopic_ref$year_end)/2
ectopic_ref$dorm_ref<- "inpatient"

ectopic_alt<-subset(ectopic_ms_hcup, ectopic_ms_hcup$clinical_data_type=="claims")
ectopic_alt<-ectopic_alt[, c("nid", "input_type", "source_type", "location_id", "location_name" , "year_start" , "year_end" , "age_start" , "age_end",  "measure",
                             "mean" ,"group_review","standard_error",  "extractor", "is_outlier", "is_reference","age","field_citation_value", "upper","lower")]
ectopic_alt<-plyr::rename(ectopic_alt, c("mean"="mean_alt", "standard_error"="standard_error_alt","nid"="nid_alt","field_citation_value"="field_citation_value_alt",
                                         "upper"="upper_alt","lower"="lower_alt"))
ectopic_alt$year_id_alt<-(ectopic_alt$year_start+ectopic_alt$year_end)/2
ectopic_ref$dorm_alt<- "claims"

ectopic_match<-merge(ectopic_ref, ectopic_alt, by=c("location_id", "location_name", "age_start", "age_end", "measure"))
ectopic_match$year_diff<- abs(ectopic_match$year_id_ref-ectopic_match$year_id_alt)
ectopic_match<-subset(ectopic_match, ectopic_match$year_diff<6)
ectopic_match<-plyr::rename(ectopic_match, c("age.y"="age"))
ectopic_match<-subset(ectopic_match, ectopic_match$measure=="incidence")
ectopic_match<-ectopic_match%>%
  mutate (matched_id=paste(nid_alt,nid_ref,age,year_id_ref,year_id_alt, sep = " - ") )
ectopic_match<-subset(ectopic_match, ectopic_match$mean_ref!=0&ectopic_match$mean_alt!=0)

###################################################################################################################################
############MR BRT model for crosswalking CI data

#transfer mean and se to log space
ectopic_match[,c("ref_mean","ref_se")]<-cw$utils$linear_to_log(mean=array(ectopic_match$mean_ref), sd=array(ectopic_match$standard_error_ref))
ectopic_match[,c("alt_mean","alt_se")]<-cw$utils$linear_to_log(mean=array(ectopic_match$mean_alt), sd=array(ectopic_match$standard_error_alt))
ectopic_match<-ectopic_match%>%mutate(ydiff_log_mean=alt_mean-ref_mean,
                                      ydiff_log_se=sqrt(alt_se^2+ref_se^2))

write.xlsx(ectopic_match, "FILEPATH/646_ectopic_extractions_bundle_BVID_CI_matched_pairs.xlsx", sheetName = "extraction")



#run the crosswalk model
dat1<-cw$CWData(
  df=ectopic_match,
  obs="ydiff_log_mean", #matched differences in log space
  obs_se="ydiff_log_se", #SE of matched differences in log space
  alt_dorms="dorm_alt", #var for the alternative def/method
  ref_dorms="dorm_ref",#var for the reference def/method
  covs=list("age"),# list of (potential) covariate column names
  study_id= "matched_id", #var for random intercepts' ie.e (1|study_id),
  dorm_separator = " "
)

fit1<-cw$CWModel(
  cwdata=dat1, #result of SWData () function call
  obs_type="diff_log", #must be "diff_logit" or "diff_log"
  cov_models=list (
    cw$CovModel("intercept"),
    cw$CovModel(cov_name="age",
                spline=cw$XSpline(
                  knots=c(12, 22, 37, 52), #min, 1stQ,3rdQ, maximum:(12, 22, 37, 52), originally: (20,30, 40, 50)
                  degree=2L, l_linear=TRUE, r_linear=TRUE))
  ),
  gold_dorm="inpatient" #level of 'ref_dorm" that is the gold standard,
)

fit1$fit(inlier_pct=0.9, max_iter=1000L)
results_cofficient<- fit1$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)
py_save_object(object = fit1, filename = "FILEPATH/fit1_gbd2023.pkl", pickle = "dill")
df_tmp1 <- fit1$create_result_df()
write.xlsx(df_tmp1, "FILEPATH/fit1_gbd2023.xlsx") 


#apply the adjustment
#load model results
pd <- import("pandas")
results1  <- pd$read_pickle("FILEPATH/fit1_gbd2023.pkl")
results1$fixed_vars
results1$beta_sd
results1$gamma


#replace alternative method mean=0 to be half of mean so the CIs of rows where mean=0 can get inflated
a<-subset(ectopic_ms_hcup, mean!=0 & clinical_data_type=="inpatient")
b<-subset(ectopic_ms_hcup, mean!=0 & clinical_data_type=="claims")
ectopic_ms_hcup<- ectopic_ms_hcup%>%mutate(mean_half= ifelse(mean== 0 & clinical_data_type=="inpatient", min(a$mean)/2, ifelse(mean== 0 & clinical_data_type=="claims", min(b$mean)/2, mean)),
                                           mean_1percent= ifelse(mean== 0 & clinical_data_type=="inpatient", median(a$mean)*0.01, ifelse(mean== 0 & clinical_data_type=="claims", median(b$mean)*0.01, mean)))
#apply the adjustment
prep_tmp1<-results1$adjust_orig_vals(
  df=ectopic_ms_hcup,
  orig_dorms="clinical_data_type",
  orig_vals_mean="mean_half", #mean in un-transformed space
  orig_vals_se="standard_error",
  data_id="row_id"
)
head(prep_tmp1)

ectopic_ms_hcup[, 
                c("meanvar_adjusted1", "sdvar_adjusted1", 
                  "pred_log1", "pred_se_log1", "data_id1")] <- prep_tmp1

#re-apply mean=0 to sources whose original mean=0
ectopic_ms_hcup$meanvar_adjusted1 <- ifelse(ectopic_ms_hcup$mean == 0 , 0, ectopic_ms_hcup$meanvar_adjusted1)


#clean the adjusted dataset to be merged with other claims data
ectopic_ms_hcup_adjusted<-ectopic_ms_hcup
ectopic_ms_hcup_adjusted$mean_original<-ectopic_ms_hcup_adjusted$mean
ectopic_ms_hcup_adjusted$standard_error_original<-ectopic_ms_hcup_adjusted$standard_error
ectopic_ms_hcup_adjusted$mean<-NULL
ectopic_ms_hcup_adjusted$standard_error<-NULL
ectopic_ms_hcup_adjusted<-ectopic_ms_hcup_adjusted%>%mutate(mean=ifelse(ectopic_ms_hcup_adjusted$is_reference == 0 , ectopic_ms_hcup_adjusted$meanvar_adjusted1, ectopic_ms_hcup_adjusted$mean_original),
                                                            standard_error=ifelse(ectopic_ms_hcup_adjusted$is_reference == 0 , ectopic_ms_hcup_adjusted$sdvar_adjusted1, ectopic_ms_hcup_adjusted$standard_error_original))
ectopic_ms_hcup_adjusted$lower<-ifelse(ectopic_ms_hcup_adjusted$is_reference == 0 , NA, ectopic_ms_hcup_adjusted$lower)
ectopic_ms_hcup_adjusted$upper<-ifelse(ectopic_ms_hcup_adjusted$is_reference == 0 , NA, ectopic_ms_hcup_adjusted$upper)
ectopic_ms_hcup_adjusted$uncertainty_type_value<-ifelse(ectopic_ms_hcup_adjusted$is_reference == 0 , NA, ectopic_ms_hcup_adjusted$uncertainty_type_value)

## delete the columns created for matching finding
ectopic_ms_hcup_adjusted <- ectopic_ms_hcup_adjusted %>%
  select(1:which(names(.) == "row_id"),mean, standard_error,mean_original,standard_error_original)

##Combine it with other CI data without envelope
ectopic_other_CI_no_envelope<-subset(ectopic_CI_exclude_envelope,ms_hcup=="other")
ectopic_CI_adjusted_no_envelope<- bind_rows(ectopic_ms_hcup_adjusted,ectopic_other_CI_no_envelope)
ectopic_CI_adjusted_no_envelope$crosswalk_parent_seq<-ifelse(ectopic_CI_adjusted_no_envelope$ms_hcup=="HCUP",ectopic_CI_adjusted_no_envelope$seq,NA)

write_excel_csv(ectopic_CI_adjusted_no_envelope, "FILEPATH/646_ectopic_extractions_bundle_BVID_CI_adjusted_no_envelope_Dutch.csv")
ectopic_CI_adjusted_Dutch_no_envelop<-read.csv( "FILEPATH/646_ectopic_extractions_bundle_BVID_CI_adjusted_no_envelope_Dutch.csv")


####################################################################################################################
####age splitting Scilit data
#import SciLit data adjusting for denominators
ectopic_scilit_adjusted<-read_excel( "FILEPATH/646_ectopic_extractions_bundle_BVID_scilit_denominator_adjusted.xlsx")

# Under 5 age range subset ------------------only age split data where age range >=5 years
age_pattern <- copy(ectopic_scilit_adjusted)
age_pattern <- age_pattern %>% mutate (age_range=age_end - age_start)
age_5 <- subset(age_pattern,age_range < 5)
age_5_over <- subset(age_pattern,age_range >= 5)
age_5_over <- subset(age_5_over,group_review==1|is.na(group_review))


##get objects
location_pattern_id <- 1
sex_id<-2
df <- copy(age_5_over)

## GET TABLES
ages <- get_age_metadata(release_id=OBJECT)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 10 & age_end <=55, age_group_id]
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 102]#102->age =54; originally it was age_end := 94
super_region_dt <- get_location_metadata(location_set_id = 22, release_id=16)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]

## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
original <- copy(df)
original <- as.data.table(original)
original[, id := 1:.N]

##FORMAT DATA; calculate incidence ratio and SE if it is missing
z <- qnorm(0.975)
dt <- copy(original)
dt<-original%>%mutate(mean = ifelse(is.na(mean), cases/sample_size, mean))
dt<-dt%>%mutate(standard_error=ifelse(is.na(standard_error), sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2)),standard_error))
dt<-dt%>%mutate(lower=ifelse(is.na(lower), mean - (z * standard_error),lower))
dt<-dt%>%mutate(upper=ifelse(is.na(upper), mean + (z * standard_error),upper))

table(addNA(dt$group_review))
table(dt$is_outlier)

## MAKE SURE DATA IS FORMATTED CORRECTLY
format_data <- function(unformatted_dt){
  dt <- copy(unformatted_dt)
  dt[, `:=` (mean = as.numeric(mean), standard_error = as.numeric(standard_error), lower = as.numeric(lower),upper = as.numeric(upper),
             age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
  dt <- dt[measure %in% c("incidence"),]
  dt <- dt[!group_review==0 | is.na(group_review),] ##don't use group_review 0
  dt <- dt[is_outlier==0,] ##don't age split outliered data
  dt <- dt[(age_end-age_start)>=5,]#was 25 previously
  dt <- dt[!mean == 0 & !cases == 0, ] ##don't split points with zero incidence
  dt[measure == "incidence", measure_id := 6]
  dt[, year_id := round((year_start + year_end)/2, 0)] ##so that can merge on year later
  return(dt)
}

dt <- format_data(dt)


## EXPAND AGE
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  
  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 102, age_end := 102] #change it age_end := 102, age =54; original age_end := 99
  
  ## EXPAND FOR AGE
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] ##drop the data points if cases/n.age is less than 1 (as per Theo)
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  #split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split <- merge(split, ages[, c("age_start", "age_end", "age_group_id")], by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id>6 & age_group_id<16]  ##don't keep where age group id isn't estimated for cause
  return(split)
}


split_df <- expand_age(dt, age_dt = ages)

## GET PULL LOCATIONS;; 
##GET LOCS AND POPS
pop_locs <- unique(split_df$location_id)
pop_years <- unique(split_df$year_id)
pop_years <- plyr::round_any(pop_years, 5) 

## 5.8 GET AGE PATTERN
b_id <- OBJECT#this is bundle ID
a_cause <- "ectopic pregnancy"
name <- "ectopic pregnancy"
id <- OBJECT 
ver_id <- OBJECT  
description <- paste0("age split using gbd2022, meid", id, " mv", ver_id, " age global pattern")
draws <- paste0("draw_", 0:999)
gbd_id <- id


### GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = OBJECT, 
                           location_id = locs, source = "epi", measure_id=OBJECT,
                           version_id = ver_id, sex_id = OBJECT,  release_id=OBJECT, 
                           age_group_id = age_groups, year_id = pop_years) 
  
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, age_group_id, location_id, se_dismod, rate_dis,year_id)]

  ## CASES AND SAMPLE SIZE
  age_pattern[, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
  age_pattern[is.nan(cases_us), cases_us := 0]
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id,year_id)]
  return(age_pattern)
}

age_pattern <- get_age_pattern(locs = pop_locs, id = gbd_id, age_groups = age)
age_pattern[,location_id:=super_region_id]
age_pattern1 <- copy(age_pattern)
setnames(split_df, "year_id", "orig_year_id")
split_df <- split_df[, year_id:=plyr::round_any(orig_year_id, 5)]
split_df <- merge(split_df, age_pattern1, by = c("age_group_id", "year_id", "location_id"))



## GET live births INFO
get_live_births_structure <- function(locs, years, age_groups){
  population <- get_population(location_id = locs, year_id = years,release_id=OBJECT, #decomp_step = "step2",
                               sex_id = OBJECT, age_group_id = age_groups)
  asfr <- get_covariate_estimates(covariate_id=OBJECT, age_group_id = age_groups, location_id=locs, year_id=years,
                                  release_id=OBJECT, sex_id=OBJECT)
  setnames(asfr, "mean_value", "asfr")
  demogs <- merge(asfr, population, by=c("location_id", "year_id", "age_group_id"))
  demogs[, live_births:=asfr*population]
  demogs[, c("location_id", "year_id", "age_group_id", "live_births")]
  return(demogs)
}
#print("getting pop structure")
births_structure <- get_live_births_structure(locs = pop_locs, years = pop_years, age_groups = age)

split_df <- merge(split_df, births_structure, by = c("location_id",  "year_id", "age_group_id"))

## CALCULATE AGE SPLIT POINTS#######################################################################
## CREATE NEW POINTS 
split_data <- function(raw_dt){
  dt1 <- copy(raw_dt)
  dt1[, total_births := sum(live_births), by = "id"]
  dt1[, sample_size := (live_births / total_births) * sample_size]
  dt1[, cases_dis := sample_size * rate_dis]
  dt1[, total_cases_dis := sum(cases_dis), by = "id"]
  dt1[, total_sample_size := sum(sample_size), by = "id"]
  dt1[, all_age_rate := total_cases_dis/total_sample_size]
  dt1[, ratio := mean / all_age_rate]
  dt1[, mean := ratio * rate_dis ]
  dt1 <- dt1[mean < 1, ]
  dt1[, cases := mean * sample_size]
  return(dt1)
}


split_df <- split_data(split_df)
split_df[, year_id:=orig_year_id]

##combine with unsplitted rows in age range >5
split_df$crosswalk_parent_seq <- NA
split_df$crosswalk_parent_seq <- as.numeric(split_df$crosswalk_parent_seq)
split_df$note_modeler <- NA
date <- gsub("-", "_", Sys.Date())

get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

#─ For unadjusted rows, set crosswalk_parent_seq to null
#─ For adjusted rows, set crosswalk_parent_seq to seq
#─ For new rows that were split, set seq to null and crosswalk_parent_seq to the seq of the original, non-split row
#─ For new rows that were collapsed, set seq to null and crosswalk_parent_seq to one of the original, noncollapsed rows of your choosing. You may also want to create a new column to record all of the original seqs that contributed to your collapsed row.
format_data_forfinal <- function(unformatted_dt, location_split_id, original_dt){
  dt <- copy(unformatted_dt)
  dt[, group := 1]
  dt[, specificity := "age"]
  dt[, group_review := 1]
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  # dt <- col_order(dt)
  dt[, note_modeler := paste0(note_modeler, "| age split using the location-year age pattern", date)]
  split_ids <- dt[, unique(id)]
  dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
  dt <- dt[, c(names(df),"crosswalk_parent_seq"), with = F]
  return(dt)
}

final_age_dt <- format_data_forfinal(split_df, location_split_id = location_pattern_id,
                                     original_dt = original)

final_age_dt<- final_age_dt[group_review==1 | is.na(group_review), ]
final_age_dt <- final_age_dt[standard_error >1, standard_error := NA]
final_age_dt[!is.na(crosswalk_parent_seq), seq := NA]


## combine with row in age range <5
age_5b <- subset(age_5, group_review==1 | is.na(group_review))
final <- as.data.table(plyr::rbind.fill(final_age_dt, age_5b))


#basic validation checks 
final[location_id ==95]
final[is.na(age_start)]
final[is.na(age_end)]
final[is.na(location_id)]
final[is.na(crosswalk_parent_seq) & is.na(seq), ]
final[!is.na(crosswalk_parent_seq) & !is.na(seq), ]
final[, unit_value_as_published := 1]
n_occur <- data.frame(table(final$seq))
n_occur[n_occur$Freq > 1,]
final <- subset(final, !(mean==0 & is.na(lower) & is.na(cases) & is.na(sample_size) & is.na(standard_error) & is.na(effective_sample_size)))
final <- subset(final, !(nid == 516858 & age_end == 34 & age_start == 30)) #duplicated group removed


# save the data
write.xlsx(final, "FILEPATH/646_ectopic_extractions_bundle_BVID_scilit_age_split.xlsx", sheetName = "extraction")


####################################################################################################################
####prepare data for saving crosswalk version 
# combine CI data and SciLit data
ectopic_CI_adjusted_Dutch_no_envelop<-read.csv( "FILEPATH/646_ectopic_extractions_bundle_BVID_CI_adjusted_no_envelope_Dutch.csv")
ectopic_scilit_adjusted_age_split<-read_excel( "FILEPATH/646_ectopic_extractions_bundle_BVID_scilit_age_split.xlsx")
ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split <- bind_rows(ectopic_CI_adjusted_Dutch_no_envelop, ectopic_scilit_adjusted_age_split)

# add required columns for saving crosswalk version 
ectopic_CI_adjusted_Dutch_MAD_scilit_age_split$sex<-"Female"
ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split$sex<-"Female"

# outlier the sources for near miss ectopic pregnancy only
ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split <- ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split[cv_diag_severe==1, is_outlier:=1][cv_diag_severe==1, note_modeler:="outliered for near-miss case definition"]

# outlier age group 10-14, and age group 50-54
ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550<-copy(ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split)
ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$is_outlier<-ifelse(ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$age_start %in% c(10,50),1,ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$is_outlier)
ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$note_modeler<-ifelse(ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$age_start %in% c(10,50),"outliered for extreme age groups",ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$note_modeler)

table(ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$ms_hcup)
table(ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550$is_outlier)
ectopic_crosswalked<-ectopic_CI_adjusted_Dutch_no_envelop_scilit_age_split_1550%>% mutate(is_outlier=ifelse(ms_hcup %in% c("MS"), 1,is_outlier))
ectopic_crosswalked<-ectopic_crosswalked%>% mutate(is_outlier=ifelse(location_name %in% c("Alaska","Delaware","District of Columbia","Georgia","Kansas","Maine","Minnesota","Mississippi",
                                                                                          "Nebraska","New Mexico","Oregon", "Rhode Island","South Carolina","South Dakota","Utah","Vermont",
                                                                                          "West Virginia"), 1,is_outlier))


##Save the data
write.xlsx(ectopic_crosswalked, "FILEPATH/646_ectopic_bundle_BVID_CI_adjusted_no_envelope_Dutch_scilit_age_split_1550_outlierMS_subnations.xlsx", sheetName = "extraction")


###########################################################################################################
###save a crosswalk version
bundle_version_id <- OBJECT
path_to_data <-"FILEPATH/646_ectopic_bundle_BVID_CI_adjusted_no_envelope_Dutch_scilit_age_split_1550_outlierMS_subnations.xlsx"
description <- "GBD_2023 CI adjusted, dutch, no envelope, Scilit age splitted data, age15-50, no near miss,no MS,bundle_BVID"
result <- save_crosswalk_version(
  bundle_version_id,
  path_to_data,
  description=description)
print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

