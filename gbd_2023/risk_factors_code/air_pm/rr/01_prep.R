
#-------------------Header------------------------------------------------
# Author: 
# Editor: 
# Purpose: Prep PM2.5 RR data for MR-BeRT
#          


#------------------Set-up--------------------------------------------------
# clear memory
rm(list=ls())


# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx","pbapply","ggplot2", "tidyverse")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

#---------------------------

hap.exp.date <- "082124"

cvd_ages <- seq(25,95,5) #full ages
terminal.age <- 110 

set.seed(143) 

# ---------------------------Functions & directories ---------------------------------------------

home_dir <- "FILEPATH"

in_dir <- file.path("FILEPATH")

out_dir <- file.path("FILEPATH")
dir.create(out_dir,recursive=T)

graphs_dir <- file.path("FILEPATH")
dir.create(graphs_dir, recursive=T)

source(file.path(central_lib,"current/r/get_location_metadata.R"))
locations <- get_location_metadata(35, gbd_round_id = 9, release_id = 16)#, decomp_step = "iterative"
source(file.path(central_lib,"current/r/get_covariate_estimates.R")) # pulls covariates, used to get air pollution

'%ni%' <- Negate('%in%')

# ---------------------------Read in data (RR, HAP mapping, OAP exposure)------------------------------------------

data.hap <- openxlsx::read.xlsx(paste0("FILEPATH"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.hap) <- openxlsx::read.xlsx(paste0("FILEPATH"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
colnames(data.hap) <-  gsub("cv_", "cov_", colnames(data.hap))

data.hap <- data.hap[!is.na(ier_source) & is_outlier==0]
data.hap <- data.hap[ier_cause %ni% c("bw","ga","cataract")] # removes three hap only causes

# OAP (adding new data for GBD2020 on 08/19/2020)

data.oap <- openxlsx::read.xlsx("FILEPATH",sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.oap) <- openxlsx::read.xlsx(paste0("FILEPATH/extraction_ier_oap_GBD2020_090320.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
colnames(data.oap) <-  gsub("cv_", "cov_", colnames(data.oap))

data.oap <- data.oap[is_outlier==0 & !is.na(ier_source)]
data.oap <- data.oap[!is.na(nid)]


# DEMENTIA (testing adding dementia as a new outcome on 08/27/21)
 
data.dementia <- openxlsx::read.xlsx("FILEPATH", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.dementia) <- openxlsx::read.xlsx(paste0("FILEPATH"), colNames=T, skipEmptyCols = F, rows=1) %>% names
data.dementia <- data.dementia[is_outlier==0 & !is.na(nid)]
data.oap <- rbind(data.oap, data.dementia)
colnames(data.oap) <-  gsub("cv_", "cov_", colnames(data.oap))

# Adding new extracted data (08/26/2024)
data <- fread("FILEPATH", sheet = "extraction")

# Set the column names to the values in the first row -- the other approach is rerroring so I am changing the approach
setnames(data.hapin, old = names(data.hapin), new = as.character(unlist(data.hapin[1,])))
data.hapin <- data.hapin[-(1:4),]


data.hapin <- data.hapin[!is.na(nid)]


# changin acause to ier.cause and filtering to applicable causes ----------

data.hapin <- data.hapin %>% rename(ier_source = acause) %>% filter(ier_source %in% c("Lower respiratory infections",
                                                                                 "Cardiovascular diseases",
                                                                                 "Ischemic heart disease",
                                                                                 "Stroke", "Ischemic stroke"))


hap_exp <- fread(paste0("FILEPATH"))
hap_exp <- hap_exp[,.(location_id,year_id,median,grouping)] # we only need these columns
hap_exp <- dcast(hap_exp, location_id + year_id ~ grouping, value.var = "median") # make wide by group

# Ambient exposure data
air_pm <- get_covariate_estimates(covariate_id = 106,
                                  gbd_round_id= 9, #7,
                                  release_id= 16)
air_pm[,ambient_exp_mean:=mean_value]


# --------------------------- PREP OAP data -------------------------------------
setnames(data.oap, c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment"),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment"))

# if IQR is a range, convert to numeric
convert_to_numeric <- function(x){
  iqr <- strsplit(x, "-")
  iqr <- as.numeric(iqr[[1]])
  numeric <- iqr[2]-iqr[1]
  return(numeric)
}

data.oap <- data.oap[grepl("-",oap_conc_iqr), oap_conc_iqr:=convert_to_numeric(oap_conc_iqr)]
data.oap[,oap_conc_iqr:=as.numeric(oap_conc_iqr)]

#estimate mean/sd if missing
data.oap[is.na(conc_mean), conc_mean := oap_conc_median]
data.oap[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.oap[is.na(conc_sd), conc_sd := (oap_conc_max-oap_conc_min)/4]  #SD can be estimated by range/4

#estimate concentration p5/p95 from mean/sd using z if necessary
data.oap[is.na(conc_5), conc_5 := conc_mean - conc_sd * 1.645]
data.oap[is.na(conc_95), conc_95 := conc_mean + conc_sd * 1.645]

#if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.oap[conc_5 < oap_conc_min, conc_5 := oap_conc_min]
data.oap[conc_95 > oap_conc_max, conc_95 := oap_conc_max]

#if the mean/median ar both missing, set to midpoint of 5th and 95th
data.oap[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]

data.oap[,conc_increment:=as.numeric(conc_increment)]

# shift the RRs using p95/p5 range concentration increment
data.oap[, exp_rr := mean ^ ((conc_95-conc_5)/conc_increment)]
data.oap[, exp_rr_lower := lower ^ ((conc_95-conc_5)/conc_increment)]
data.oap[, exp_rr_upper := upper ^ ((conc_95-conc_5)/conc_increment)]

# also generage RRs and betas per 1 unit
data.oap[, exp_rr_unit := mean ^ (1/conc_increment)]
data.oap[, exp_rr_unit_lower := lower ^ (1/conc_increment)]
data.oap[, exp_rr_unit_upper := upper ^ (1/conc_increment)]


# --------------------------- PREP HAP data -------------------------------------

# merge data.hap and data.hapin -- they do not map so I need to figure out how to merge these two


# changes to the data.hapin -----------------------------------------------

names.hapin <- names(data.hapin)

int_names <- grep("^int_", names.hapin, value = TRUE)
cohort_names <- grep("^cohort_", names.hapin, value = TRUE)
cc_names <- grep("^cc_", names.hapin, value = TRUE)

other_names <- grep("^(int_|cohort_|cc_)", names.hapin, value = TRUE, invert = TRUE)


colnames_RCT <- c(other_names, int_names)
colnames_cohort <- c(other_names, cohort_names)
colnames_cc <- c(other_names, cc_names)

RCT <- data.hapin[design == "Randomized controlled trial", ]
RCT <- RCT %>% select(colnames_RCT)
RCT <- RCT %>% select(!c(source_type, measure)) %>%
  rename(source_type = design, 
                          measure = effect_size_measure,
                          mean = effect_size,
                          # person_years = cohort_person_years_total, 
                          hap_exposed_def = exp_def)

#cases: In the new extraction they are separated by intervention and comparison; I am not sure this is needed so I am going to add the number of cases
# between intervention and comparison group

RCT <- RCT %>% mutate(cases = as.numeric(int_number_events_int) + as.numeric(int_number_events_comp)) %>%
  select(!c(int_number_events_int, int_number_events_comp))

#sample size: there is no baseline sample size for locations; so I am summing 

RCT <- RCT %>% mutate(sample_size = as.numeric(int_sample_size_int_group_baseline) + as.numeric(int_sample_size_comparison_group_baseline)) %>%
  select(!c((grep("sample_size_", names(RCT), value = TRUE))))


RCT <- RCT %>% rename( duration_fup_measure = exp_fup_num) %>% 
  select(!c( grep("^int_fup_", names(RCT), value = TRUE)))
  
# EXPOSURE: there are 33 variables with "exp_" in its name -- only (all variables start with int_baseline_exp so I am going to omit it here;) _int_mean, _int_sd, 
# _comp_mean, _comp_sd have values so I am ging to rename those and confirm that when I rbind min, max, increment, 5, 95 and iqr are NAs. 

RCT <- RCT %>% rename(oap_conc_mean_int = int_baseline_exp_int_mean_PM2.5,
                      oap_conc_sd_int = int_baseline_exp_int_sd_PM2.5,
                      oap_conc_sd_comp = int_baseline_exp_comp_sd_PM2.5,
                      oap_conc_mean_comp = int_baseline_exp_comp_mean_PM2.5)

RCT <- RCT %>% select(!c(grep("exp_", names(RCT), value = TRUE)))
RCT <- RCT %>% select(!c(grep("^int_", names(RCT), value = TRUE)))


# changes to cohort studies: selecting all variables that start with "cohort" --------

cohort <- data.hapin[design == "Prospective cohort", ]
cohort <- cohort %>% select(colnames_cohort)
cohort <- cohort %>% select(!c(source_type, measure)) %>%
  rename(source_type = design, 
         measure = effect_size_measure,
         mean = effect_size,
         # person_years = cohort_person_years_total, 
         hap_exposed_def = exp_def)

#cases: In the new extraction they are separated by intervention and comparison; I am not sure this is needed so I am going to add the number of cases
# between intervention and comparison group

cohort <- cohort %>% mutate(cases = as.numeric(cohort_number_events_total)) %>%
  select(!c(grep("events_", names(cohort), value = TRUE)))

#sample size: there is no baseline sample size for locations; so I am summing 

cohort <- cohort %>% mutate(sample_size = as.numeric(cohort_total_cohort_sample_size)) %>%
  select(!c((grep("sample_size", names(cohort), value = TRUE))))

#person year and follow-up: 

cohort <- cohort %>% mutate( duration_fup_measure = exp_fup_num) %>% 
  select(!(exp_fup_num))

cohort <- cohort %>% mutate(person_year = as.numeric(cohort_person_years_total)) %>% 
  select (!c(cohort_person_years_exp, cohort_person_years_unexp, cohort_person_years_total))
# EXPOSURE: there are 33 variables with "exp_" in its name -- only (all variables start with int_baseline_exp so I am going to omit it here;) _int_mean, _int_sd, 
# _comp_mean, _comp_sd have values so I am ging to rename those and confirm that when I rbind min, max, increment, 5, 95 and iqr are NAs. 

cohort <- cohort %>% mutate(oap_conc_mean = cohort_reported_exp_mean,
                      oap_conc_sd = cohort_reported_exp_sd,
                      oap_conc_median = cohort_reported_exp_median,
                      oap_conc_min = cohort_reported_exp_min,
                      oap_conc_max = cohort_reported_exp_max)

cohort <- cohort %>% select(!c(grep("exp_", names(cohort), value = TRUE)))

cohort <- cohort%>% select(!c(grep("cohort_", names(cohort), value = TRUE)))


# I want to do an Rbind first between RCT and cohort and then with data.hap to see what comes out of that 

data.hapin_selected_columns <- rbind(RCT, cohort, fill = TRUE)



nas_data_hapin <- c("shortened_citation", "year_issue", "age_demographer", "age_issue", "sex_issue", "percent_male", "sampling_type", "rep_geography", "rep_selection_criteria", 
                    "rep_prevalent_disease", "notes_outcome_def", "notes_exposed_group_def", "notes_unexposed_reference_group_def", "bw_mean_int", "bw_sd_int", "bw_mean_control", 
                    "bw_sd_control", "bw_mean_total", "bw_sd_total", "bw_unit", "ga_mean_intervention_baseline", "ga_sd_intervention_baseline", "ga_mean_control_baseline", 
                    "ga_sd_control_baseline", "ga_unit_baseline", "outcome_def_flag", "outcome_assess_1", "outcome_assess_2", "outcome_assess_3", "Shortened_effect_size_description", 
                    "nonCI_uncertainty_value", "nonCI_uncertainty_type", "effect_size_multi_location", "pooled_cohort", "dose_response", "dose_response_detail", "confounders_age", "confounders_sex",
                    "confounders_smoking", "confounders_alcohol_use", "confounders_physical_activity", "confounders_dietary_components", "confounders_bmi",
                    "confounders_hypertension", "confounders_diabetes", "confounders_hypercholesterolemia", "confounders_other", "keep", "specificity", 
                    "group_review", "duplicate_with_other_study", "duplicate_with_other_study_explained", "notes_selection_bias_follow_up", "input_type")
data.hapin_selected_columns <- data.hapin_selected_columns %>%
  mutate(rei = ifelse(rei == "Household air pollution from solid fuels", "air_hap", "air_pm"))

data.hapin_selected_columns <- data.hapin_selected_columns %>% rename(page_num = page_num_effect_size, 
                                                                      table_num = table_num_effect_size,
                                                                      uncertainty_type_value = CI_uncertainty_type_value,
                                                                      education =confounders_education,
                                                                      income = confounders_income,
                                                                      note_SR = note_sr,
                                                                      person_years = person_year)

data.hapin_selected_columns$new_gbd2023 <-1 
data.hapin_selected_columns <- data.hapin_selected_columns %>% select(!c(nas_data_hapin))

data.hap <- data.hap %>% mutate(rei = "air_hap") %>% select(!c(ier_source))
data.hapin_selected_columns <- data.hapin_selected_columns %>% rename(ier_cause = ier_source) %>%
  filter(ier_cause %in% c("Lower respiratory infections", "Ischemic heart disease", "Stroke"))

data.hapin_selected_columns <- data.hapin_selected_columns %>%  
  mutate(ier_cause = case_when(ier_cause == "Lower respiratory infections" ~"lri",
                               ier_cause == "Ischemic heart disease"~ "cvd_ihd",
                               TRUE ~ "cvd_stroke"))
data.hapin_selected_columns <- data.hapin_selected_columns %>% select(!c(notes_exposure_def, outcome_def))


data.hap <- data.hap %>% select(!c(new_gbd2019))
data.hapin_selected_columns <- data.hapin_selected_columns %>% rename(ier_cause_measure = outcome_type )%>%
  mutate(ier_cause_measure = case_when(ier_cause_measure == "Incidence & Mortality" ~ "incidence & mortality",
                                       TRUE ~ "mortality"))

data.hapin_selected_columns <- data.hapin_selected_columns %>% select(!c("group " ,"custom_*", "effect_size_coefficient", "effect_size_unit"))

data.hapin_selected_columns <- data.hapin_selected_columns %>% rename(cov_counfounding.uncontroled = cov_confounding_uncontrolled)
setdiff( names(data.hapin_selected_columns), names(data.hap))

data.hap_merged <- rbind(data.hap, data.hapin_selected_columns, fill = TRUE)



# -------------------------------------------------------------------------


setnames(data.hap_merged, c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment", 
                     "oap_conc_mean_comp", "oap_conc_mean_int",  "oap_conc_sd_int",   "oap_conc_sd_comp" ),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment", "conc_mean_comp", "conc_mean_int", "conc_sd_int", "conc_sd_comp"))

# find midpoint of study
data.hap_merged[,year_id:=round((as.numeric(year_start)+as.numeric(year_end))/2)]

# If studies report some measure of HAP (includes both fuel use and ambient), use that
# some HAP studies already have concentration
data.hap_merged[is.na(conc_mean), conc_mean := oap_conc_median]
data.hap_merged[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.hap_merged[is.na(conc_sd), conc_sd := (as.numeric(oap_conc_max)-as.numeric(oap_conc_min))/4]  #SD can be estimated by range/4

# estimate concentration p5/p95 from mean/sd using z if necessary
data.hap_merged[is.na(conc_5), conc_5 := as.numeric(conc_mean) - as.numeric(conc_sd) * 1.645]
data.hap_merged[is.na(conc_95), conc_95 := as.numeric(conc_mean) + as.numeric(conc_sd) * 1.645]

# if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.hap_merged[as.numeric(conc_5) < as.numeric(oap_conc_min), conc_5 := as.numeric(oap_conc_min)]
data.hap_merged[as.numeric(conc_95) > as.numeric(oap_conc_max), conc_95 := as.numeric(oap_conc_max)]

data.hap_merged[,conc_increment:=as.numeric(conc_increment)]


# for categorical hap exposures, no need to shift -- I am not sure what a categorical exposure is
data.hap_merged[is.na(conc_increment), exp_rr := as.numeric(mean)]
data.hap_merged[is.na(conc_increment), exp_rr_lower := as.numeric(lower)]
data.hap_merged[is.na(conc_increment), exp_rr_upper := as.numeric(upper)]

# If studies don't report anything about HAP (what the actual PM2.5 concentration is), we use our ambient and HAP estimates
# merge on ambient and hap exposures to calculate 5th and 95th percentiles
# 5th percentile is ambient exposure; 95th percentile is the ambient + HAP exposure

# merge on ambient first
data.hap_merged <- data.hap_merged[ ,location_id := as.character(location_id)]
air_pm[, location_id := as.character(location_id)]
data.hap_merged <- merge(data.hap_merged,air_pm[,.(location_id,year_id,ambient_exp_mean)], by=c("year_id","location_id"), all.x=T)

# merge on hap exposure
data.hap_merged[year_id < min(hap_exp$year_id), year_id:=min(hap_exp$year_id)]

# data.hap <- data.hap[ ,location_id := as.character(location_id)]
setDT(hap_exp)
hap_exp[, location_id := as.character(location_id)]
data.hap_merged <- merge(data.hap_merged, hap_exp, by= c("year_id","location_id"), all.x=T)

# assign hap exposure based on age and gender -- age_mean is not a values for all the rows so I need to create that
data.hap_merged[age_mean<18, household_exp_mean:=child]
data.hap_merged[age_mean>18 & sex=="Female", household_exp_mean:=female]
data.hap_merged[age_mean>18 & sex=="Male", household_exp_mean:=male]
data.hap_merged[age_mean>18 & sex=="Both", household_exp_mean:=((female+male)/2)]

data.hap_merged$age_start <- as.numeric(data.hap_merged$age_start)
data.hap_merged$age_end <- as.numeric(data.hap_merged$age_end)
data.hap_merged[is.na(household_exp_mean) & (((age_start+age_end)/2)<18), household_exp_mean:=child]
data.hap_merged[is.na(household_exp_mean) & (((age_start+age_end)/2)>18) & sex=="Female", household_exp_mean:=female]
data.hap_merged[is.na(household_exp_mean) & (((age_start+age_end)/2)>18) & sex=="Male", household_exp_mean:=male]
data.hap_merged[is.na(household_exp_mean) & (((age_start+age_end)/2)>18) & sex=="Both", household_exp_mean:=((female+male)/2)]

# for now setting multi country studies to global average for gbd2019 #TODO: FIX
data.hap_merged[location_id==1 & year_id==2000,ambient_exp_mean:=39.58]
data.hap_merged[location_id==1 & year_id==2008,ambient_exp_mean:= 36.43]

data.hap_merged[,conc_5:=as.numeric(conc_5)]
data.hap_merged[,conc_95:=as.numeric(conc_95)]
data.hap_merged[is.na(conc_95),conc_95:=ambient_exp_mean+household_exp_mean]
data.hap_merged[is.na(conc_5),conc_5:=ambient_exp_mean]


# for categorical hap exposures, shift by range of exposed to unexposed
data.hap_merged$mean <- as.numeric(data.hap_merged$mean)


data.hap_merged[is.na(conc_increment), exp_rr_unit := mean ^ (1/(as.numeric(conc_95)-as.numeric(conc_5))) ]
data.hap_merged[is.na(conc_increment), exp_rr_unit_lower := is.numeric(lower) ^ (1/(conc_95-conc_5)) ]
data.hap_merged[is.na(conc_increment), exp_rr_unit_upper := is.numeric(upper) ^ (1/(conc_95-conc_5)) ]

# set the mean/median to be the midpoint
data.hap_merged[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]


#  ----------------------- Combine datasets  ------------------------------------------
#adapting data.oap

setdiff(names(data.oap), names(data.hap_merged))
setdiff(names(data.hap_merged), names(data.oap))

data.oap <- data.oap %>% mutate(rei = "air_pm") %>% select(!c(ier_source))
data.oap <- data.oap %>% select(!c(new_gbd2020))

# ALL DATA PREP -----------------------------------------------------------

data <- rbind(data.oap,data.hap_merged,fill=T,use.names=T)
# data <- data.oap # for dementia test

# generate ln_rr and ln_rr_se
data[,ln_rr:=log(exp_rr)]
data[,ln_rr_lower:=log(exp_rr_lower)]
data[,ln_rr_upper:=log(exp_rr_upper)]
data[,ln_rr_se:=(ln_rr_upper-ln_rr_lower)/3.92]



# int: received hapin intervention
data[is.na(conc_mean_int),ref_risk_lower:=conc_5][is.na(conc_mean_comp),ref_risk_upper:=ref_risk_lower+0.00001]
data[is.na(conc_mean_int),alt_risk_lower:=conc_95][is.na(conc_mean_comp),alt_risk_upper:=alt_risk_lower+0.00001] 
old_hapin_values <- data[!is.na(conc_mean_int),.(ref_risk_lower,ref_risk_upper, alt_risk_lower, alt_risk_upper)]

# Convert variables to numeric to ensure correct calculations
data$conc_mean_int <- as.numeric(data$conc_mean_int)
data$conc_sd_int <- as.numeric(data$conc_sd_int)
data$sample_size <- as.numeric(data$sample_size)
data$conc_mean_comp <- as.numeric(data$conc_mean_comp)
data$conc_sd_comp <- as.numeric(data$conc_sd_comp)

data[!is.na(conc_mean_int),ref_risk_lower := conc_mean_comp - 1.96 *(conc_sd_comp/sqrt(sample_size))]
data[!is.na(conc_mean_int),ref_risk_upper := conc_mean_comp + 1.96 *(conc_sd_comp/sqrt(sample_size))]
data[!is.na(conc_mean_comp),alt_risk_lower := conc_mean_int - 1.96 *(conc_sd_int/sqrt(sample_size))]
data[!is.na(conc_mean_comp),alt_risk_upper := conc_mean_int + 1.96 *(conc_sd_int/sqrt(sample_size))]
new_hapin_values <- data[!is.na(conc_mean_int),.(sample_size, conc_mean_int, conc_sd_int, conc_mean_comp, conc_sd_comp, 
                                                 ref_risk_lower,ref_risk_upper, alt_risk_lower, alt_risk_upper)]
# handling HAPIN global study ---------------------------------------------


data_na <- data[is.na(ln_rr_se),]
data <- data[!is.na(ln_rr_se)]

# remove data missing conc_increment exposure info (only NID 380008)
data <- data[nid!=380008]

# create age variable to represent median age at followup

# estimate average follow up
data[duration_fup_units=="weeks",value_of_duration_fup:=as.numeric(value_of_duration_fup)/52]
data[duration_fup_units=="months",value_of_duration_fup:=as.numeric(value_of_duration_fup)/12]

data[duration_fup_measure %in% c("mean","median"), median_fup:=value_of_duration_fup]

# calculates an average ratio of median follow up to study length to calculate median follow up when it is missing
data$year_end <- as.numeric(data$year_end)
data$year_start <- as.numeric(data$year_start)
data$median_fup <- as.numeric(data$median_fup)
fup_ratio <- data[year_end-year_start>=median_fup,(median_fup)/(year_end-year_start)] %>% unique() %>% mean(na.rm=T)
data[is.na(median_fup),median_fup:=(year_end-year_start)*fup_ratio]

data[is.na(age_mean),age_mean:=(age_start+age_end)/2]

# this is an estimate of average age over the whole cohort. It takes the median age and half of median followup

data[,median_age_fup:=as.numeric(age_mean)+median_fup/2]

# create child/adult indicator for LRI
data[ier_cause=="lri",child:=0]
data[ier_cause=="lri" & age_end<=5,child:=1]


# mortality v incidence variable
# assuming mortality unless evidence otherwise
data[,incidence:=0]
data <- data %>% 
  mutate(cov_incidence = ifelse(grepl("incidence", outcome, ignore.case = TRUE), 1, 0))

#european cov

europe <- c("Netherlands|NLD", "Czech Republic|CZE", "United Kingdom|GBR", "Italy|ITA" , "Oslo|NOR_4910", "Stockholm|SWE_4944",
            "Sweden|SWE", "Denmark|DNK", "France|FRA", "Greece|GRC", "Germany|DEU", "Finland|FIN", "Norway|NOR", "Spain|ESP", "Austria|AUT") 

data[, cov_europe := ifelse(location_name %in% europe, 1, 0)]
# fill missingness in follow_up covariate with maximum
data[is.na(cov_selection_bias),cov_selection_bias:=2]
# data[cov_selection_bias=="Not reported",cov_selection_bias:=2] # for dementia

# ----------------------- Recode covs for compatibility w/ new MRBRT--------------------------
# these are the covariates we need to recode: cov_duration_fup, cov_subpopulation, cov_exposure_population,
# cov_exposure_selfreport, cov_exposure_study, cov_outcome_selfreport, cov_outcome_unblinded, cov_reverse_causation,
# cov_confounding_nonrandom, cov_confounding_uncontrolled, cov_selection_bias

# make a linear_exp and a log_exp -- there is no 
data[,linear_exp := alt_risk_lower - ref_risk_lower]
data[,ln_exp:=log(1+linear_exp)]

# duration follow-up (these have already been converted so their units are "years")
data[,cov_duration_fup:=ifelse(value_of_duration_fup<median(data$value_of_duration_fup,na.rm=T),1,0)]
data[is.na(cov_duration_fup),cov_duration_fup:=1] # if duration follow-up is missing, count is as a 1

# confounding uncontrolled
# for this covariate, I am only coding one level to avoid a singular matrix error when using CovFinder
setnames(data, "cov_counfounding.uncontroled","cov_confounding_uncontrolled")
data[cov_confounding_uncontrolled==20,cov_confounding_uncontrolled:=0] # one of the observations was extracted with an error


# fill this is because this is an extraction error
data[is.na(cov_confounding_nonrandom),cov_confounding_nonrandom:=1]

# test ier_source as a covariate!

data[, cov_hap := ifelse(rei=="air_hap",1,0)]


# make a covariate for mean age
data[, cov_age_mean := age_mean]

# ## define covariate names
cov_names <- c("cov_duration_fup", "cov_subpopulation","cov_exposure_population", "cov_exposure_selfreport", "cov_exposure_study", "cov_outcome_selfreport",
               "cov_outcome_unblinded", "cov_reverse_causation", "cov_confounding_nonrandom",
               "cov_confounding_uncontrolled", "cov_selection_bias","cov_hap","cov_age_mean", "cov_reverse_causation", "cov_incidence")


# remove covariates that all have the same values
for (name in cov_names){
  if(nrow(unique(data[,..name]))==1){
    data[,(name):=NULL]
  }
}

length(data)

cov_names <- names(data)[grep("cov",names(data))]


#  ----------------------- Weight sample sizes for studies with multiple observations --------------------------

# new GBD 2020: for multiple datapoints from the same study, we want to weight their SEs by sqrt(n)
weight_ss <- function(n){
  
  # make a temporary dataset for each nid
  temp <- data[nid==n]
  
  # if sample size is empty, assign it to 1 (so we can avoid NAs; we'll put it back at the end)
  temp[is.na(sample_size),sample_size:=1]
  
  if (nrow(temp)>1){
    
    # if there are >1 observation, loop through causes
    causes <- unique(temp$ier_cause)
    for(c in causes){
      
        # do each sample size separately (because we only want to weight observations that have the same sample size)
        ss <- unique(temp[ier_cause==c,sample_size])
        for (s in ss){
          
          n_observations <- nrow(temp[ier_cause==c & sample_size==s])
          temp[ier_cause==c & sample_size==s,ln_rr_se_weighted:=(ln_rr_se*sqrt(n_observations))]
          # temp[ier_cause==c & sample_size==s,log_se_weighted:=(log_se + log(sqrt(n_observations)))]
        }
    }
    
  } else {
    
    # if there is only 1 observation for that nid, we can skip the whole weighting process
    temp[,ln_rr_se_weighted:=ln_rr_se]
  }
  
  # put back the NA for sample size
  temp[sample_size==1,sample_size:=NA]
  
  return(temp)
}
NIDList <- as.list(unique(data$nid))
temp <- pblapply(unique(data$nid),weight_ss,cl=4) %>% rbindlist

# plot to check weighted vs original SEs
plot <- ggplot(temp, aes(x = ln_rr_se_weighted, y = ln_rr_se, color = nid)) +
  geom_point()+
  geom_abline(slope = 1, intercept = 0)
print(plot)

data <- temp

#  ----------------------- Liz update for new MR-BRT -------------------------------------------
# need columns specified here: https://hub.ihme.washington.edu/pages/viewpage.action?spaceKey=MSCA&title=The+Burden+of+Proof+Pipeline#expand-Howtoformattheinputdata
# assume that all risk type is continuous?? 
data[,risk_type:="continuous"]
data[,seq:= NA] #seq(from, to, by,length. out)


#there should be a different bundle ID for each RO pair 
data[,bundle_id := 10046][,crosswalk_version_id:=NA]

# risk unit
data$risk_unit <- "ug/m3" # unit of risk == ug/m3 ? 
data[,design:=source_type]

### For adjusted rows, set crosswalk_parent_seq to seq.
data[,crosswalk_parent_seq:=NA] # is NA the same as null?


data[,input_type:="extracted"]


data[is.na(age_end), age_end:=99]
data[is.na(age_start), age_start:=0]

data[,measure:="relrisk"] # 'measure' must be one of the following: mtstandard, relrisk, risk_diff. 

# change design columns to all lower case tolower ... 
data$design <- tolower(data$design)
data[sex=="both ", sex:="Both"]
data[sex=="Both ", sex:="Both"]
data[sex=="both", sex:="Both"]
data[sex=="female", sex:="Female"]
data[sex=="male", sex:="Male"]



# converting the non-binary variables to binary 
data[, cov_confounding_uncontrolled_1 := ifelse(cov_confounding_uncontrolled == 1, 1, 0)]
data[, cov_confounding_uncontrolled_2 := ifelse(cov_confounding_uncontrolled == 2, 1, 0)]
data[, cov_selection_bias_1 := ifelse(cov_selection_bias == 1, 1, 0)]
data[, cov_selection_bias_2 := ifelse(cov_selection_bias == 2, 1, 0)]
data[, c("cov_confounding_uncontrolled","cov_selection_bias") := NULL]

warning("using weighted rr se")
data[, ln_rr_se:=ln_rr_se_weighted]


data <- data %>% mutate(effect_size_measure = "relative risk") #%>%


data <- data %>%
  mutate(rei = "air_pmhap",
                        underlying_nid = as.integer(underlying_nid),
                        acause = ier_cause, 
                        is_outlier = 0,
                        age_mean = ifelse(age_mean == 0 , NA, age_mean),
                        design = ifelse(design == 'randomized controlled trial', 'randomized control trial', design)) 

data <- data %>% mutate(acause = ifelse(acause == "t2_dm", "diabetes_typ2", acause))

# assigning source_type == "Survey - other/unknown" because old extraction doe snot have this row, new hapin extraction does include it but I changed 
# variable name to design as source_type in old extraction included that information. In the future if old extraction is revisited; change approach in data. hapin preparation lines
data$effect_size_unit <- "linear"

data$source_type <- as.character(data$source_type)

# Now assign the character string to the column
data$source_type <- "Survey - other/unknown"
table(data$is_outlier)
table(data$sex)
data$acause %>% head()

#an idea would be to try to select all the column from the bundle

bundle_names <- c("seq", "nid", "underlying_nid", "rei", "bundle_id", "acause", "sex", "measure", "location_name", "location_id", "crosswalk_version_id", "risk_type", "source_type", #still need to see if all this variables are present and if they include all the variables needed for new MR-BRT
                  "risk_unit", "crosswalk_parent_seq","year_start", "year_end", "age_start", "age_end", "age_mean", 
                  "age_sd", "sex",  "design", "effect_size_unit", "mean", "effect_size_measure", "lower", "upper", "standard_error", "ref_risk_lower", "ref_risk_upper", "alt_risk_lower",
                  "alt_risk_upper", "exp_rr","exp_rr_lower","exp_rr_upper","ln_rr","ln_rr_lower","ln_rr_upper","ln_rr_se", "ln_rr_se_weighted",
                  "exp_rr_unit","exp_rr_unit_lower","exp_rr_unit_upper",
#                 ln_rr_unit,ln_rr_unit_lower,ln_rr_unit_upper,ln_unit_se,
                  "median_age_fup","incidence","child", "ln_rr_se_weighted", "linear_exp", "ln_exp",
                  "cov_exposure_population", "cov_confounding_uncontrolled_1", "cov_selection_bias_1", "cov_selection_bias_2",
                  "cov_duration_fup",  "cov_representativeness", "cov_exposure_quality", 
                 "cov_outcome_quality", "cov_confounder_quality", "cov_reverse_causation", "cov_selection_bias", "is_outlier", "input_type", "source_type")



# create all type only 
#data <- data[cov_outcome_VascularDementia==0 & cov_outcome_AD ==0 & cov_outcome_nonAD==0 & cov_outcome_senileDementia==0 & cov_outcome_other==0,]

write.csv(data, "/mnt/share/erf/GBD2020/air_pmhap/rr/data_hap_updated.csv")
#  ----------------------- Save final RR datasets -------------------------------------------
source("/ihme/cc_resources/libraries/current/r/upload_bundle_data.R")
source("/ihme/cc_resources/libraries/current/r/validate_input_sheet.R")
source("/ihme/cc_resources/libraries/current/r/save_bundle_version.R")
# 
bundle_ID_HAP <- 10046
# data <- fread("/mnt/share/erf/GBD2020/air_pmhap/rr/data_hap_updated.csv")

write.xlsx(data, "/mnt/share/erf/GBD2020/air_pmhap/rr/data_hap_updated.xlsx", sheetName = "extraction")

data <- read.xlsx("/mnt/share/erf/GBD2020/air_pmhap/rr/data_hap_updated.xlsx")


# fix of "\" for "/" ------------------------------------------------------
setDT(data)

data[, file_path := gsub("\\\\", "/", file_path)]
write.xlsx(data, "FILEPATH/data_hap_updated.xlsx", sheetName = "extraction")

user <- Sys.info()[7]

path_to_data <- "FILEPATH/data_hap_updated.xlsx"
path_to_error_log <- paste0('FILEPATH')
# Filepath to a clear bundle(empty bundle)


result <- validate_input_sheet(
  bundle_ID_HAP,
  path_to_data,
  path_to_error_log)


result <- upload_bundle_data(bundle_ID_HAP, filepath=path_to_data)

source("FILEPTH/save_bundle_version.R")
result<- save_bundle_version(bundle_ID_HAP)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Bundle version ID: %s', result$bundle_version_id))


