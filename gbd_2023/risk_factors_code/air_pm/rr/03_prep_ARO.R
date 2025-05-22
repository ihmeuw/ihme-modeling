
#-------------------Header------------------------------------------------
# Author: 
# Editor: 
# Purpose: Prep ARO data for MR-BeRT

#------------------SET-UP--------------------------------------------------

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

packages <- c("data.table","magrittr","openxlsx","ggplot2","pbapply","parallel")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# Directories -------------------------------------------------------------

version <- "HAPIN_1_USER"

copula.date <- "051121"
hap.exp.date <- "082124" #"121620" # which version of HAP MAPPING model?
reprep_bwga <- F # last prepped on 051121

home_dir <- "FILEPATH"

in_dir <- "FILEPATH"
out_dir <- file.path(home_dir,"FILEPATH")
dir.create(out_dir,recursive=T)

dist_dir <- file.path("FILEPATH")
graphs_dir <- file.path("FILEPATH")
dir.create(graphs_dir, recursive=T)

source(file.path(central_lib,"current/r/get_location_metadata.R"))
source(file.path(central_lib,"current/r/get_covariate_estimates.R"))
source(file.path(central_lib,"current/r/get_population.R"))
decomp <- "iterative" # set the decomp step you want to use to pull populations

# bring in locations
locations <- get_location_metadata(35,gbd_round_id = 7,decomp_step = "iterative")

# birthweight/ga distribution directory:
bwga_dir <- paste0("FILEPATH/rr/bwga/input_copula/",copula.date,"/") # I now have to prep these before running this script
# set the following interactively by reading in an example dataset. These are used later when we create aggregate distributions
test <- readRDS(paste0(bwga_dir,"/101.RDS"))
bwga.draws <- unique(test$draw)
bwga.years <- unique(test$year_id)
rm(test)

# open HAP and ARO data
data.hap <- openxlsx::read.xlsx("FILEPATH"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.hap) <- openxlsx::read.xlsx("FILEPATH"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.hap <- data.hap[!is.na(ier_source) & is_outlier==0]
data.aro.hap <- data.hap[ier_cause %in% c("bw","ga") & use_ier_cont==1]


data.aro.oap <- openxlsx::read.xlsx(paste0("FILEPATH"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.aro.oap) <- openxlsx::read.xlsx("FILEPATH"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.aro.oap <- data.aro.oap[is_outlier==0 & use_ier_cont==1]

# we are only taking those for all trimesters for now (not for individual trimesters)
data.aro.oap <- data.aro.oap[is.na(cv_trimester)|cv_trimester==0]

# read in hap exposure data
hap_exp <- fread(paste0("FILEPATH"))
hap_exp <- hap_exp[,.(location_id,year_id,median,grouping)] # we only need these columns
hap_exp <- dcast(hap_exp, location_id + year_id ~ grouping, value.var = "median") # make wide by group

# Ambient exposure data
air_pm <- get_covariate_estimates(covariate_id = 106,
                                  gbd_round_id=9, #7,
                                  release_id=16)
air_pm[,ambient_exp_mean:=mean_value]


tmrel.mean <- mean(c(2.4,5.9)) #mean of uniform distribution defining TMREL

# Prep ARO data ----------------------------------------------------------------
setnames(data.aro.oap, c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment","standard_error"),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment","exp_rr_se"))
data.aro.oap[,case_cutpoint:=as.numeric(case_cutpoint)]

#estimate mean/sd if missing
data.aro.oap[is.na(conc_mean), conc_mean := oap_conc_median]
data.aro.oap[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.aro.oap[is.na(conc_sd), conc_sd := (oap_conc_max-oap_conc_min)/4]  #SD can be estimated by range/4

# estimate mean if still missing (there is one NID with special a circumstance)
# we are assuming a normal distribution because it's a 3-year annual avg of PM2.5 concentrations
min <- data.aro.oap[nid==476138,oap_conc_min]
max <- data.aro.oap[nid==476138,oap_conc_max]
am <- (max-min)/2
asd <- (max-min)/4

data.aro.oap[nid==476138,conc_mean:=am]
data.aro.oap[nid==476138,conc_sd:=asd]

#estimate concentration p5/p95 from mean/sd using z if necessary
data.aro.oap[is.na(conc_5), conc_5 := conc_mean - conc_sd * 1.645]
data.aro.oap[is.na(conc_95), conc_95 := conc_mean + conc_sd * 1.645]

#if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.aro.oap[conc_5 < oap_conc_min, conc_5 := oap_conc_min]
data.aro.oap[conc_95 > oap_conc_max, conc_95 := oap_conc_max]

#if the mean/median ar both missing, set to midpoint of 5th and 95th
data.aro.oap[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]

# #if the 5th percentile is less than the tmrel, set it to the tmrel
# data.aro.oap[conc_5 < tmrel.mean, conc_5 := tmrel.mean]

data.aro.oap[,conc_increment:=as.numeric(conc_increment)]

# shift the RRs using p95/p5 range concentration increment
data.aro.oap[measure != "beta", exp_rr := mean ^ ((conc_95-conc_5)/conc_increment)]
data.aro.oap[measure != "beta", exp_rr_lower := lower ^ ((conc_95-conc_5)/conc_increment)]
data.aro.oap[measure != "beta", exp_rr_upper := upper ^ ((conc_95-conc_5)/conc_increment)]

# shift the betas accordingly, but they shift on the additive scale
data.aro.oap[measure == "beta", shift := mean * ((conc_95-conc_5)/conc_increment)]
data.aro.oap[measure == "beta", shift_lower := lower * ((conc_95-conc_5)/conc_increment)]
data.aro.oap[measure == "beta", shift_upper := upper * ((conc_95-conc_5)/conc_increment)]
data.aro.oap[measure == "beta", shift_se := exp_rr_se * ((conc_95-conc_5)/conc_increment)]

#also generage RRs and betas per 1 unit
# shift the RRs using p95/p5 range concentration increment
data.aro.oap[measure != "beta", exp_rr_unit := mean ^ (1/conc_increment)]
data.aro.oap[measure != "beta", exp_rr_unit_lower := lower ^ (1/conc_increment)]
data.aro.oap[measure != "beta", exp_rr_unit_upper := upper ^ (1/conc_increment)]

# shift the betas accordingly, but they shift on the additive scale
data.aro.oap[measure == "beta", shift_unit := mean * (1/conc_increment)]
data.aro.oap[measure == "beta", shift_unit_lower := lower * (1/conc_increment)]
data.aro.oap[measure == "beta", shift_unit_upper := upper * (1/conc_increment)]
data.aro.oap[measure == "beta", shift_unit_se := exp_rr_se * (1/conc_increment)]

# HAP ARO prep ------------------------------------------------------------
# Adding new extracted data (08/26/2024)
data.hapin <- openxlsx::read.xlsx("FILEPATH",
                                  sheet="extraction", colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table

# Set the column names to the values in the first row -- the other approach is rerroring so I am changing the approach
setnames(data.hapin, old = names(data.hapin), new = as.character(unlist(data.hapin[1,])))
data.hapin <- data.hapin[-(1:4),]


data.hapin <- data.hapin[!is.na(nid)]
data.hapin <- data.hapin %>% dplyr::rename(ier_source = acause)

data.hapin.aro <- data.hapin %>% filter(ier_source == "Low birth weight and short gestation outcomes")

data.hapin.aro[ , ier_source := ifelse(outcome_def == "Birth weight", "bw", "ga")]
# changes to the data.hapin -----------------------------------------------

names.hapin <- names(data.hapin)

int_names <- grep("^int_", names.hapin, value = TRUE)
cohort_names <- grep("^cohort_", names.hapin, value = TRUE)
cc_names <- grep("^cc_", names.hapin, value = TRUE)

other_names <- grep("^(int_|cohort_|cc_)", names.hapin, value = TRUE, invert = TRUE)

colnames_RCT <- c(other_names, int_names)
colnames_cohort <- c(other_names, cohort_names)
colnames_cc <- c(other_names, cc_names)

# changes to RCT: select all the columns with "int_" subscript
RCT <- data.hapin.aro[design == "Randomized controlled trial", ]
RCT <- RCT %>% select(all_of(colnames_RCT))
RCT <- RCT %>% filter(measure == "relrisk") %>%
  select(!c(source_type, measure)) %>%
  rename(source_type = design, 
         measure = effect_size_measure,
         mean = effect_size,
         # person_years = cohort_person_years_total, 
         hap_exposed_def = exp_def) %>%
    mutate(case_cutpoint = case_when(ier_source == "bw" ~2500,
                                     TRUE ~ 37))

#cases: In the new extraction they are separated by intervention and comparison; I am not sure this is needed so I am going to add the number of cases
# between intervention and comparison group

RCT <- RCT %>% mutate(cases = as.numeric(int_number_events_int) + as.numeric(int_number_events_comp)) %>%
  select(!c(int_number_events_int, int_number_events_comp))

#sample size: there is no baseline sample size for locations; so I am summing 

RCT <- RCT %>% mutate(sample_size = as.numeric(int_sample_size_int_group_baseline) + as.numeric(int_sample_size_comparison_group_baseline)) %>%
  select(!c((grep("sample_size_", names(RCT), value = TRUE))))

RCT <- RCT %>% rename( duration_fup_measure = exp_fup_num) %>% 
  select(!c( grep("^int_fup_", names(RCT), value = TRUE)))

# all columns with a measure of exposure 
RCT[, .SD, .SDcols = 142:157]

#for future extraction if more measures are extracted more mapping needs to be done e.g:int_baseline_exp_comp_sd_PM2.5 = oap_conc_sd
RCT[,.SD, .SDcols = grep("exp_", names(RCT))]

# only reporting median that is later assigned to mean, but if mean is extracted this needs to change too

RCT <- RCT %>% rename(oap_conc_mean = int_baseline_full_group_median,
                      oap_conc_mean_int = int_baseline_exp_int_median_PM2.5,
                      oap_conc_mean_comp = int_baseline_exp_comp_median_PM2.5,
                      oap_conc_iqr = int_baseline_full_group_IQR)

RCT <- RCT %>% select(!c(grep("exp_", names(RCT), value = TRUE)))
RCT <- RCT %>% select(!c(grep("^int_", names(RCT), value = TRUE)))

nas_data_hapin <- c("shortened_citation", "year_issue", "age_demographer", "age_issue", "sex_issue", "percent_male", "sampling_type", "rep_geography", "rep_selection_criteria", 
                    "rep_prevalent_disease", "notes_outcome_def", "notes_exposed_group_def", "notes_unexposed_reference_group_def", "outcome_def_flag", "outcome_assess_1", 
                    "outcome_assess_2", "outcome_assess_3", "Shortened_effect_size_description", "nonCI_uncertainty_value", "nonCI_uncertainty_type", "effect_size_multi_location", 
                    "pooled_cohort", "dose_response", "dose_response_detail", "confounders_age", "confounders_sex",
                    "confounders_smoking", "confounders_alcohol_use", "confounders_physical_activity", "confounders_dietary_components", "confounders_bmi",
                    "confounders_hypertension", "confounders_diabetes", "confounders_hypercholesterolemia", "confounders_other", "keep", "specificity", 
                    "group_review", "duplicate_with_other_study", "duplicate_with_other_study_explained", "notes_selection_bias_follow_up", "input_type")

data.hapin_RCT <- RCT %>%
  mutate(rei = ifelse(rei == "Household air pollution from solid fuels", "air_hap", "air_pm"))

data.hapin_RCT <- data.hapin_RCT %>% rename(page_num = page_num_effect_size, 
                                                                      table_num = table_num_effect_size,
                                                                      uncertainty_type_value = CI_uncertainty_type_value,
                                                                      education =confounders_education,
                                                                      income = confounders_income,
                                                                      note_SR = note_sr)

data.hapin_RCT$new_gbd2023 <-1 
data.hapin_RCT <- data.hapin_RCT %>% select(!c(nas_data_hapin))

data.hapin_RCT[, .SD, .SDcols = 26:41]
data.aro.hap[, .SD, .SDcols = 20:23]
write.csv(names(data.hapin_RCT), "~/data.hapin_RCT_names.csv")
write.csv(names(data.aro.hap), "~/data.hap_RCT_names.csv")

data.aro.hap <- data.aro.hap %>% mutate(rei = "air_hap") %>% select(!c(ier_source)) 

# changing cv to cov for new MR-BRT 
setDT(data.aro.hap) # Ensure 'data' is a data.table

# Find variable names starting with "cv_"
cv_names <- grep("^cv_", names(data.aro.hap), value = TRUE)

# Create new variable names by substituting "cv_" with "cov_"
cov_names <- sub("^cv_", "cov_", cv_names)

# Rename the variables
setnames(data.aro.hap, old = cv_names, new = cov_names)

data.hapin_RCT <- data.hapin_RCT %>% rename(ier_cause = ier_source)
data.hapin_RCT <- data.hapin_RCT %>% rename(outcome_type = ier_cause_measure)
data.hapin_RCT <- data.hapin_RCT %>%
  mutate(measure = "RR",
         uncertainty_type_value = as.numeric(uncertainty_type_value))

data.hapin_RCT <- data.hapin_RCT %>% select(!c(notes_exposure_def, outcome_def))


data.aro.hap <- data.aro.hap %>% select(!c(new_gbd2019))
data.hapin_RCT <- data.hapin_RCT %>% rename(ier_cause_measure = outcome_type )%>%
  mutate(ier_cause_measure = case_when(ier_cause_measure == "Incidence & Mortality" ~ "incidence & mortality",
                                       TRUE ~ "mortality"))

data.hapin_RCT <- data.hapin_RCT %>% select(!c("group " ,"custom_*", "effect_size_coefficient"))

data.hapin_RCT <- data.hapin_RCT %>% rename(cov_counfounding.uncontroled = cov_confounding_uncontrolled)
setdiff( names(data.hapin_RCT), names(data.aro.hap))
setdiff(  names(data.aro.hap), names(data.hapin_RCT))
data.hap_merged <- rbind(data.aro.hap, data.hapin_RCT, fill = TRUE)

# -------------------------------------------------------------------------

setnames(data.hap_merged , c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment", 
                         "standard_error", "oap_conc_mean_int", "oap_conc_mean_comp"),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment", "exp_rr_se", "conc_mean_int", "conc_mean_comp"))
data.hap_merged[,case_cutpoint:=as.numeric(case_cutpoint)]

# find midpoint of study
data.hap_merged$year_start <- as.numeric(data.hap_merged$year_start)
data.hap_merged$year_end <- as.numeric(data.hap_merged$year_end)
data.hap_merged[,year_id:=round((year_start+year_end)/2)]

# some HAP studies already have concentration
#estimate mean/sd if missing
data.hap_merged$oap_conc_iqr <- as.numeric(data.hap_merged$oap_conc_iqr)
data.hap_merged$conc_mean <- as.numeric(data.hap_merged$conc_mean)

data.hap_merged[is.na(conc_mean), conc_mean := oap_conc_median]
data.hap_merged[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.hap_merged[is.na(conc_sd), conc_sd := (oap_conc_max-oap_conc_min)/4]  #SD can be estimated by range/4

#estimate concentration p5/p95 from mean/sd using z if necessary

data.hap_merged[is.na(conc_5), conc_5 := conc_mean - conc_sd * 1.645]
data.hap_merged[is.na(conc_95), conc_95 := conc_mean + conc_sd * 1.645]

#if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.hap_merged[conc_5 < oap_conc_min, conc_5 := oap_conc_min]
data.hap_merged[conc_95 > oap_conc_max, conc_95 := oap_conc_max]

# #if the 5th percentile is less than the tmrel, set it to the tmrel
# data.hap_merged[conc_5 < tmrel.mean, conc_5 := tmrel.mean]

data.hap_merged[,conc_increment:=as.numeric(conc_increment)]
data.hap_merged[,conc_95:=as.numeric(conc_95)]
data.hap_merged[,conc_5:=as.numeric(conc_5)]
data.hap_merged[,mean:=as.numeric(mean)]
data.hap_merged[,lower:=as.numeric(lower)]
data.hap_merged[,upper:=as.numeric(upper)]

#commenting everything out because there is no measure == beta
# shift the RRs using p95/p5 range concentration increment
data.hap_merged[!is.na(conc_increment) & measure != "beta", exp_rr := mean ^ ((conc_95-conc_5)/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure != "beta", exp_rr_lower := lower ^ ((conc_95-conc_5)/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure != "beta", exp_rr_upper := upper ^ ((conc_95-conc_5)/conc_increment)]

# shift the betas accordingly, but they shift on the additive scale
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift := mean * ((conc_95-conc_5)/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift_lower := lower * ((conc_95-conc_5)/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift_upper := upper * ((conc_95-conc_5)/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift_se := exp_rr_se * ((conc_95-conc_5)/conc_increment)]

# # for categorical hap exposures, no need to shift betas
data.hap_merged[is.na(conc_increment) & measure == "beta", shift := mean ]
data.hap_merged[is.na(conc_increment) & measure == "beta", shift_lower := lower ]
data.hap_merged[is.na(conc_increment) & measure == "beta", shift_upper := upper ]
data.hap_merged[is.na(conc_increment) & measure == "beta", shift_se := exp_rr_se ]
data.hap_merged[is.na(conc_increment) & measure != "beta", exp_rr := mean]
data.hap_merged[is.na(conc_increment) & measure != "beta", exp_rr_lower := lower]
data.hap_merged[is.na(conc_increment) & measure != "beta", exp_rr_upper := upper]

# merge on ambient and hap exposures to calculate 5th and 95th percentiles
data.hap_merged$location_id <- as.numeric(data.hap_merged$location_id)
data.hap_merged <- merge(data.hap_merged,air_pm[,.(location_id,year_id,ambient_exp_mean)], by=c("year_id","location_id"), all.x=T)
data.hap_merged <- merge(data.hap_merged,hap_exp,by= c("year_id","location_id"),all.x=T)
data.hap_merged[, household_exp_mean:=female] # because the babies were exposed through their moms

data.hap_merged[,conc_5:=as.numeric(conc_5)]
data.hap_merged[,conc_95:=as.numeric(conc_95)]
data.hap_merged[is.na(conc_95),conc_95:=ambient_exp_mean+household_exp_mean]
data.hap_merged[is.na(conc_5),conc_5:=ambient_exp_mean]

# also shift the RRs and Betas such that they are for one unit
# shift the RRs using p95/p5 range concentration increment
data.hap_merged[!is.na(conc_increment) & measure != "beta", exp_rr_unit := mean ^ (1/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure != "beta", exp_rr_unit_lower := lower ^ (1/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure != "beta", exp_rr_unit_upper := upper ^ (1/conc_increment)]

# shift the betas accordingly, but they shift on the additive scale
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift_unit := mean * (1/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift_unit_lower := lower * (1/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift_unit_upper := upper * (1/conc_increment)]
data.hap_merged[!is.na(conc_increment) & measure == "beta", shift_unit_se := exp_rr_se * (1/conc_increment)]

# for categorical hap exposures, shift by range of exposed to unexposed
data.hap_merged[is.na(conc_increment) & measure == "beta", shift_unit := mean * (1/(conc_95-conc_5)) ]
data.hap_merged[is.na(conc_increment) & measure == "beta", shift_unit_lower := lower * (1/(conc_95-conc_5)) ]
data.hap_merged[is.na(conc_increment) & measure == "beta", shift_unit_upper := upper * (1/(conc_95-conc_5)) ]
data.hap_merged[is.na(conc_increment) & measure == "beta", shift_unit_se := exp_rr_se * (1/(conc_95-conc_5)) ]
data.hap_merged[is.na(conc_increment) & measure != "beta", exp_rr_unit := mean ^ (1/(conc_95-conc_5)) ]
data.hap_merged[is.na(conc_increment) & measure != "beta", exp_rr_unit_lower := lower ^ (1/(conc_95-conc_5)) ]
data.hap_merged[is.na(conc_increment) & measure != "beta", exp_rr_unit_upper := upper ^ (1/(conc_95-conc_5)) ]


# if the mean is missing set the mean/median to be the midpoint
data.hap_merged[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]

#  ----------------------- Combine datasets  ------------------------------------------
#adapting data.oap
# changing cv to cov for new MR-BRT 
setDT(data.aro.oap) # Ensure 'data' is a data.table

# Find variable names starting with "cv_"
cv_names <- grep("^cv_", names(data.aro.oap), value = TRUE)

# Create new variable names by substituting "cv_" with "cov_"
cov_names <- sub("^cv_", "cov_", cv_names)

# Rename the variables
setnames(data.aro.oap, old = cv_names, new = cov_names)

data.aro.oap[,year_id:=round((year_start+year_end)/2)]

setdiff(names(data.aro.oap), names(data.hap_merged))
setdiff(names(data.hap_merged), names(data.aro.oap))

data.aro.oap <- data.aro.oap %>% mutate(rei = "air_pm") #%>% select(!c(ier_source))
data.aro.oap <- data.aro.oap %>% select(!c(new_gbd2020))


data.aro <- rbind(data.aro.oap,data.hap_merged,fill=T,use.names=T)


# -------------------------------------------------------------------------


# For each location where we have a categorical study, translate effect sizes to linear shifts
data.aro[measure!="beta",id:=seq(1:.N),by=location_id]
data.aro[, bw_min := as.numeric(bw_min)]
data.aro[, bw_max := as.numeric(bw_max)]
data.aro[, ga_min := as.numeric(ga_min)]
data.aro[, ga_max := as.numeric(ga_max)]
data.aro[exp_rr !=0,]
# Converts ORs to linear shifts
data.aro.locs <- data.aro[measure!="beta" & is.na(shift),unique(location_id)]
for(loc in data.aro.locs){
  # browser()
  print(paste0("Working on ",loc))
  
  if(file.exists(paste0(dist_dir,loc,"copula.RDS")) & reprep_bwga==F){
    loc_bwga <- readRDS(paste0(dist_dir,loc,"copula.RDS"))
  }else if(loc == "97,4750,4751,4752,4753,4754,4755,4756,4757,4758,4759,4760,4761,4762,4763,4764,4765,4766,4767,4768,4769,4770,4771,4772,4773,4774,4775,4776,354,361,491,492,493,494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518,519,520,521,171,109,139,122,43872,43873,43874,43875,43877,43880,43881,43882,43883,43884,43885,43886,43887,43888,43890,43891,43892,43893,43894,43895,43896,43898,43899,43900,43901,43902,43903,43904,43905,43906,43908,43909,43910,43911,43913,43916,43917,43918,43919,43920,43921,43922,43923,43924,43926,43927,43928,43929,43930,43931,43932,43934,43935,43936,43937,43938,43939,43940,43941,43942,44539,44540,35424,35425,35426,35427,35428,35429,35430,35431,35432,35433,35434,35435,35436,35437,35438,35439,35440,35441,35442,35443,35444,35445,35446,35447,35448,35449,35450,35451,35452,35453,35454,35455,35456,35457,35458,35459,35460,35461,35462,35463,35464,35465,35466,35467,35468,35469,35470,35617,35618,35619,35620,35621,35622,35623,35624,35625,35626,35627,35628,35629,35630,35631,35632,35633,35634,35635,35636,35637,35638,35639,35640,35641,35642,35643,35644,35645,35646,35647,35648,35649,35650,35651,35652,35653,35654,35655,35656,35657,35658,35659,35660,35661,35662,35663,10,17,4643,4644,4645,4646,4647,4648,4649,4650,4651,4652,4653,4654,4655,4656,4657,4658,4659,4660,4661,4662,4663,4664,4665,4666,4667,4668,4669,4670,4671,4672,4673,4674,213,25318,25319,25320,25321,25322,25323,25324,25325,25326,25327,25328,25329,25330,25331,25332,25333,25334,25335,25336,25337,25338,25339,25340,25341,25342,25343,25344,25345,25346,25347,25348,25349,25350,25351,25352,25353,25354,131,164,123,53533,53534,53535,53536,53537,53538,53539,53540,53541,53542,53543,53544,53545,53546,53547,53548,53549,53550,53551,53552,53553,53554,53555,53556,53557,53558,53559,53560,53561,53562,53563,53564,53565,53566,53567,53568,53569,53570,53571,53572,53573,53574,53575,53576,53577,53578,53579,53580,53581,53582,53583,53584,53585,53586,53587,53588,53589,53590,53591,53592,53593,53594,53595,53596,53597,53598,53599,53600,53601,53602,53603,53604,53605,53606,53607,53608,53609,53610,53611,53612,53613,53614,18,20"
           & reprep_bwga==F){ #i had to do this because the study with a ton of countries wasnt saving due to its name being too long
    loc_bwga <- readRDS(paste0(dist_dir,1,"copula.RDS"))
 }else{
    if(file.exists(paste0(bwga_dir,loc,".RDS"))){
      
      loc_bwga <- readRDS(paste0(bwga_dir,loc,".RDS"))
      
    }else{ # read in child locations and population weight
     
      if(grepl(",",loc)){
        loc_list <- unlist(strsplit(loc, split=","))
      }else{
        loc_list <- locations[(grepl(paste0(",",loc,","),path_to_top_parent) | grepl(paste0(",",loc),path_to_top_parent)) & most_detailed==1,location_id]
      }
      
      pop <- get_population(age_group_id=2,
                            location_id=loc_list,
                            year_id=bwga.years,
                            gbd_round_id=7,
                            decomp_step=decomp)
      
      # expand the population dt so that there is one row for each draw
      pop <- merge(pop[,.(age_group_id,location_id,year_id,population)],data.table(age_group_id=2,draw=bwga.draws),by="age_group_id",allow.cartesian=T)
      # sample the draws for each year with the probably for each draw being sampled given by the population for that location
      set.seed(100)
      sample <- pop[,.SD[sample(.N, length(bwga.draws), prob=population)],by = "year_id"]
      
      read_and_sample <- function(x) {
        loc_bwga <- readRDS(paste0(bwga_dir,"/",x,".RDS"))
        loc_bwga <- merge(loc_bwga,sample,by=c("year_id","age_group_id","location_id","draw"),all.x=F,all.y=F)
        return(loc_bwga)
        print(paste0("Done sampling ",x)) # this function can take a long time, so this is to make sure it's still running
      }
      
      # loc_bwga <- mclapply(unique(sample$location_id), read_and_sample, mc.cores=4) %>% rbindlist()
      loc_bwga <- pblapply(unique(sample$location_id), read_and_sample, cl=8) %>% rbindlist()
      
    }
    # write csvs so that this process doesn't have to be repeated every time
    if(nchar(loc)<100){
      saveRDS(loc_bwga,paste0(dist_dir,loc,"copula.RDS"))
    }else{
      saveRDS(loc_bwga,paste0(dist_dir,1,"copula.RDS")) # this is only for the one study w/ the really long loc_id
    }
  }
  
  for(i in 1:data.aro[location_id==loc & measure!="beta",.N]){
    # browser()
    row <- data.aro[location_id==loc & measure!="beta" & id==i]
    
    # Study-specific characteristics 
    # We use these study-specific restrictions to restrict the bw/ga distribution 
    # We are assuming 50/50 split of M/F
    nearest_year <- unique(loc_bwga$year_id)[which.min(abs(mean(c(row$year_start,row$year_end))-unique(loc_bwga$year_id)))]
    
    bw_min <- as.numeric(ifelse(is.na(row$bw_min),0,row$bw_min))
    bw_max <- as.numeric(ifelse(is.na(row$bw_max),1e6,row$bw_max)) # max or average in the dataset (?), basically don't want it to be too big
    ga_min <- as.numeric(ifelse(is.na(row$ga_min),0,row$ga_min))
    ga_max <- as.numeric(ifelse(is.na(row$ga_max),1e6,
                                ifelse(row$ga_max==round(row$ga_max),row$ga_max+1,row$ga_max))) # this one is special because of the way we define birthweight. 44 weeks includes any baby born between 44 and 45 weeks. For studies reported in integers/days, I want to keep their cutoff, but for those reported in whole weeks, I need to round up to the next highest week.
    
    # here's what we're doing:
    # 1) take the proportion of the bwga distribution with lbw or sga 
    # 2) divide it by the RR from the study to get the counterfactual proportion (if there were no air pollution)
    # 3) get the quantile of the bwga distribution for ga or bw at that counterfactual proportion
    # 4) subtract the case cutpoint (definition of lbw or sga by the study) from the counterfactual bwga value
    # 5) this is the "shift": how much we have to shift the bwga distribution in order to get to the counterfactual level
    # if you want to see this on a plot, see the ambient writeup figures Kate made for GBD2019!
    if (row$ier_cause=="ga" & row$exp_rr ==0) {
      data.aro[location_id==loc & measure!="beta" & id==i, shift:=0]
      data.aro[location_id==loc & measure!="beta" & id==i, shift_upper:=0]
      data.aro[location_id==loc & measure!="beta" & id==i, shift_lower :=0]
    }else if (row$ier_cause=="ga"){
      bwga <- loc_bwga[year_id==nearest_year & bw >= bw_min & bw <= bw_max]
      data.aro[location_id==loc & measure!="beta" & id==i, shift:=quantile(bwga$ga,(bwga[ga<row$case_cutpoint,.N]/nrow(bwga))/row$exp_rr)-as.numeric(row$case_cutpoint)]
      data.aro[location_id==loc & measure!="beta" & id==i, shift_upper:=quantile(bwga$ga,(bwga[ga<row$case_cutpoint,.N]/nrow(bwga))/row$exp_rr_lower)-as.numeric(row$case_cutpoint)]
      data.aro[location_id==loc & measure!="beta" & id==i, shift_lower:=quantile(bwga$ga,(bwga[ga<row$case_cutpoint,.N]/nrow(bwga))/row$exp_rr_upper)-as.numeric(row$case_cutpoint)]
    } else if (row$ier_cause=="bw") {
      bwga <- loc_bwga[year_id==nearest_year & ga >= ga_min & ga <= ga_max]
      data.aro[location_id==loc & measure!="beta" & id==i, shift:=quantile(bwga$bw,(bwga[bw<row$case_cutpoint,.N]/nrow(bwga))/row$exp_rr)-as.numeric(row$case_cutpoint)]
      data.aro[location_id==loc & measure!="beta" & id==i, shift_upper:=quantile(bwga$bw,(bwga[bw<row$case_cutpoint,.N]/nrow(bwga))/row$exp_rr_lower)-as.numeric(row$case_cutpoint)]
      data.aro[location_id==loc & measure!="beta" & id==i, shift_lower:=quantile(bwga$bw,(bwga[bw<row$case_cutpoint,.N]/nrow(bwga))/row$exp_rr_upper)-as.numeric(row$case_cutpoint)]
    }
    
    print(paste("finished", i ,"of", data.aro[location_id==loc & measure!="beta",.N], "for location_id,",loc))
  }
}

# for categorical hap exposures, shift by range of exposed to unexposed
# data.aro[measure != "beta", shift_unit := shift * (1/(conc_95-conc_5)) ]
# data.aro[measure != "beta", shift_unit_lower := shift_lower * (1/(conc_95-conc_5)) ]
# data.aro[measure != "beta", shift_unit_upper := shift_upper * (1/(conc_95-conc_5)) ]

# generate ln_rr and ln_rr_se
data.aro<-data.aro[shift != 0,]

write.csv(data.aro,paste0(out_dir,"/dataset_aro_check.csv"),row.names=F)
data.aro <- fread(paste0(out_dir,"/dataset_aro_check.csv"))

data.aro[,order:=.N,by=c("ier_cause","study")]

pdf(file.path(graphs_dir,"ARO_plots.pdf"), width=22, height=17)

ggplot(data.aro[ier_cause=="bw" & order>1],aes(x=paste(ier_cause,measure,case_cutpoint),y=shift))+geom_point()+facet_wrap(~study + ier_cause,scales="free")+geom_errorbar(aes(ymin=shift_lower,ymax=shift_upper))+geom_hline(yintercept=0)

ggplot(data.aro[ier_cause=="ga" & order>1],aes(x=paste(ier_cause,measure,case_cutpoint),y=shift))+geom_point()+facet_wrap(~study + ier_cause,scales="free")+geom_errorbar(aes(ymin=shift_lower,ymax=shift_upper))+geom_hline(yintercept=0)

dev.off()

if(version==35){
  data.aro <- data.aro[measure!="beta" & case_cutpoint %in% c(2500,37)]
}

#generate log_rr and log_se
data.aro[,log_rr:=log(exp_rr)]
data.aro[,log_rr_lower:=log(exp_rr_lower)]
data.aro[,log_rr_upper:=log(exp_rr_upper)]
data.aro[,log_se:=(log_rr_upper-log_rr_lower)/3.92]

#generate log_rr and log_se
data.aro[,log_rr_unit:=log(exp_rr_unit)]
data.aro[,log_rr_unit_lower:=log(exp_rr_unit_lower)]
data.aro[,log_rr_unit_upper:=log(exp_rr_unit_upper)]
data.aro[,log_unit_se:=(log_rr_unit_upper-log_rr_unit_lower)/3.92]

#generate shift_se
data.aro[is.na(shift_se),shift_se:=(shift_upper-shift_lower)/3.92]

# generate shift_se
data.aro[is.na(shift_unit_se),shift_unit_se:=(shift_unit_upper-shift_unit_lower)/3.92]


# rename variables
# setnames(data.aro,c("conc_den","conc"), c("conc_5","conc_95"))
setnames(data.aro,c("conc_5","conc_95"),c("conc_den","conc"))

data.aro[is.na(conc_mean_int),ref_risk_lower:=conc_5][is.na(conc_mean_comp),ref_risk_upper:=ref_risk_lower+0.00001]#liz note check this 
data.aro[is.na(conc_mean_int),alt_risk_lower:=conc_95][is.na(conc_mean_comp),alt_risk_upper:=alt_risk_lower+0.00001] 
# -------------------------------- Format covariates -------------------------------------------------------
# format covariates for compatibility with new MR-BRT

# we can remove these: cv_subpopulation, cv_outcome_selfreport, cv_reverse_causation
data.aro[,cov_subpopulation:=NULL]
data.aro[,cov_outcome_selfreport:=NULL]
data.aro[,cov_reverse_causation:=NULL]

# create binary indicators for cv_confounding_uncontrolled
data.aro[,cov_confounding_uncontrolled_1:=ifelse(cov_counfounding.uncontroled==1,1,0)]
data.aro[,cov_confounding_uncontrolled_2:=ifelse(cov_counfounding.uncontroled==2,1,0)]
data.aro[,cov_counfounding.uncontroled:=NULL]

# create binary indicators for cv_selection_bias
data.aro[is.na(cov_selection_bias),cov_selection_bias:=2] # code missing values to the worst category (2)
data.aro[,cov_selection_bias_1:=ifelse(cov_selection_bias==1,1,0)]
data.aro[,cov_selection_bias_2:=ifelse(cov_selection_bias==2,1,0)]
data.aro[,cov_selection_bias:=NULL]

cov_values <- grep("^cov_", names(data.aro), value = TRUE)
length(data.aro)


length(data)

cov_names <- names(data)[grep("cov",names(data))]
# -------------------------------- Save output files -------------------------------------------------------

# new GBD 2020: for multiple datapoints from the same study, we want to weight their SEs by sqrt(n)
weight_ss <- function(n){
  
  # make a temporary dataset for each nid
  temp <- data.aro[nid==n]
  
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
        temp[ier_cause==c & sample_size==s,shift_se_weighted:=(shift_se*sqrt(n_observations))]
      }
    }
    
  } else {
    
    # if there is only 1 observation for that nid, we can skip the whole weighting process
    temp[,shift_se_weighted:=shift_se]
  }
  
  # put back the NA for sample size
  temp[sample_size==1,sample_size:=NA]
  
  return(temp)
}

temp <- pblapply(unique(data.aro$nid),weight_ss,cl=4) %>% rbindlist

# plot to check weighted vs original SEs
plot <- ggplot(temp, aes(x = shift_se_weighted, y = shift_se, color = nid)) +
  geom_point()+
  geom_abline(slope = 1, intercept = 0)+
  facet_wrap(~ier_cause)
print(plot)

data <- temp



# -------------------------------------------------------------------------


# PREPARE DATA FOR PIPELINE -----------------------------------------------

data[,risk_type:="continuous"]
data[,seq:= NA] #seq(from, to, by,length. out)


#this needs to change to the correct bundle ids-- there should be a different bundle ID for each RO pair 
data[,bundle_id := 10046][,crosswalk_version_id:=NA]

# risk unit
data$risk_unit <- "ug/m3" # unit of risk == ug/m3 ? 
data[,design:=source_type]
data[ , design := case_when(design %in% c("Case control", "Cohort", "Case-control", "Nested matched case-control")  ~ "case-control",
                           design == "Retrospective Cohort" ~ "retrospective cohort",
                           design == "Case-cohort" ~ "case-cohort",
                           design == "Randomized controlled trial" ~ "randomized control trial", 
                           design == "Prospective cohort" ~ "prospective cohort",
                           TRUE ~ design)]
# data[,is_outlier:=ifelse(new_gbd2023 ==1, 1, 0)]# need to check that facility -outpatient is correct

# crosswalk_parent_seq

data[,crosswalk_parent_seq:=NA] # is NA the same as null?


data[,input_type:="extracted"]


data[is.na(age_end), age_end:=99]
data[is.na(age_start), age_start:=0]

data[,measure_archive:=measure] 


data[, ln_rr:=shift]
warning("using weighted rr se")
data[, ln_rr_se:=shift_se_weighted]


data <- data %>% mutate(effect_size_measure = "relative risk") #%>%


data <- data %>%
  mutate(rei = "air_pmhap",
         underlying_nid = as.integer(underlying_nid),
         acause = ier_cause, 
         is_outlier = 0,
         age_mean = ifelse(age_mean == 0 , NA, age_mean),
         design = ifelse(design == 'randomized controlled trial', 'randomized control trial', design)) 

data$effect_size_unit <- "linear"

# Now assign the character string to the column
data$source_type <- "Survey - other/unknown"


table(data$is_outlier)
table(data$sex)
data$acause %>% head()
data[,measure:="relrisk"] 

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

write.csv(data, "/mnt/share/erf/GBD2020/air_pmhap/rr/data_hap_updated_ARO.csv")



# saving causes separetely  -----------------------------------------------

for (cause in unique(data$ier_cause)){
  # browser()
  out_cause<- data[ier_cause==cause,.(seq, rei, ier_cause, bundle_id, crosswalk_version_id, risk_type, #still need to see if all this variables are present and if they include all the variables needed for new MR-BRT
                                      risk_unit, crosswalk_parent_seq,
                                      underlying_nid, nid, design, source_type, is_outlier,
                                      location_name,location_id,
                                      study, year_start, year_end, measure,
                                      mean, lower, upper, hap_exposed_def,
                                      age_start, age_end, sex, 
                                      ref_risk_lower, ref_risk_upper, alt_risk_lower,
                                      alt_risk_upper,
                                      exp_rr,exp_rr_lower,exp_rr_upper,ln_rr,ln_rr_se,
                                      exp_rr_unit,exp_rr_unit_lower,exp_rr_unit_upper,
                                      # ln_rr_unit,ln_rr_unit_lower,ln_rr_unit_upper,ln_unit_se, standard_error, ln_rr_lower,ln_rr_upper,ln_rr_se_weighted, median_age_fup,incidence,child,
                                      # linear_exp, ln_exp,
                                      cov_exposure_population,cov_confounding_uncontrolled_1,
                                      cov_selection_bias_1,cov_selection_bias_2,
                                      # cov_duration_fup,
                                      input_type)]
  print(cause)
  write.xlsx(out_cause,paste0("/mnt/share/erf/GBD2020/air_pmhap/rr/air_pm-",cause,".xlsx"),rowNames=F, sheetName = "extraction")
}



