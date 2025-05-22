##########################################################################
### Purpose: Data processing for hepatitis B seroprevalence model (counterfactual)  
##########################################################################

rm(list=ls())


# SOURCE FUNCTIONS AND LIBRARIES --------------------------------------------------
invisible(sapply(list.files(FILEPATH, full.names = T), source))
pacman::p_load(data.table, ggplot2, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, readxl, boot, gtools, msm, Hmisc, metafor)
source("FILEPATH/cw_mrbrt_helper_functions.R")

# SOURCE CROSSWALK PACKAGE (Latest version) --------------------------------------------------------
library(reticulate)
reticulate::use_python("/FILEPATH/python")
mr <- import("mrtool")
cw <- import("crosswalk")


# SET OBJECTS -------------------------------------------------------------
b_id <- OBJECT
a_cause <- "hepatitis_b"
date <- gsub("-", "_", Sys.Date())
release_id = OBJECT
draws <- paste0("draw_", 0:999)
mrbrt_crosswalk_dir <- FILEPATH
mrbrt_sexratio_dir <- FILEPATH

if (!dir.exists(mrbrt_crosswalk_dir)) dir.create(mrbrt_crosswalk_dir)
if (!dir.exists(mrbrt_sexratio_dir)) dir.create(mrbrt_sexratio_dir)

outlier_status <- c(0, 1) # this should only be c(0,1) for hepatitis b
sex_split <- T
sex_covs <- "" 
id_vars <- "study_id"
sex_remove_x_intercept <- T
keep_x_intercept <- T
logit_transform <- T
reference <- ""
nash_cryptogenic <- F
reference_def <- "reference"
trim <- 0.1
measures <- "prevalence"
sex_ratio_model_name <- paste0("sex_split_", date)
if (!dir.exists(paste0(mrbrt_sexratio_dir, model_name))) dir.create(paste0(mrbrt_sexratio_dir, sex_ratio_model_name))
# modeling <- T

if(logit_transform == T) {
  response <- "ldiff"
  data_se <- "ldiff_se"
  mrbrt_response <- "diff_logit"
} else {
  response <- "ratio_log"
  data_se <- "ratio_se_log"
  mrbrt_response <- "diff_log"
}


if(sex_split == T) { 
  ## Sex splitting only done in log space
  sex_split_response <- "log_ratio"
  sex_split_data_se <- "log_se"
  sex_mrbrt_response <- "diff_log"
}

# GET METADATA ------------------------------------------------------
loc_dt <- get_location_metadata(35, release_id=release_id)
loc_dt1 <- loc_dt[, .(location_id, super_region_name)]


################################################################################
#run hepb_adjustment_subtraction.R before going on with the rest of the data processing--------------------------
################################################################################


################################################################################
## STEP 2: save a new bundle version
################################################################################

print(sprintf('Bundle version ID: %s', bundle_version$bundle_version_id))
bundle_version_id_new <- OBJECT 

################################################################################
## STEP 3: get all data (new and pre-existing) from the new bundle version
################################################################################
orig_dt <- get_bundle_version(bundle_version_id = bundle_version_id_new, fetch = "all", export = FALSE)

bundle_version_data <- copy(orig_dt)

table(orig_dt$measure)
orig_dt_csmr <- orig_dt[measure %in% c("mtexcess", "mtspecific")]
orig_dt <- orig_dt[measure %in% c("prevalence", "incidence")]


################################################################################
## STEP 4: Clean covariates
################################################################################
#clean cvs
# --- Check what covariates are in the dataset
cvs <- names(orig_dt)[grepl("^cv_", names(orig_dt))]
cvs

# --- Clean up covariates
#blood donors
table(orig_dt$cv_blood_donors)
table(orig_dt$cv_blood_donor)
table(orig_dt$cv_blood_donors, orig_dt$cv_blood_donor)

orig_dt <- orig_dt[is.na(cv_blood_donor), cv_blood_donor :=cv_blood_donors]
orig_dt <- orig_dt[is.na(cv_blood_donor), cv_blood_donor :=0]
table(orig_dt$cv_blood_donor)
orig_dt$cv_blood_donors <- NULL

#healthcare workers
table(orig_dt$cv_healthcare_workers)
table(orig_dt$cv_healthcare_worker)
table(orig_dt$cv_healthcare_workers, orig_dt$cv_healthcare_worker)

orig_dt <- orig_dt[is.na(cv_healthcare_worker), cv_healthcare_worker :=cv_healthcare_workers]
orig_dt <- orig_dt[is.na(cv_healthcare_worker), cv_healthcare_worker :=0]
table(orig_dt$cv_healthcare_worker)
orig_dt$cv_healthcare_workers <- NULL

#hospital referral
table(orig_dt$cv_hospital_referrals)
table(orig_dt$cv_hospital_referral)
table(orig_dt$cv_hospital_referrals, orig_dt$cv_hospital_referral)

orig_dt <- orig_dt[is.na(cv_hospital_referral), cv_hospital_referral :=cv_hospital_referrals]
orig_dt <- orig_dt[is.na(cv_hospital_referral), cv_hospital_referral :=0]
table(orig_dt$cv_hospital_referral)
orig_dt$cv_hospital_referrals <- NULL


# --- Check new list of covariates again after cleaning
cvs <- names(orig_dt)[grepl("^cv_", names(orig_dt))]
cvs
table(orig_dt$cv_attending_unrelated_clinic)
table(orig_dt$cv_schoolchildren)
table(orig_dt$cv_pregnant)
table(orig_dt$cv_specific_occupation)
table(orig_dt$cv_institutionalized)
table(orig_dt$cv_vaccination)
table(orig_dt$cv_double_anti)
table(orig_dt$cv_antibody)
orig_dt <- orig_dt[is.na(cv_attending_unrelated_clinic), cv_attending_unrelated_clinic :=0]
orig_dt <- orig_dt[is.na(cv_schoolchildren), cv_schoolchildren :=0]
orig_dt <- orig_dt[is.na(cv_pregnant), cv_pregnant :=0]
orig_dt <- orig_dt[is.na(cv_medical_condition), cv_medical_condition :=0]
orig_dt <- orig_dt[is.na(cv_specific_occupation), cv_specific_occupation :=0]
orig_dt <- orig_dt[is.na(cv_institutionalized), cv_institutionalized :=0]
orig_dt <- orig_dt[is.na(cv_vaccination), cv_vaccination :=0]
orig_dt <- orig_dt[is.na(cv_double_anti), cv_double_anti :=0]
orig_dt <- orig_dt[is.na(cv_antibody), cv_antibody :=0]


# --- Exclude cv_medical_codition==1 as it only captures HBV seropositivity among HIV+ individuals
orig_dt <- subset(orig_dt, !(cv_medical_condition==1 & cv_institutionalized==0))
orig_dt$cv_medical_condition<- NULL

write.xlsx(orig_dt, paste0(mrbrt_crosswalk_dir, "bundle_version_", bundle_version_id_new, "_cleaned_cov_", date, ".xlsx"))


################################################################################
## STEP 5: Run sex split
################################################################################

#Additional data cleaning - data extracted incorrectly
orig_dt[(nid==420070), `:=` (standard_error=NA, lower=NA, upper=NA)]
orig_dt <- subset(orig_dt, nid!=214749 & age_start!=0)


pd <- import("pandas")
sex_results  <- pd$read_pickle("FILEPATH/results.pkl"))

sex_results$fixed_vars
sex_results$beta_sd
sex_results$gamma



# ADJUST VALUES FOR SEX SPLITTING FUNCTION -----------------------
# Sex splitting 
full_dt <- as.data.table(copy(orig_dt))
full_dt <- full_dt[measure == measures, ]


# The offset does not affect the estimated value of pred_diff_se or pred_diff_mean but is needed to run through adjust_orig_vals
full_dt$crosswalk_parent_seq <- ""
dt_sex_split <- split_both_sex(full_dt, sex_results)
final_sex_dt <- copy(dt_sex_split$final)
graph_sex_dt <- copy(dt_sex_split$graph)

sex_graph <- graph_sex_predictions(graph_sex_dt)
sex_graph




###################################################################################################
# STEP 6: CROSSWALK
##################################################################################################


# Apply the same crosswalk to BLOOD DONORs ----------------------------------------------------------
table(final_sex_dt$cv_blood_donor)
table(final_sex_dt$cv_pregnant)

pd <- import("pandas")
results  <- pd$read_pickle(paste0("FILEPATH/results.pkl"))
results$fixed_vars
results$beta_sd
results$gamma

cvs2 <- names(final_sex_dt)[grepl("^cv_", names(final_sex_dt)) ]
cvs2
cv_drop <- c( "cv_antibody", "cv_RNA", "cv_healthcare_worker" ,         "cv_attending_unrelated_clinic",
              "cv_schoolchildren" ,   "cv_hospital_referral" ,  "cv_specific_occupation"   ,     "cv_institutionalized"   ,      
              "cv_vaccination"  ,  "cv_double_anti"     , "cv_antibody"    )



# ADJUST THE DATA  -------------------------------------------------
full_dt <- copy(final_sex_dt[measure %in% measures])
full_dt <- get_definitions(full_dt)
full_dt <- full_dt[[1]]
full_dt$definition <- gsub("_cv_", "", full_dt$definition)
unique(full_dt$definition)
full_dt_exc <- subset(full_dt, definition =="blood_donorpregnant") 
full_dt_adj <- subset(full_dt, definition =="reference" | definition =="blood_donor" | definition =="pregnant")
unique(full_dt_exc$definition)
unique(full_dt_adj$definition)
full_dt_adj <- subset(full_dt_adj, mean<1) #there is one data point from nid 500441, age gorup 2-4 where prevalence for both sexes combined is 1. So splitting it to sex-specific estimates pushes the male estimate above 1.
adjusted <- make_adjustment(results, full_dt_adj)
epidb <- copy(adjusted$epidb)

test3 <- epidb[, c("nid", "year_start", "year_end", "age_start", "age_end", "sex", "mean", "cases", "sample_size", "cv_blood_donor","cv_pregnant", "extractor")]
test3
cw_file_path <- FILEPATH





# Under 25 subset ---------------------------------------------------------
age_pattern <- copy(epidb)
age_pattern <- age_pattern[, age_range := age_end - age_start ]
age_25 <- age_pattern[age_range <= 25,]
age_25_over <- age_pattern[age_range > 25,]




# GET OBJECTS -------------------------------------------------------------
b_id <- OBJECT 
a_cause <- "hepatitis_b"
name <- "hepatitis_b"
id <- OBJECT 
ver_id <- OBJECT  
description <- DESCRIPTION


ages <- get_age_metadata()
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 0, age_group_id]

df <- copy(age_25_over)
gbd_id <- id


region_pattern <- F
location_pattern_id <- 1

## GET TABLES
sex_names <- get_ids(table = "sex")
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 99]
super_region_dt <- get_location_metadata()
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]


## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
original <- copy(df)
original[, id := 1:.N]

## FORMAT DATA
dt <- get_cases_sample_size(original)
dt <- get_se(dt)
dt <- calculate_cases_fromse(dt)
dt <- format_data(dt, sex_dt = sex_names)

## EXPAND AGE
split_dt <- expand_age(dt, age_dt = ages)

## GET PULL LOCATIONS
if (region_pattern == T){
  message("Splitting by region pattern")
  split_dt <- merge(split_dt, super_region_dt, by = "location_id")
  super_regions <- unique(split_dt$super_region_id) ##get super regions
  locations <- super_regions
} else {
  locations <- location_pattern_id
  message("Splitting by global pattern")
}

##GET LOCS AND POPS
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)

## GET AGE PATTERN
print("getting age pattern")
age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) #set desired super region here if you want to specify

if (region_pattern == T) {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
} else {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
}

## GET POPULATION INFO
print("getting pop structure")
pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

#####CALCULATE AGE SPLIT POINTS#######################################################################
## CREATE NEW POINTS
print("splitting data")
split_dt <- split_data(split_dt)
######################################################################################################
# split_dt$crosswalk_parent_seq <- NA

final_age_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                     original_dt = original)


#final_age_dt <- final_age_dt[!(group_review %in% 0), ]
final_age_dt <- final_age_dt[standard_error >1, standard_error := NA]
final_age_dt <- final_age_dt[location_id != 95, ]
# final_age_dt[crosswalk_parent_seq == 1, crosswalk_parent_seq := origin_seq]
final_age_dt[!is.na(crosswalk_parent_seq), seq := NA]
locs <- get_location_metadata()
gbr <- locs[grepl("GBR_", ihme_loc_id) & level == 5, unique(location_id)]
final_age_dt <- final_age_dt[!(location_id %in% gbr)]
length(unique(final_age_dt$nid))

final <- as.data.table(rbind.fill(final_age_dt, age_25))
final <- merge( df_old_prev[,c("nid",  "sex","origin_seq",  "location_id", "year_start", "age_start","is_outlier_old")], final, by =c("nid",  "sex","origin_seq",  "location_id", "year_start", "age_start"), all.y = TRUE )
final[(is_outlier!=is_outlier_old), `:=` (is_outlier = is_outlier_old)]




final <- as.data.table(rbind.fill(final, df_old_csmr))

test <- subset(age_25, seq!=crosswalk_parent_seq)

final[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
final[is.na(crosswalk_parent_seq), crosswalk_parent_seq := origin_seq]

final[is.na(crosswalk_parent_seq)]
final[, seq := NA]

# basic validation checks 
final[location_id ==95]
final[is.na(age_start)]
final[is.na(age_end)]
final[is.na(location_id)]
final[is.na(crosswalk_parent_seq) & is.na(seq), ]
final[!is.na(crosswalk_parent_seq) & !is.na(seq), ]

final$group_review[final$group_review==0] <- NA
final$group[is.na(final$group_review)] <- NA
final$specificity[is.na(final$group_review)] <- NA
final$note_sr <- substring(final$note_sr, 1, 2000)

final[, unit_value_as_published := 1]
final <- final[standard_error >1, standard_error := NA]

n_occur <- data.frame(table(final$seq))
n_occur[n_occur$Freq > 1,]

final <- subset(final, !(mean==0 & is.na(lower) & is.na(cases) & is.na(sample_size) & is.na(standard_error) & is.na(effective_sample_size)))


# all_final1[!is.na(crosswalk_parent_seq) & !is.na(seq), seq := NA]
file_path <- "FILEPATH"
write.xlsx(final , file_path, sheetName = "extraction", rowNames = FALSE)

###########
#STEP 6: Upload MAD-outliered new data 
description <- DESCRIPTION
result <- save_crosswalk_version( bundle_version_id_new, file_path, description)

