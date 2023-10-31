##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: CROSSWALKING 
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "/home/j/"
  h <- "~/"
  l <- "/ihme/limited_use/"
} else {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
}

date <- gsub("-", "_", Sys.Date())

library(crosswalk, lib.loc = FILEPATH)
library(mortdb, lib = FILEPATH))
pacman::p_load(data.table, openxlsx, ggplot2, boot, gtools, msm, Hmisc, dplyr, plyr)
library(gridExtra)
library(grid) 

gbd_round_id <- OBJECT
decomp_step <- OBJECT

# SET OBJECTS -------------------------------------------------------------


b_id <- OBJECT
a_cause <- OBJECT
bv_id <- OBJECT

# For crosswalking 
cv_drop <- ""
outlier_status <- c(0) # this should only be c(0,1) for hepatitis b
id_vars <- "id_var"
keep_x_intercept <- T
logit_transform <- T
reference <- ""
reference_def <- "reference"
trim <- 0.1
measures <- "prevalence"
method_var <- "obs_method"
gold_def <- "inpatient"
year_range <- 5
# modeling <- T
mrbrt_dir <- FILEPATH


model_name_ms2000 <- paste0("total_cirrhosis_2000_", date)
dir.create(paste0(mrbrt_dir, model_name_ms2000))

model_name_ms_other <- paste0("total_cirrhosis_other_", date)
dir.create(paste0(mrbrt_dir, model_name_ms_other))

#For MAD outlier
MAD <- 2
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) # new in GBD 2020
byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 

#Bulk outlier as suggested by the Clinical Informatics Team
outlier_list <- as.data.table(read.xlsx(FILEPATH))


# Crosswalk specifications 
if(logit_transform == T) {
  response <- "ldiff"
  data_se <- "ldiff_se"
  mrbrt_response <- "diff_logit"
} else {
  response <- "ratio_log"
  data_se <- "ratio_se_log"
  mrbrt_response <- "diff_log"
}


draws <- paste0("draw_", 0:999)
mrbrt_dir <- FILEPATH)
if (!dir.exists(mrbrt_dir)) dir.create(mrbrt_dir)

# SOURCE FUNCTIONS --------------------------------------------------------
source(FILEPATH)


# DATA PROCESSING FUNCTIONS -----------------------------------------------

# GET DATA FOR ADJUSTMENT 
orig_dt <- get_bundle_version(bundle_version_id = bv_id, fetch = "all")

# RUN DATA SETUP ----------------------------------------------------------

message("Formatting data")
cir_dt <- copy(orig_dt)
cir_dt <- fix_mkscn_cvs(cir_dt)
cir_dt <- get_cases_sample_size(cir_dt)
cir_dt <- get_se(cir_dt)
cir_dt <- calculate_cases_fromse(cir_dt)
cir_dt[, mid_year := (year_start + year_end) / 2]

# Create obs_method 
cir_dt[(cv_ms2000==1), `:=` (obs_method = "ms2000")]
cir_dt[(cv_marketscan==1), `:=` (obs_method = "marketscan")]
cir_dt[(cv_hospital==1), `:=` (obs_method = "inpatient")]


df_zero <- subset(cir_dt, mean ==0 | nid == 416752 | nid == 454277)
unique(df_zero$obs_method) #check if any of the mean = 0 data points are from alternative case definitions
df_nonzero <- subset(cir_dt, mean >0 & mean <1 & nid!=416752 & nid != 454277) #drop all rows of data with mean greater than 1
df_nonzero[(sex=="Male"), `:=` (sex_id =1)]
df_nonzero[(sex=="Female"), `:=` (sex_id =2)]

df_nonzero_ms2000 <- subset(df_nonzero, cv_ms2000 ==1)
df_nonzero_ms <- subset(df_nonzero, cv_marketscan ==1)
df_nonzero_ref <- df_nonzero[!(cv_marketscan %in% 1) & !(cv_ms2000 %in% 1)] # != is not working right now so use %in% 

#logit transform mean and delta-transform SE
df_nonzero[, c("mean_logit", "se_logit")] <- as.data.frame(delta_transform(
  mean = df_nonzero$mean, 
  sd = df_nonzero$standard_error,
  transformation = "linear_to_logit"))

# Match alternative to reference 
# 1. Subset datasets based on case definition 
ref <- subset(df_nonzero, cv_hospital==1)
alt_ms2000 <- subset(df_nonzero, cv_ms2000==1)
alt_ms <- subset(df_nonzero, cv_marketscan==1)

#2. clean ref and alt datasets
alt_ms2000 <- alt_ms2000[, c("nid", "location_id", "sex", "age_start", "age_end",  "mid_year", "measure", "mean", "standard_error", "cv_ms2000","mean_logit", "se_logit")]
alt_ms <- alt_ms[, c("nid", "location_id", "sex", "age_start", "age_end", "mid_year", "measure", "mean", "standard_error",  "cv_marketscan","mean_logit", "se_logit")]
ref[, c("cv_ms2000", "cv_marketscan")] <- NULL

#3. change the mean and standard error variables in both ref and alt datasets for clarification
setnames(ref, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_ref", "prev_se_ref", "mid_year_ref", "nid_ref", "mean_logit_ref", "se_logit_ref"))
setnames(alt_ms2000, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))
setnames(alt_ms, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))

#4. Create a variable identifying different case definitions
alt_ms2000$dorm_alt<- "ms2000"
alt_ms$dorm_alt<- "marketscan"
ref$dorm_ref<- "inpatient"

#5. between-study mathces based on exact location, sex, age
df_matched_ms2000 <- merge(ref, alt_ms2000, by = c("location_id", "sex", "age_start", "age_end", "measure"))
df_matched_ms     <- merge(ref, alt_ms, by = c( "location_id", "sex", "age_start", "age_end", "measure"))

#6 between-study matches based on 5-year span
df_matched_ms2000 <- df_matched_ms2000[abs(df_matched_ms2000$mid_year_ref - df_matched_ms2000$mid_year_alt) <= year_range, ] 
df_matched_ms <- df_matched_ms[abs(df_matched_ms$mid_year_ref - df_matched_ms$mid_year_alt) <= year_range, ] 

#7. Optional: check the matches
df_matched_ms2000 %>% select(location_name, age_start, age_end, nid_ref, mid_year_ref, prev_ref, dorm_ref, mean_logit_ref, se_logit_ref, nid_alt, mid_year_alt, prev_alt, dorm_alt, mean_logit_alt, se_logit_alt)
unique(df_matched_ms2000$nid_ref)
unique(df_matched_ms2000$nid_alt)

#8. calculate logit difference : logit(prev_alt) - logit(prev_ref)
df_matched_ms2000[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = df_matched_ms2000, 
  alt_mean = "mean_logit_alt", alt_sd = "se_logit_alt",
  ref_mean = "mean_logit_ref", ref_sd = "se_logit_ref")

df_matched_ms[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = df_matched_ms, 
  alt_mean = "mean_logit_alt", alt_sd = "se_logit_alt",
  ref_mean = "mean_logit_ref", ref_sd = "se_logit_ref")

#9. create study_id 
df_matched_ms2000 <- as.data.table(df_matched_ms2000)
df_matched_ms2000[, id := .GRP, by = c("nid_ref", "nid_alt")]

df_matched_ms <- as.data.table(df_matched_ms)
df_matched_ms[, id := .GRP, by = c("nid_ref", "nid_alt")]

#10. fill in cv_ms2000 and cv_markestcan
df_matched_ms[(is.na(cv_marketscan)), `:=` (cv_marketscan=0)]
df_matched_ms2000[(is.na(cv_ms2000)), `:=` (cv_ms2000=0)]


# FORMAT DATA FOR MR MRBRT 
#CWData() formats meta-regression data  
dat1_ms2000 <- CWData(df = df_matched_ms2000, 
                      obs = "logit_diff",                         #matched differences in logit space
                      obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                      alt_dorms = "dorm_alt",                     #var for the alternative def/method
                      ref_dorms = "dorm_ref",                     #var for the reference def/method
                      covs = list("age_start", "sex_id"),  #list of (potential) covariate columes 
                      study_id = "id" )

dat1_ms <- CWData(df = df_matched_ms, 
                  obs = "logit_diff",                         #matched differences in logit space
                  obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                  alt_dorms = "dorm_alt",                     #var for the alternative def/method
                  ref_dorms = "dorm_ref",                     #var for the reference def/method
                  covs = list("age_start", "sex_id"),  #list of (potential) covariate columes 
                  study_id = "id" )


# RUN MRBRT MODEL FOR BOTH MS OTHER AND MS 2000 
fit1_ms2000 <- CWModel(
  cwdata = dat1_ms2000,                              #result of CWData() function  call
  obs_type = "diff_logit",                    #must be "diff_logit" or "diff_log"
  cov_models = list(
    CovModel("age_start"),
    CovModel("sex_id"),
    CovModel("intercept")),      #specify covariate details
  inlier_pct = 0.9,               
  gold_dorm = "inpatient" )                   #level of "dorm_ref" that is the gold standard



fit1_ms <- CWModel(
  cwdata = dat1_ms,                              #result of CWData() function  call
  obs_type = "diff_logit",                    #must be "diff_logit" or "diff_log"
  cov_models = list(
    CovModel("age_start"),
    CovModel("sex_id"),
    CovModel("intercept")),      #specify covariate details
  inlier_pct = 0.9,               
  gold_dorm = "inpatient" )   

fit1_ms2000$fixed_vars
fit1_ms$fixed_vars

## SAVE MODEL OUTPUTS ---------------------------------------
save_ms <- save_mrbrt(fit1_ms, mrbrt_dir, model_name_ms_other)
save_ms2000 <- save_mrbrt(fit1_ms2000, mrbrt_dir, model_name_ms2000)

# ADJUST VALUES -----------------------------------------------
## apply crosswalk coefficients to original data
setnames(df_nonzero_ms2000, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
df_nonzero_ms2000[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = fit1_ms2000,       # result of CWModel()
  df = df_nonzero_ms2000,            # original data with obs to be adjusted
  orig_dorms = "obs_method", # name of column with (all) def/method levels
  orig_vals_mean = "orig_mean",  # original mean
  orig_vals_se = "orig_standard_error"  # standard error of original mean
)

setnames(df_nonzero_ms, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
df_nonzero_ms[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = fit1_ms,       # result of CWModel()
  df = df_nonzero_ms,            # original data with obs to be adjusted
  orig_dorms = "obs_method", # name of column with (all) def/method levels
  orig_vals_mean = "orig_mean",  # original mean
  orig_vals_se = "orig_standard_error"  # standard error of original mean
)


df_final <- as.data.table(rbind.fill(df_nonzero_ms, df_nonzero_ms2000, df_nonzero_ref, df_zero))
nrow(df_final) == nrow(orig_dt)

df_nonzero_ms2000 %>% select(location_name, age_start, age_end, nid, orig_mean, orig_standard_error, mean, standard_error, diff, diff_se, obs_method)
df_nonzero_ms %>% select(location_name, age_start, age_end, nid, orig_mean, orig_standard_error, mean, standard_error, diff, diff_se, obs_method)

## clean for Epi Uploader validation
df_final <- df_final[(obs_method!="inpatient"), `:=` (lower = NA, upper = NA)]
df_final[is.na(lower), uncertainty_type_value := NA]
df_final[(obs_method!="inpatient"), `:=` (crosswalk_parent_seq = seq)]
df_final[(obs_method=="inpatient"), `:=` (crosswalk_parent_seq = NA)]

df_final$standard_error[df_final$standard_error>1] <-1

data_filepath <- FILEPATH
write.xlsx(df_final, data_filepath, row.names = FALSE, sheetName = "extraction")


#########################################################################################
##STEP 5: MAD outlier
new_xwalk <- as.data.table(copy(df_final))


new_xwalk_not_MAD <- subset(new_xwalk, measure!="incidence" & measure != "prevalence")
new_xwalk<- subset(new_xwalk, measure=="incidence" | measure == "prevalence")
unique(new_xwalk$measure)
nrow(new_xwalk)
nrow(new_xwalk[is_outlier == 1])

## GET AGE WEIGHTS
unique(new_xwalk$age_start)

fine_age_groups <- "no" #specify how you want to get your age group weights

if (fine_age_groups=="yes") {
  #---option 1: if all data are at the most detailed age groups
  all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) 
  setnames(all_fine_ages, c("age_group_years_start"), c("age_start"))
  
  
} else if (fine_age_groups=="no") {
  #---option 2: if <1 years is aggregated, change that age group id to 28 and aggregate age weights
  all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) 
  setnames(all_fine_ages, c("age_group_years_start"), c("age_start"))
  not_babies <- all_fine_ages[!age_group_id %in% c(2:3, 388, 389)]
  not_babies[, age_group_years_end := age_group_years_end-1]
  not_babies <- not_babies[, c("age_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
  group_babies1 <- all_fine_ages[age_group_id %in% c(2:3, 388, 389)]
  group_babies1$age_group_id <- 28
  group_babies1$age_group_years_end <- 0.999
  group_babies1$age_start <- 0
  group_babies1[, age_group_weight_value := sum(age_group_weight_value)]
  group_babies1 <- group_babies1[, c("age_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
  group_babies1 <- unique(group_babies1)
  all_fine_ages <- rbind(not_babies, group_babies1)  
  
}

## Delete rows with emtpy means
nrow(new_xwalk[is.na(mean)])
new_xwalk<- new_xwalk[!is.na(mean)]


##merge age table map and merge on to dataset
new_xwalk <- as.data.table(merge(new_xwalk, all_fine_ages, by = c("age_start")))

#calculate age-standardized prevalence/incidence below:
##create new age-weights for each data source
new_xwalk <- new_xwalk[, sum1 := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
new_xwalk <- new_xwalk[, new_weight1 := age_group_weight_value/sum1, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 

##age standardizing per location-year by sex
#add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
new_xwalk[, as_mean := mean * new_weight1] #initially just the weighted mean for that age-location-year-sex-nid
new_xwalk[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series

##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
new_xwalk[as_mean == 0, is_outlier := 1] 
new_xwalk$note_modeler <- as.character(new_xwalk$note_modeler)
new_xwalk[as_mean == 0, note_modeler := paste0(note_modeler, " |  outliered this location-year-sex-NID age-series because age standardized mean is 0")]

## log-transform to pick up low outliers
new_xwalk[as_mean != 0, as_mean := log(as_mean)]

# calculate median absolute deviation
new_xwalk[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
new_xwalk[,mad_val:=mad(as_mean,na.rm = T),by=c("sex")]
new_xwalk[,median:=median(as_mean,na.rm = T),by=c("sex")]


# outlier based on MAD
new_xwalk[as_mean>((MAD*mad_val)+median), is_outlier := 1]
new_xwalk[as_mean>((MAD*mad_val)+median), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is higher than ", MAD, " MAD above median")]
new_xwalk[as_mean<(median-(MAD*mad_val)), is_outlier := 1]
new_xwalk[as_mean<(median-(MAD*mad_val)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than ", MAD, " MAD below median")]
with(new_xwalk, table(sex, mad_val))
with(new_xwalk, table(sex, median))
with(new_xwalk, table(sex, exp(median)))


# bulk outlier as requested by the Clinical Informatics Team
outlier <- subset(outlier_list, bundle_id == b_id)
nrow(outlier)
length(unique(outlier$nid))
outlier$sex[outlier$sex_id==1] <- "Male"
outlier$sex[outlier$sex_id==2] <- "Female"

outlier <- outlier[, c("nid", "age_group_id", "location_id", "year_start", "year_end", "sex")]
outlier$bulk_outlier <- 1

new_xwalk2 <- merge(new_xwalk, outlier, by =c("nid", "age_group_id", "location_id", "year_start", "year_end", "sex"),  all.x= T)
new_xwalk2[(bulk_outlier==1), is_outlier:=1]

# clean for upload
new_xwalk2[, c("age_group_name",	"age_group_years_start",	"age_group_years_end",	"most_detailed",	"sum1",	"new_weight1","sum", "new_weight", "as_mean","age_group_weight_value", "age_group_id") := NULL]
new_xwalk2[is.na(is_outlier), is_outlier:=0]

all_final <- rbind.fill(new_xwalk2, new_xwalk_not_MAD)
all_final <- as.data.table(all_final)

