############################################################################################################################
# This script is used to adjust systematic bias in alternative case definitions using log-transformation and MR-BRT analysis
###########################################################################################################################

library(dplyr)
library(plyr)
library(msm, lib.loc="FILEPATH")
library(metafor, lib.loc="FILEPATH")
library(readxl)
library(data.table)
library(readxl)
source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(location_set_id = 35)
main_dir <- "FILEPATH"
draws <- paste0("draw_", 0:999)

# INPUT DATA ---------------------------------------------------------
df <- as.data.table(read.csv("FILEPATH FOR SEX SPLIT DATA"))
df_inc <- subset(df, measure=="incidence")
df_prev <- subset(df, measure=="prevalence")

######################################################################################################################
## INCIDENCE FIRST
######################################################################################################################
###Ref: Hospital data from all locations + Taiwan claims data
###Alt 1 (only alternative): Chart-reviewed data
location_match <- "exact"
year_range <-0       #SPECIFY YEAR RANGE FOR BETWEEN STUDY COMPARISON
covariate_name <- "cv_chart_review"   #set covariate of interest for crosswalk (alt)
cause_path <- "Chronic_pancreatitis"  #SPECIFY FILEPATH
cause_name <- "digest_pancreatitis" #SPECIFY CAUSE NAME


#CREATE COVARIATES AND TAG REFERENCE vs. ALTERNATIVE
df_inc = within(df_inc, {cv_chart_review = ifelse(cv_chart_review==1, 1, 0)})
df_inc = within(df_inc, {cv_hospital = ifelse( clinical_data_type=="inpatient" | location_name=="Taiwan", 1, 0)})

df_inc$is_reference <- ifelse((df_inc$cv_hospital==1),1,0)
df_inc$is_alt <-ifelse(df_inc$cv_chart_review==1,1,0)

## FILL OUT MEAN/CASES/SAMPLE SIZE 
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS 
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

# BACK CALCULATE CASES AND SAMPLE SIZE FROM SE
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

df_inc <- get_cases_sample_size(df_inc)
df_inc <- get_se(df_inc)
df_inc <- calculate_cases_fromse(df_inc)


# REMOVE OUTLIER ROWS
df_inc <- df_inc[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 


# CREATE A WORKING DATAFRAME
wdf <- df_inc[,c("measure", "location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt")]
covs <- select(df_inc, matches("^cv_"))
wdf <- cbind(wdf, covs)
wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

#AGGREGATING MARKETSCAN DATA
aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  if (covariate_name == "cv_marketscan") marketscan_dt <- copy(dt[cv_marketscan==1])
  if (covariate_name == "cv_ms2000") marketscan_dt <- copy(dt[cv_ms2000==1])
  
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt, fill=TRUE)
  return(full_dt)
}
wdf <- aggregate_marketscan(wdf)

#CREATE INDICATORS FOR MERGING
if(location_match=="exact"){
  wdf$location_match <- wdf$location_id
} else if(location_match=="country"){
  wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdf$location_match <- wdf$region_name
} else if(location_match=="super"){
  wdf$location_match <- wdf$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)

#EXACT AGE MATCH
wdf$age_start_match <- wdf$age_start
wdf$age_end_match <- wdf$age_end

#MID-YEAR CALCULATION
wdf$mid_year <- (wdf$year_start + wdf$year_end)/2


#COLLAPSE TO THE DESIRED MERGING
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)

#MERGE AND MATCH REFERENCE AND ALTERNATIVE WITHIN THE SAME STUDY
setnames(nref, c("mean","standard_error","cases","sample_size"), c("n_mean","n_standard_error","n_cases","n_sample_size"))
wmean <- merge(ref, nref, by=c("measure", "nid","age_start","age_end","year_start","year_end", "location_match","sex")) 
wmean <- unique(wmean)


#GET THE RATIO
wmean$ratio <- (wmean$n_cases/wmean$n_sample_size) / (wmean$cases/wmean$sample_size)
wmean$ratio_se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))
wmean <- subset(wmean, standard_error > 0 )
wmean <- subset(wmean, mean != 0 )
wmean <- subset(wmean, n_mean != 0 )


#CONVERT THE RATIO AND STANDARD ERROR TO LOG SPACE
wmean$log_ratio <- log(wmean$ratio)
wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
  ratio_i <- wmean[i, "ratio"]
  ratio_se_i <- wmean[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#SUMMARIZE RESULTS 
mod <- rma(yi=log_ratio, sei=delta_log_se, data=wmean, measure="RR")
results_inc <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))  
results_inc


#WRITE OUTPUT TO CSV  
write.csv(wmean_inc, "FILEPATH", row.names = F)
write.csv(results_inc, "FILEPATH", row.names = F)


#DATA PREP TO RUN MR-BRT AND APPLY PREDICTION TO THE ORIGINAL DATA ---------------------------------------------------
# Load original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables
dat_original <- as.data.table(df_inc)

#SET YOUR REFERENCE IF ALREADY NOT DONE
dat_original = within(dat_original, {cv_chart_review = ifelse(cv_chart_review==1, 1, 0)})
dat_original = within(dat_original, {cv_hospital = ifelse( clinical_data_type=="inpatient" | location_name=="Taiwan", 1, 0)})

dat_original$is_reference <- ifelse((dat_original$cv_hospital==1),1,0)

reference_var <- "is_reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- "cv_chart_review" # can be a vector of names 


# Load data for meta-regression model 
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as alternative/reference
tmp_metareg <- read.csv(master_xwalk) %>%
  mutate(cv_ms2000 = if_else(cv_chart_review.y == reference_value, 1, 0, 0))


#STANDARDIZE VARIABLE NAMES IN THE ORIGINAL EXTRACTED DATASHEET
orig_vars <- c(mean_var, se_var, reference_var, cov_names)
tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))    ##CREATE A SUBSET OF DATASETS THAT ONLY CONTAIN MEAN SE REF AND COVARIATES OF INTEREST


#Log transform the original data
#-- SEs transformed using the delta method
tmp_orig$mean_log <- log(tmp_orig$mean)
tmp_orig$se_log <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "standard_error"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

#FIT THE MR-BRT MODEL -----------------------------------------------------------------------------------
repo_dir <- "/home/j/temp/reed/prog/projects/run_mr_brt/"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

covariate_name <- "_inc"
trim <- 0.10

fit1 <- run_mr_brt(
  output_dir =  "FILEPATH",
  model_label = paste0("step2_inc_digest_", trim),
  data = tmp_metareg,
  overwrite_previous = TRUE,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10, 
  mean_var = "log_ratio",
  se_var = "delta_log_se"
)

#FIT THE MR-BRT MODEL -----------------------------------------------------------------------------------
eval(parse(text = paste0("predicted <- expand.grid(", paste0(cov_names, "=c(0, 1)", collapse = ", "), ")")))
predicted <- as.data.table(predict_mr_brt(fit1, newdata = predicted)["model_summaries"])

names(predicted) <- gsub("model_summaries.", "", names(predicted))
names(predicted) <- gsub("X_d_", "cv_", names(predicted))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
predicted$cv_chart_review <-1
predicted <- as.data.table(predicted)

review_sheet_final_inc <- merge(total_dt_inc, predicted, by ="cv_stringent")
review_sheet_final_inc <-as.data.table(review_sheet_final_inc)
review_sheet_final_inc[, `:=` (log_mean = log(mean), log_se = deltamethod(~log(x1), mean, standard_error^2)), by = c("mean", "standard_error")]
review_sheet_final_inc[, `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
review_sheet_final_inc[, `:=` (mean = exp(log_mean), standard_error = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
review_sheet_final_inc[, `:=` (cases = NA, lower = NA, upper = NA)]

review_sheet_final_inc[, (c("Y_mean", "Y_se", "log_mean", "log_se")) := NULL]
review_sheet_final_inc[is.na(lower), uncertainty_type_value := NA]

######################################################################################################################
## PREVALENCE NEXT
############################################################################################################################ SETTING UP COVARIATES ######
###Ref: Hospital data from all locations + Taiwan claims data
###Alt 1: Marketscan 2010-2014
###Alt 2: Marketscan 2000
###Alt 3: Chart-reviewed data

# Adjust Alt 1 first
location_match <- "exact"
year_range <-0       #SPECIFY YEAR RANGE FOR BETWEEN STUDY COMPARISON
covariate_name <- "cv_marketscan"   #set covariate of interest for crosswalk (alt)
cause_path <- "Chronic_pancreatitis"  #SPECIFY FILEPATH
cause_name <- "digest_pancreatitis" #SPECIFY CAUSE NAME


#CREATE COVARIATES AND TAG REFERENCE vs. ALTERNATIVE
df <- as.data.table(df_prev)
df = within(df, {cv_marketscan = ifelse((year_start!=2000 & clinical_data_type=="claims" & location_name!="Taiwan"), 1, 0)})
df = within(df, {cv_hospital = ifelse( clinical_data_type=="inpatient" | location_name=="Taiwan", 1, 0)})

df$is_reference <- ifelse((df$cv_hospital==1),1,0)
df$is_alt <-ifelse(df$cv_marketscan==1,1,0)


## FILL OUT MEAN/CASES/SAMPLE SIZE 
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS 
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

# BACK CALCULATE CASES AND SAMPLE SIZE FROM SE
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)


# REMOVE OUTLIER ROWS
df <- df[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 


# CREATE A WORKING DATAFRAME
wdf <- df[,c("measure", "location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt")]
covs <- select(df, matches("^cv_"))
wdf <- cbind(wdf, covs)
wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

#AGGREGATING MARKETSCAN DATA
aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  if (covariate_name == "cv_marketscan") marketscan_dt <- copy(dt[cv_marketscan==1])
  if (covariate_name == "cv_ms2000") marketscan_dt <- copy(dt[cv_ms2000==1])
  
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt, fill=TRUE)
  return(full_dt)
}
wdf <- aggregate_marketscan(wdf)

#CREATE INDICATORS FOR MERGING
if(location_match=="exact"){
  wdf$location_match <- wdf$location_id
} else if(location_match=="country"){
  wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdf$location_match <- wdf$region_name
} else if(location_match=="super"){
  wdf$location_match <- wdf$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

  wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)

#EXACT AGE MATCH
  wdf$age_start_match <- wdf$age_start
  wdf$age_end_match <- wdf$age_end

#MID-YEAR CALCULATION
  wdf$mid_year <- (wdf$year_start + wdf$year_end)/2


#COLLAPSE TO THE DESIRED MERGING
  ref <- subset(wdf, is_reference == 1)
  nref <- subset(wdf, is_alt == 1)

#MERGE AND MATCH REFERENCE AND ALTERNATIVE
  setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "mid_year"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_mid_year"))
  wmean <- merge(ref, nref, by=c("age_start_match", "age_end_match", "location_match", "measure"), allow.cartesian = T)
  wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x",  "ihme_loc_abv.x", "ihme_loc_abv.y", "location_id.x", "location_id.y")] <- NULL
  wmean <- wmean[abs(wmean$mid_year - wmean$n_mid_year) <= year_range, ]
  
#DROP ROWS IF SEXES ARE NOT THE SAME FOR SEX AND N=SEX IN A GIVE ROW - THEN COMBINE SEX.X AND SEX.Y INTO ONE SEX COLUMN
  wmean <- wmean[wmean$sex.x==wmean$sex.y, ]
  wmean$sex <- wmean$sex.x
  wmean[, c("sex.x", "sex.y")] <- NULL
  
#DROP DUPLICATES
  wmean <- unique(wmean)

#GET THE RATIO
 wmean$ratio <- (wmean$n_cases/wmean$n_sample_size) / (wmean$cases/wmean$sample_size)
 wmean$ratio_se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))
 wmean <- subset(wmean, standard_error > 0 )
 wmean <- subset(wmean, mean != 0 )
 wmean <- subset(wmean, n_mean != 0 )
 
 
#CONVERT THE RATIO AND STANDARD ERROR TO LOG SPACE
 wmean$log_ratio <- log(wmean$ratio)
 wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
   ratio_i <- wmean[i, "ratio"]
   ratio_se_i <- wmean[i, "ratio_se"]
   deltamethod(~log(x1), ratio_i, ratio_se_i^2)
 })
 
#SUMMARIZE RESULTS 
 mod <- rma(yi=log_ratio, sei=delta_log_se, data=wmean, measure="RR")
 results1 <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))  
 results1


#WRITE OUTPUT TO CSV  
write.csv(wmean1, "FILEPATH", row.names = F)
write.csv(results1, "FILEPATH", row.names = F)


#######################################################################################################################
# Adjust Alt 2 next
covariate_name <- "cv_ms2000"   #set covariate of interest for crosswalk (alt)

df$is_reference <- ifelse((df$cv_hospital==1),1,0)
df$is_alt <-ifelse(df$cv_ms2000==1,1,0)


## FILL OUT MEAN/CASES/SAMPLE SIZE 
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS 
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

# BACK CALCULATE CASES AND SAMPLE SIZE FROM SE
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)


# REMOVE OUTLIER ROWS
df <- df[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 


# CREATE A WORKING DATAFRAME
wdf <- df[,c("measure", "location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt")]
covs <- select(df, matches("^cv_"))
wdf <- cbind(wdf, covs)
wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

#AGGREGATING MARKETSCAN DATA
aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  if (covariate_name == "cv_marketscan") marketscan_dt <- copy(dt[cv_marketscan==1])
  if (covariate_name == "cv_ms2000") marketscan_dt <- copy(dt[cv_ms2000==1])
  
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt, fill=TRUE)
  return(full_dt)
}
wdf <- aggregate_marketscan(wdf)

#CREATE INDICATORS FOR MERGING
if(location_match=="exact"){
  wdf$location_match <- wdf$location_id
} else if(location_match=="country"){
  wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdf$location_match <- wdf$region_name
} else if(location_match=="super"){
  wdf$location_match <- wdf$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)

#EXACT AGE MATCH
wdf$age_start_match <- wdf$age_start
wdf$age_end_match <- wdf$age_end

#MID-YEAR CALCULATION
wdf$mid_year <- (wdf$year_start + wdf$year_end)/2


#COLLAPSE TO THE DESIRED MERGING
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)

#MERGE AND MATCH REFERENCE AND ALTERNATIVE
setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "mid_year"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_mid_year"))
wmean <- merge(ref, nref, by=c("age_start_match", "age_end_match", "location_match", "measure"), allow.cartesian = T)
wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x",  "ihme_loc_abv.x", "ihme_loc_abv.y", "location_id.x", "location_id.y")] <- NULL
wmean <- wmean[abs(wmean$mid_year - wmean$n_mid_year) <= year_range, ]

#DROP ROWS IF SEXES ARE NOT THE SAME FOR SEX AND N=SEX IN A GIVE ROW - THEN COMBINE SEX.X AND SEX.Y INTO ONE SEX COLUMN
wmean <- wmean[wmean$sex.x==wmean$sex.y, ]
wmean$sex <- wmean$sex.x
wmean[, c("sex.x", "sex.y")] <- NULL

#DROP DUPLICATES
wmean <- unique(wmean)

#GET THE RATIO
wmean$ratio <- (wmean$n_cases/wmean$n_sample_size) / (wmean$cases/wmean$sample_size)
wmean$ratio_se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))
wmean <- subset(wmean, standard_error > 0 )
wmean <- subset(wmean, mean != 0 )
wmean <- subset(wmean, n_mean != 0 )


#CONVERT THE RATIO AND STANDARD ERROR TO LOG SPACE
wmean$log_ratio <- log(wmean$ratio)
wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
  ratio_i <- wmean[i, "ratio"]
  ratio_se_i <- wmean[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#SUMMARIZE RESULTS 
mod <- rma(yi=log_ratio, sei=delta_log_se, data=wmean, measure="RR")
results2 <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))  
results2


#WRITE OUTPUT TO CSV  
write.csv(wmean2, "FILEPATH", row.names = F)
write.csv(results2, "FILEPATH", row.names = F)


#######################################################################################################################
# Lastly, adjust Alt 3: between study matches
location_match <- "exact"
year_range <-5       #SPECIFY YEAR RANGE FOR BETWEEN STUDY COMPARISON
covariate_name <- "cv_chart_review"   #set covariate of interest for crosswalk (alt)


#CREATE COVARIATES AND TAG REFERENCE vs. ALTERNATIVE
df <- as.data.table(read.csv("FILEPATH"))
df$cv_stringent <- ifelse((df$cv_chart_review==1 | cv_diag_exam==1) ,1,0)
df = within(df, {cv_hospital = ifelse( clinical_data_type=="inpatient" | location_name=="Taiwan", 1, 0)})

df$is_reference <- ifelse((df$cv_hospital==1),1,0)
df$is_alt <-ifelse(df$cv_stringent==1,1,0)


## FILL OUT MEAN/CASES/SAMPLE SIZE 
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS 
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

# BACK CALCULATE CASES AND SAMPLE SIZE FROM SE
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)

##AGGREGATE JAPAN SUBNATIONALS FOR MATCHING
df$cv_japan <- ifelse((df$nid==336851 | df$nid==336852),1,0)
aggregate_japan <- function(mark_dt){
  dt <- copy(mark_dt)
  marketscan_dt <- copy(dt[cv_japan==1])
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 67, location_name = "Japan", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt)
  return(full_dt)
}

df <- aggregate_japan(df)


# REMOVE OUTLIER ROWS
df <- df[!is_outlier == 1 & measure %in% c("prevalence")] 


# CREATE A WORKING DATAFRAME
wdf <- df[,c("measure", "location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt")]
covs <- select(df, matches("^cv_"))
wdf <- cbind(wdf, covs)
wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")


#CREATE INDICATORS FOR MERGING
if(location_match=="exact"){
  wdf$location_match <- wdf$location_id
} else if(location_match=="country"){
  wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdf$location_match <- wdf$region_name
} else if(location_match=="super"){
  wdf$location_match <- wdf$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)

#EXACT AGE MATCH
wdf$age_start_match <- wdf$age_start
wdf$age_end_match <- wdf$age_end

#MID-POINT AGE 
wdf$age_mid = (wdf$age_start + wdf$age_end)/2

#MID-YEAR CALCULATION
wdf$mid_year <- (wdf$year_start + wdf$year_end)/2


#COLLAPSE TO THE DESIRED MERGING
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)

#MERGE AND MATCH REFERENCE AND ALTERNATIVE WITHIN SAME STUDIES
setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "mid_year", "age_mid"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_mid_year", "n_age_mid"))
wmean <- merge(ref, nref, by=c("location_match", "measure"), allow.cartesian = T)
wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x",  "ihme_loc_abv.x", "ihme_loc_abv.y", "location_id.x", "location_id.y")] <- NULL
wmean <- subset(wmean, nid.x!=nid.y)
wmean <- wmean[abs(wmean$mid_year - wmean$n_mid_year) <= year_range, ] #set this to AND instead of OR to be stricter

#Drop rows if sexes are not the same for sex and n_sex in a given row - then combine sex.x and sex.y into one sex column
wmean <- wmean[wmean$sex.x==wmean$sex.y, ]
wmean$sex <- wmean$sex.x
wmean[, c("sex.x", "sex.y")] <- NULL

#DROP DUPLICATES
wmean <- unique(wmean)

#AGGREGATE JAPAN
aggregate_japan_by_age <- function(mark_dt){
  dt <- copy(mark_dt)
  marketscan_dt <- copy(dt[cv_japan.y==1])
  marketscan_dt$age_match[(marketscan_dt$age_start.y >= marketscan_dt$age_start.x) ] <-1 
  marketscan_dt <- subset(marketscan_dt, age_match==1)
  marketscan_dt[, id := .GRP, by = c("age_start.x", "age_end.x")]
  
  by_vars <- c("id", "n_year_start", "sex")
  marketscan_dt[, `:=` (n_cases = sum(n_cases), n_sample_size = sum(n_sample_size)), by = by_vars]
  marketscan_dt[, `:=` (n_mean = n_cases/n_sample_size, lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(n_standard_error) & measure == "prevalence", n_standard_error := sqrt(n_mean*(1-n_mean)/n_sample_size + z^2/(4*n_sample_size^2))]
  marketscan_dt[is.na(n_standard_error) & measure == "incidence" & cases < 5, n_standard_error := ((5-n_mean*n_sample_size)/n_sample_size+n_mean*n_sample_size*sqrt(5/n_sample_size^2))/5]
  marketscan_dt[is.na(n_standard_error) & measure == "incidence" & cases >= 5, n_standard_error := sqrt(n_mean/n_sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  return(marketscan_dt)
}

wmean <- aggregate_japan_by_age(wmean)


#GET THE RATIO
wmean$ratio <- (wmean$n_cases/wmean$n_sample_size) / (wmean$cases/wmean$sample_size)
wmean$ratio_se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))
wmean <- subset(wmean, standard_error > 0 )
wmean <- subset(wmean, mean != 0 )
wmean <- subset(wmean, n_mean != 0 )


#CONVERT THE RATIO AND STANDARD ERROR TO LOG SPACE
wmean$log_ratio <- log(wmean$ratio)
wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
  ratio_i <- wmean[i, "ratio"]
  ratio_se_i <- wmean[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

#SUMMARIZE RESULTS 
mod <- rma(yi=log_ratio, sei=delta_log_se, data=wmean, measure="RR")
results3 <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))  
results3


#WRITE OUTPUT TO CSV  
write.csv(wmean3, "FILEPATH", row.names = F)
write.csv(results3, "FILEPATH", row.names = F)

######################################################################################################################
#STEP 2: DATA PREP FOR META-REGRESSION MODEL: NETWORK ANALYSIS 
######################################################################################################################
main_dir <- "FILEPATH"

df1 <- as.data.table(wmean1)
df2 <- as.data.table(wmean2)
df3 <- as.data.table(wmean3)

#CREATE FUNCTION TO ORDER COLUMNS OF CROSSWALK CSV FILES CONSISTENTLY AND DROP UN-NEEDED COLUMNS
  df_vector <- list(df1,df2)                                                                                  
  
  reorder_columns <- function(datasheet){
    ## set ordered list of columns for master crosswalk csv
    template_cols <- c("location_match", "age_start_match", "age_end_match", "sex", "log_ratio", "delta_log_se", 
                       "cv_marketscan.y", "cv_ms2000.y", "cv_chart_review.y",
                       "nid.x", "year_start", "year_end", "nid.y", "n_year_start", "n_year_end")
  
    col_order <- template_cols
    ## find which column names are in the extraction template but not in your datasheet
    to_fill_blank <- c()  
    for(column in col_order){
      if(!column %in% names(datasheet)){
        to_fill_blank <- c(to_fill_blank, column)
      }}
    ## create blank column which will be filled in for columns not in your datasheet
    len <- length(datasheet$nid.x)
    blank_col <- rep.int(NA,len)
    
    ## for each column not found in your datasheet, add a blank column and rename it appropriately
    for(column in to_fill_blank){
      datasheet <- cbind(datasheet,blank_col)
      names(datasheet)[names(datasheet)=="blank_col"]<-column
    }
    ## for columns in datasheet but not in epi template or cv list, delete
    dt_cols <- names(datasheet)
    datasheet <- as.data.table(datasheet)
    for(col in dt_cols){
      if(!(col %in% col_order)){
        datasheet[, c(col):=NULL]
      }}
    ## reorder columns with template columns
    setcolorder(datasheet, col_order)
    ## return
    return(datasheet)   
  }    

#CREATE MASTER FILES WITH ALL CROSSWALKS FOR CAUSE, REMOVE DUPLICATE ROWS, WRITE CSV
  master_xwalk <- lapply(df_vector, reorder_columns) %>% rbindlist()
  master_xwalk <- unique(master_xwalk)
  master_xwalk[, id := .GRP, by = c("nid.x", "nid.y")]
  write.csv(master_xwalk, "FILEPATH", row.names = F)


#########################################################################################################################################
#STEP 3: DATA PREP TO RUN MR-BRT AND APPLY PREDICTION TO THE ORIGINAL DATA 
#########################################################################################################################################

# Load original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables
  dat_original <- as.data.table(read.csv("FILEPATH FOR SEX SPLIT DATA"))

#SET YOUR REFERENCE IF ALREADY NOT DONE
  dat_original = within(dat_original, {cv_ms2000 = ifelse((year_start==2000 & clinical_data_type=="claims" & location_name!="Taiwan"), 1, 0)})
  dat_original = within(dat_original, {cv_marketscan = ifelse((year_start!=2000 & clinical_data_type=="claims" & location_name!="Taiwan"), 1, 0)})
  dat_original = within(dat_original, {cv_hospital = ifelse( clinical_data_type=="inpatient" | location_name=="Taiwan", 1, 0)})
  dat_original$cv_stringent <- ifelse((dat_original$cv_chart_review==1 | dat_original$cv_diag_exam==1),1,0)
  
  dat_original$is_reference <- ifelse((dat_original$cv_hospital==1),1,0)

  reference_var <- "is_reference"
  reference_value <- 1
  mean_var <- "mean"
  se_var <- "standard_error"
  cov_names <- c("cv_ms2000", "cv_marketscan", "cv_chart_review") 
  

# Load data for meta-regression model 
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as alternative/reference
  dat_metareg <- copy(master_xwalk) %>%
    mutate(cv_chart_review = if_else(cv_chart_review.y == reference_value, 1, 0, 0), cv_ms2000 = if_else(cv_ms2000.y == reference_value, 1, 0, 0), cv_marketscan = if_else(cv_marketscan.y == reference_value, 1, 0, 0))
  
#STANDARDIZE VARIABLE NAMES IN THE ORIGINAL EXTRACTED DATASHEET
  orig_vars <- c(mean_var, se_var, reference_var, cov_names)
  tmp_orig <- as.data.frame(dat_original) %>%
    .[, orig_vars] %>%
    setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
    mutate(ref = if_else(ref == reference_value, 1, 0, 0))    ##CREATE A SUBSET OF DATASETS THAT ONLY CONTAIN MEAN SE REF AND COVARIATES OF INTEREST


#Log transform the original data
#-- SEs transformed using the delta method
  tmp_orig$mean_log <- log(tmp_orig$mean)
  tmp_orig$se_log <- sapply(1:nrow(tmp_orig), function(i) {
    mean_i <- tmp_orig[i, "mean"]
    se_i <- tmp_orig[i, "standard_error"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })


#########################################################################################################################################
#STEP 4: FIT THE MR-BRT MODEL 
#########################################################################################################################################

  repo_dir <- "FILEPATH"
  source(paste0(repo_dir, "run_mr_brt_function.R"))
  source(paste0(repo_dir, "cov_info_function.R"))
  source(paste0(repo_dir, "check_for_outputs_function.R"))
  source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
  source(paste0(repo_dir, "predict_mr_brt_function.R"))
  source(paste0(repo_dir, "check_for_preds_function.R"))
  source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
  
  
  covariate_name <- "_direct"    #set covariate of interest for crosswalk (alt), used for the model name
  trim <- 0.10 #this is used for the model name; actual trimming must be specified within the run_mr_brt function below
  
  #Run MR-BRT
  fit1 <- run_mr_brt(
    output_dir =  "FILEPATH",
    model_label = paste0(cause_name, covariate_name, trim),
    data = tmp_metareg,
    overwrite_previous = TRUE,
    remove_x_intercept = TRUE,
    study_id = "id",
    method = "trim_maxL",
    trim_pct = trim, 
    mean_var = "log_ratio",
    se_var = "delta_log_se",
    covs = list(  
      cov_info("cv_ms2000", "X"),
      cov_info("cv_marketscan", "X"),
      cov_info("cv_chart_review", "X" )
    ))
  
  
  check_for_outputs(fit1)


#########################################################################################################################################
#STEP 5: CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA 
#########################################################################################################################################
  eval(parse(text = paste0("predicted <- expand.grid(", paste0(cov_names, "=c(0, 1)", collapse = ", "), ")")))
  predicted <- as.data.table(predict_mr_brt(fit1, newdata = predicted)["model_summaries"])
  
  names(predicted) <- gsub("model_summaries.", "", names(predicted))
  names(predicted) <- gsub("X_d_", "cv_", names(predicted))
  predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
  crosswalk_reporting <- copy(predicted) 
  predicted <- predicted %>%
    mutate(
      cv_ms2000 = X_cv_ms2000,
      cv_marketscan = X_cv_marketscan, 
      cv_chart_review = X_cv_chart_review)
  predicted <-as.data.table(predicted)
  
  predicted[, (c("X_cv_marketscan", "X_cv_ms2000","X_cv_chart_review", "Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
  openxlsx::write.xlsx(predicted, "FILEPATH", row.names = F)

  cov_names <- c("cv_ms2000", "cv_marketscan", "cv_stringent") 
  
  review_sheet_final <- merge(tmp_orig, predicted, by=cov_names)
  review_sheet_final <-as.data.table(review_sheet_final)
  
  review_sheet_final[, `:=` (log_mean = log(mean), log_se = deltamethod(~log(x1), mean, standard_error^2)), by = c("mean", "standard_error")]
  review_sheet_final[Y_mean != predicted[1,Y_mean], `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
  review_sheet_final[Y_mean != predicted[1,Y_mean], `:=` (mean = exp(log_mean), standard_error = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
  review_sheet_final[Y_mean != predicted[1,Y_mean], `:=` (cases = NA, lower = NA, upper = NA)]

  review_sheet_final[, (c("Y_mean", "Y_se", "log_mean", "log_se")) := NULL]
  review_sheet_final[is.na(lower), uncertainty_type_value := NA]
  review_sheet_final$crosswalk_parent_seq <- review_sheet_final$seq

  
  #APPEND PREV AND INC ADJUSTED DATASETS
  review_sheet_final <- rbind.fill(review_sheet_final, review_sheet_final_inc)
  
  #THIS IS THE DATASET THAT WILL BE USED FOR NONFATAL MODELING
  openxlsx::write.xlsx(review_sheet_final, paste0("FILEPATH"),sheetName="extraction", row.names = F)


