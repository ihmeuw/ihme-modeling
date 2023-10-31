####################################################################################################
## ENV_XWALKS_BY_SEX.R
## Script to crosswalk survey data to inpatient data for utilization envelope, updated for GBD 2020
## to use central crosswalking R package 
####################################################################################################

rm(list=ls()) 

library(mgcv)
library(data.table)
library(readr)
library(ggplot2)
library(ggpubr)
library(dplyr)
library(crosswalk, lib.loc="FILEPATH")
library(Hmisc)

## ----------------------------------------------------------------------------------------------------
## SETUP
date <- gsub("-", "_", Sys.Date())
user <- Sys.info()[7][[1]]
root_j <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
root_h <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", paste0("FILEPATH", user))

## Source functions needed
setwd(paste0("FILEPATH")) 
source("get_covariate_estimates.R")
source("get_location_metadata.R")

## Set directories to read from and write to
read_dir <- paste0(root_j, "FILEPATH")
write_dir <- paste0(root_j, "FILEPATH")

## Read in the data
avg_12 <- read.csv(paste0(read_dir, "FILEPATH"), stringsAsFactors = FALSE)
frac_1 <- read.csv(paste0(read_dir, "FILEPATH"), stringsAsFactors = FALSE)
frac_12 <- read.csv(paste0(read_dir, "FILEPATH"), stringsAsFactors = FALSE)
admin <- read.csv(paste0(read_dir, "FILEPATH"), stringsAsFactors = FALSE)

## Bind frac_1, frac_12, appropriately set up all data
fraction <- rbind(frac_1, frac_12)
fraction$cv_mics <- ifelse(is.na(fraction$cv_mics), 0, fraction$cv_mics)
fraction <- fraction[fraction$is_outlier==0,]
## Re-subset frac_1, frac-2
frac_12 <- fraction[fraction$cv_12_month_recall==1,]
frac_1 <- fraction[fraction$cv_1_month_recall==1,]
## Appropriately set up other data types
admin <- admin[admin$is_outlier==0,]
avg_12 <- avg_12[avg_12$is_outlier==0,]

## ----------------------------------------------------------------------------------------------------
## COVARIATES
## Get hosp_beds_per1000
hosp <- get_covariate_estimates(50, gbd_round_id=7, decomp_step="iterative")
hosp$log_beds <- log(hosp$mean_value) # rename
hosp <- hosp[,c("year_id", "location_id", "log_beds")] # keep necessary columns

# Get LDI
ldi <- get_covariate_estimates(57, gbd_round_id=7, decomp_step="iterative")
ldi$log_ldi <- log(ldi$mean_value) # rename
ldi <- ldi[,c("year_id", "location_id", "log_ldi")] # keep necessary columns

## Merge covariate info onto survey dfs
avg_12 <- merge(avg_12, ldi, by.x=c("year_start", "location_id"), by.y=c("year_id","location_id"))
avg_12 <- merge(avg_12, hosp, by.x=c("year_start", "location_id"), by.y=c("year_id","location_id"))

frac_12 <- merge(frac_12, ldi, by.x=c("year_start", "location_id"), by.y=c("year_id","location_id"))
frac_12 <- merge(frac_12, hosp, by.x=c("year_start", "location_id"), by.y=c("year_id","location_id"))

frac_1 <- merge(frac_1, ldi, by.x=c("year_start", "location_id"), by.y=c("year_id","location_id"))
frac_1 <- merge(frac_1, hosp, by.x=c("year_start", "location_id"), by.y=c("year_id","location_id"))


## ----------------------------------------------------------------------------------------------------
## ASSORTED DF CLEANUP BEFORE XWALK
frac_1$age_end <- ifelse(frac_1$age_end == "0.999", 1, frac_1$age_end)
frac_12$age_end <- ifelse(frac_12$age_end == "0.999", 1, frac_12$age_end)
avg_12$age_end <- ifelse(avg_12$age_end == "0.999", 1, avg_12$age_end)
admin$age_end <- ifelse(admin$age_end == "0.999", 1, admin$age_end)

## Get age midpoint
avg_12$mid_age <- (avg_12$age_start + avg_12$age_end)/2
frac_12$mid_age <- (frac_12$age_start + frac_12$age_end)/2
frac_1$mid_age <- (frac_1$age_start + frac_1$age_end)/2

## Functions to prep data
year_rounder <-function(df){
  df$year_round <- df$age_end
  df$year_round <- round(df$year_round/3,0)
  df$year_round <- df$year_round*3
  
  return(df)
}

## adjust_labels 
adjust_labels <- function(df){
  df$mean_2 <- df$mean
  df$standard_error_2 <- df$standard_error
  return(df)
}

## Create "dorms"
avg_12$dorm_alt <- "12_mo_survey_avg"
frac_12$dorm_alt <- "12_mo_survey_frac"
frac_1$dorm_alt <- "1_mo_survey_frac"
admin$dorm_ref <- "hosp_data"

## Split survey data types into M/F
avg_12_m <- subset(avg_12, sex=="Male")
avg_12_f <- subset(avg_12, sex=="Female")

frac_12_m <- subset(frac_12, sex=="Male")
frac_12_f <- subset(frac_12, sex=="Female")

frac_1_m <- subset(frac_1, sex=="Male")
frac_1_f <- subset(frac_1, sex=="Female")

## Use year_rounder 
avg_12_pred_m <- year_rounder(avg_12_m)
avg_12_pred_f <- year_rounder(avg_12_f)

frac_12_pred_m <- year_rounder(frac_12_m)
frac_12_pred_f <- year_rounder(frac_12_f)

frac_1_pred_m <- year_rounder(frac_1_m)
frac_1_pred_f <- year_rounder(frac_1_f)

## Adjust labels 
avg_12_pred_m <- adjust_labels(avg_12_pred_m)
avg_12_pred_f <- adjust_labels(avg_12_pred_f)

frac_12_pred_m <- adjust_labels(frac_12_pred_m)
frac_12_pred_f <- adjust_labels(frac_12_pred_f)

frac_1_pred_m <- adjust_labels(frac_1_pred_m)
frac_1_pred_f <- adjust_labels(frac_1_pred_f)

## Copy version of admin to use to re-bind later
admin_df <- admin

## Subset admin for merge
admin <- admin[, c("mean","standard_error","age_start","age_end","year_start","location_id","sex","dorm_ref")]

## Split admin into M/F
admin_m <- subset(admin, sex=="Male")
admin_f <- subset(admin, sex=="Female")

## Name columns to merge on
cols <- c("age_start", "age_end", "year_start",  "location_id", "sex")

## Create matched pairs - merge admin data with each type of survey data where possible
m_avg_12_match <- merge(admin, avg_12_pred_m[,c("mean_2", "standard_error_2", "mid_age", "dorm_alt", "log_beds","log_ldi", cols)], by=cols) 
f_avg_12_match <- merge(admin, avg_12_pred_f[,c("mean_2", "standard_error_2", "mid_age", "dorm_alt", "log_beds","log_ldi", cols)], by=cols) 

m_frac_12_match <- merge(admin, frac_12_pred_m[, c("mean_2", "standard_error_2","mid_age", "dorm_alt", "log_beds","log_ldi", cols)], by=cols)
f_frac_12_match <- merge(admin, frac_12_pred_f[, c("mean_2", "standard_error_2","mid_age", "dorm_alt", "log_beds","log_ldi", cols)], by=cols)

m_frac_1_match <- merge(admin, frac_1_pred_m[, c("mean_2", "standard_error_2", "mid_age", "dorm_alt", "log_beds","log_ldi", cols)], by=cols)
f_frac_1_match <- merge(admin, frac_1_pred_f[, c("mean_2", "standard_error_2", "mid_age", "dorm_alt", "log_beds","log_ldi", cols)], by=cols)

## Define id for crosswalks
m_avg_12_match$id <- 1
f_avg_12_match$id <- 1

m_frac_12_match$id <- 1
f_frac_12_match$id <- 1

m_frac_1_match$id <- 1
f_frac_1_match$id <- 1

## ----------------------------------------------------------------------------------------------------
## DO AVG_12 CROSSWALKS
## MALE
dat_diff_avg_12_m <- as.data.frame(cbind(
  delta_transform(
    mean = m_avg_12_match$mean_2,
    sd = m_avg_12_match$standard_error_2,
    transformation = "linear_to_log"),
  delta_transform(
    mean = m_avg_12_match$mean,
    sd = m_avg_12_match$standard_error,
    transformation = "linear_to_log")
))

names(dat_diff_avg_12_m) <- c("mean_alt","mean_se_alt","mean_ref","mean_se_ref")

m_avg_12_match[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff_avg_12_m,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref")

m_avg_12_xwalk_df <- CWData(
  df = m_avg_12_match,
  obs = "log_diff",
  obs_se = "log_diff_se",
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  covs = list("mid_age","log_beds","log_ldi"),
  study_id = "id"
)

m_fit_avg_12_xwalk <- CWModel(
  cwdata = m_avg_12_xwalk_df,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name="intercept"),
    CovModel(cov_name="mid_age", spline=XSpline(knots=c(5,15,45,75), degree=3L, l_linear=TRUE, r_linear=TRUE)),
    CovModel(cov_name="log_beds"),
    CovModel(cov_name="log_ldi")),
  gold_dorm = "hosp_data"
)

avg_12_m[, c("mean_adj","se_adj","diff","diff_se")] <- adjust_orig_vals(
  fit_object = m_fit_avg_12_xwalk,
  df = avg_12_m,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

## FEMALE
dat_diff_avg_12_f <- as.data.frame(cbind(
  delta_transform(
    mean = f_avg_12_match$mean_2,
    sd = f_avg_12_match$standard_error_2,
    transformation = "linear_to_log"),
  delta_transform(
    mean = f_avg_12_match$mean,
    sd = f_avg_12_match$standard_error,
    transformation = "linear_to_log")
))

names(dat_diff_avg_12_f) <- c("mean_alt","mean_se_alt","mean_ref","mean_se_ref")

f_avg_12_match[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff_avg_12_f,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref")

f_avg_12_xwalk_df <- CWData(
  df = f_avg_12_match,
  obs = "log_diff",
  obs_se = "log_diff_se",
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  covs = list("mid_age","log_beds","log_ldi"),
  study_id = "id"
)

f_fit_avg_12_xwalk <- CWModel(
  cwdata = f_avg_12_xwalk_df,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name="intercept"),
    CovModel(cov_name="mid_age", spline=XSpline(knots=c(5,15,45,75), degree=3L, l_linear=TRUE, r_linear=TRUE)),
    CovModel(cov_name="log_beds"),
    CovModel(cov_name="log_ldi")),
  gold_dorm = "hosp_data"
)

avg_12_f[, c("mean_adj","se_adj","diff","diff_se")] <- adjust_orig_vals(
  fit_object = f_fit_avg_12_xwalk,
  df = avg_12_f,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

## RBIND M/F AVG_12
avg_12_final <- rbind(avg_12_f, avg_12_m)


## ----------------------------------------------------------------------------------------------------
## DO FRAC_12 CROSSWALKS
## MALE
dat_diff_frac_12_m <- as.data.frame(cbind(
  delta_transform(
    mean = m_frac_12_match$mean_2,
    sd = m_frac_12_match$standard_error_2,
    transformation = "linear_to_log"),
  delta_transform(
    mean = m_frac_12_match$mean,
    sd = m_frac_12_match$standard_error,
    transformation = "linear_to_log")
))

names(dat_diff_frac_12_m) <- c("mean_alt","mean_se_alt","mean_ref","mean_se_ref")

m_frac_12_match[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff_frac_12_m,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref")

m_frac_12_xwalk_df <- CWData(
  df = m_frac_12_match,
  obs = "log_diff",
  obs_se = "log_diff_se",
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  covs = list("mid_age","log_beds","log_ldi"),
  study_id = "id"
)

m_fit_frac_12_xwalk <- CWModel(
  cwdata = m_frac_12_xwalk_df,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name="intercept"),
    CovModel(cov_name="mid_age", spline=XSpline(knots=c(5,15,45,75), degree=3L, l_linear=TRUE, r_linear=TRUE)),
    CovModel(cov_name="log_beds"),
    CovModel(cov_name="log_ldi")),
  gold_dorm = "hosp_data"
)

frac_12_m[, c("mean_adj","se_adj","diff","diff_se")] <- adjust_orig_vals(
  fit_object = m_fit_frac_12_xwalk,
  df = frac_12_m,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

## FEMALE
dat_diff_frac_12_f <- as.data.frame(cbind(
  delta_transform(
    mean = f_frac_12_match$mean_2,
    sd = f_frac_12_match$standard_error_2,
    transformation = "linear_to_log"),
  delta_transform(
    mean = f_frac_12_match$mean,
    sd = f_frac_12_match$standard_error,
    transformation = "linear_to_log")
))

names(dat_diff_frac_12_f) <- c("mean_alt","mean_se_alt","mean_ref","mean_se_ref")

f_frac_12_match[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff_frac_12_f,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref")

f_frac_12_xwalk_df <- CWData(
  df = f_frac_12_match,
  obs = "log_diff",
  obs_se = "log_diff_se",
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  covs = list("mid_age","log_beds","log_ldi"),
  study_id = "id"
)

f_fit_frac_12_xwalk <- CWModel(
  cwdata = f_frac_12_xwalk_df,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name="intercept"),
    CovModel(cov_name="mid_age", spline=XSpline(knots=c(5,15,45,75), degree=3L, l_linear=TRUE, r_linear=TRUE)),
    CovModel(cov_name="log_beds"),
    CovModel(cov_name="log_ldi")),
  gold_dorm = "hosp_data"
)

frac_12_f[, c("mean_adj","se_adj","diff","diff_se")] <- adjust_orig_vals(
  fit_object = f_fit_frac_12_xwalk,
  df = frac_12_f,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

## RBIND M/F FRAC_12
frac_12_final <- rbind(frac_12_f, frac_12_m)

## ----------------------------------------------------------------------------------------------------
## DO FRAC_1 CROSSWALKS
## MALE
dat_diff_frac_1_m <- as.data.frame(cbind(
  delta_transform(
    mean = m_frac_1_match$mean_2,
    sd = m_frac_1_match$standard_error_2,
    transformation = "linear_to_log"),
  delta_transform(
    mean = m_frac_1_match$mean,
    sd = m_frac_1_match$standard_error,
    transformation = "linear_to_log")
))

names(dat_diff_frac_1_m) <- c("mean_alt","mean_se_alt","mean_ref","mean_se_ref")

m_frac_1_match[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff_frac_1_m,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref")

m_frac_1_xwalk_df <- CWData(
  df = m_frac_1_match,
  obs = "log_diff",
  obs_se = "log_diff_se",
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  covs = list("mid_age","log_beds","log_ldi"),
  study_id = "id"
)

m_fit_frac_1_xwalk <- CWModel(
  cwdata = m_frac_1_xwalk_df,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name="intercept"),
    CovModel(cov_name="mid_age", spline=XSpline(knots=c(5,15,45,75), degree=3L, l_linear=TRUE, r_linear=TRUE)),
    CovModel(cov_name="log_beds"),
    CovModel(cov_name="log_ldi")),
  gold_dorm = "hosp_data"
)

frac_1_m[, c("mean_adj","se_adj","diff","diff_se")] <- adjust_orig_vals(
  fit_object = m_fit_frac_1_xwalk,
  df = frac_1_m,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

## FEMALE
dat_diff_frac_1_f <- as.data.frame(cbind(
  delta_transform(
    mean = f_frac_1_match$mean_2,
    sd = f_frac_1_match$standard_error_2,
    transformation = "linear_to_log"),
  delta_transform(
    mean = f_frac_1_match$mean,
    sd = f_frac_1_match$standard_error,
    transformation = "linear_to_log")
))

names(dat_diff_frac_1_f) <- c("mean_alt","mean_se_alt","mean_ref","mean_se_ref")

f_frac_1_match[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff_frac_1_f,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref")

f_frac_1_xwalk_df <- CWData(
  df = f_frac_1_match,
  obs = "log_diff",
  obs_se = "log_diff_se",
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  covs = list("mid_age","log_beds","log_ldi"),
  study_id = "id"
)

f_fit_frac_1_xwalk <- CWModel(
  cwdata = f_frac_1_xwalk_df,
  obs_type = "diff_log",
  cov_models = list(
    CovModel(cov_name="intercept"),
    CovModel(cov_name="mid_age", spline=XSpline(knots=c(5,15,45,75), degree=3L, l_linear=TRUE, r_linear=TRUE)),
    CovModel(cov_name="log_beds"),
    CovModel(cov_name="log_ldi")),
  gold_dorm = "hosp_data"
)

frac_1_f[, c("mean_adj","se_adj","diff","diff_se")] <- adjust_orig_vals(
  fit_object = f_fit_frac_1_xwalk,
  df = frac_1_f,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

## RBIND M/F
frac_1_final <- rbind(frac_1_f, frac_1_m)

## ----------------------------------------------------------------------------------------------------
# RE-BIND ALL DATA TOGETHER
## Bind xwalked dfs
xwalked <- rbind(frac_1_final, frac_12_final, avg_12_final)

## Save adjusted (xwalked) means as mean_final, se_final
xwalked$mean_final <- xwalked$mean_adj
xwalked$se_final <- xwalked$se_adj

## Remove columns not needed going forward
xwalked$log_beds <- NULL
xwalked$log_ldi <- NULL
xwalked$dorm_alt <- NULL
xwalked$mean_adj <- NULL
xwalked$se_adj <- NULL
xwalked$mean <- NULL
xwalked$standard_error <- NULL
xwalked$diff <- NULL
xwalked$diff_se <- NULL
xwalked$crosswalk_parent_seq <- xwalked$seq
xwalked$X <- NULL
xwalked$Unnamed..0 <- NULL
xwalked$V1 <- NULL

## Save admin means as mean_final, se_final (to match xwalked)
admin_df$mean_final <- admin_df$mean
admin_df$se_final <- admin_df$standard_error

## Remove columns not needed going forward 
admin_df$dorm_ref <- NULL
admin_df$mean <- NULL
admin_df$standard_error <- NULL
admin_df$Unnamed..0 <- NULL
admin_df$X <- NULL
admin_df$V1 <- NULL

## Add mid_age back to admin
admin_df$mid_age <- (admin_df$age_start + admin_df$age_end)/2
admin_df$crosswalk_parent_seq <- NA

## Re-bind
full_df_post_xwalk <- rbind(xwalked, admin_df)

## ----------------------------------------------------------------------------------------------------
## WRITE TO CSVS - SAVE
## Full dataframe - includes admin data
xwalk_file_name <- "FILEPATH"
write_csv(full_df_post_xwalk, paste0(write_dir, xwalk_file_name))







