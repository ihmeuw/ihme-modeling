###########################################################################
## This file is intended to house all LRI crosswalks.
## First, it takes active survey data in bundle, replaces with
## the raw, un-crosswalked survey data. This has been done already (5-24-19).
## Second, it takes the survey data from GBD 2019 step 1 and performs
## a series of crosswalks for the case definition in the surveys.
## Third, it calculates crosswalk values for literature data that are
## from hospitalized populations and for self-reported data (typically surveys).
## Fourth, it calculates a single crosswalk for all clinical data (mix of inpatient
## and claims data).
## Lastly, it saves a bundle_version and uploads as a crosswalk_version.
###########################################################################

rm(list = ls())

library(metafor)
library(msm)
library(plyr)
library(boot)
library(ggplot2)
library(openxlsx)
library(readxl)
library(dplyr)
library(magrittr)

source() # filepaths for uploading and downloading functions
source("filepath/plot_mr_brt_function.R")
source("filepath/run_mr_brt_function.R")
source("filepath/bundle_crosswalk_collapse.R")
source("filepath/convert_inc_prev_function.R")
locs <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 7))

eti_info <- read.csv("data_filepath")

age_groups <- get_ids("age_group")

gbd_round_id <- 7

`%notin%` <- Negate(`%in%`)

xwvals <- data.frame()

################################################
## STEP 1 -- PULL BUNDLE VERSION AND GET DATA
################################################

## Check for most recent bundle

bundle_meta <- get_bundle_version_ids(cause_id = 322)

## Get bundle version

bundle_version_id <- 32996 ## ----> change to desired bundle version

df_lri <- as.data.table(get_bundle_version(bundle_version_id = bundle_version_id, fetch = "all"))

df_2019 <- as.data.table(get_bundle_version(bundle_version_id = 16325, fetch = "all"))

dt_lri <- as.data.table(df_lri)

## New data nids

n_distinct(dt_lri$nid)
length(unique(dt_lri$nid))

nid_old <- unique(df_2019$nid)
seq_old <- unique(df_2019$seq)
seq_all <- unique(dt_lri$seq)

n_distinct(df_2019$nid)
length(unique(df_2019$nid))

############################################
## STEP 1 -- Add in updated clinical data
############################################

surv_2020 <- as.data.table(read_excel("filepath"))

susenas_nids <- c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)
surv_2020 <- surv_2020[nid %notin% susenas_nids]
table(surv_2020$nid[surv_2020$nid %in% susenas_nids])
surv_2020$representative_name <- ifelse(surv_2020$representative_name=="Subnationally representative only", "Representative for subnational location only", "Nationally and subnationally representative")

surv_2020$mean <- as.numeric(surv_2020$mean)
surv_2020$upper <- as.numeric(surv_2020$upper)
surv_2020$lower <- as.numeric(surv_2020$lower)
surv_2020$standard_error <- as.numeric(surv_2020$standard_error)
surv_2020[is.na(upper), upper := 0]
surv_2020[is.na(lower), lower := 0]
surv_2020[is.na(standard_error), standard_error := 0]

write.xlsx(surv_2020, "filepath", sheetName = "extraction")

################################################################
## STEP 2 -- RECODE NEW DATA, CLINICAL DATA, and GBD ROUND
################################################################

table(dt_lri$gbd_round)

# count number of distinct seqs in new data

n_distinct(dt_new$seq)

# create list of new seqs; should equal the same as above distinct count

nid_2019 <- unique(df_2019$nid)
seq_2019 <- unique(df_2019$seq)

# recode gbd_round to include new data, where gbd_round == 2020 for new seqs

dt_lri$gbd_round <- ifelse(dt_lri$seq %in% seq_2019, dt_lri$gbd_round, 2020)

# check count of new values; should equal the same as seq_new

table(dt_lri$gbd_round)

# recode gbd_2020_new

dt_lri$gbd_2020_new <- ifelse(dt_lri$gbd_round == 2020,1,0)
dt_lri[is.na(gbd_2020_new), gbd_2020_new := 0]

table(dt_lri$gbd_2020_new)

## CLINICAL recode ##

table(dt_lri$clinical_data_type)

dt_lri <- dt_lri[clinical_data_type == "inpatient", cv_inpatient := 1]
dt_lri <- dt_lri[clinical_data_type == "claims", cv_marketscan := 1]

# save!

write.csv(dt_lri, "filepath", row.names=F)

dt_lri <- read.csv("filepath")

####################################
## STEP 3 - SURVEY DATA
####################################

df <- read.csv("filepath") 

#df$location_name <- df$location
df <- join(df, locs, by="ihme_loc_id")

## Define ratios of prevalence in various definitions
df$chest_fever_chest <- df$chest_fever / df$chest_symptoms
df$chest_fever_diff_fever <- df$chest_fever / df$diff_fever
df$chest_fever_diff <- df$chest_fever / df$diff_breathing
df$diff_fever_diff <- df$diff_fever / df$diff_breathing
df$chest_diff <- df$chest_symptoms / df$diff_breathing

## Define standard errors for each symptom
for(v in c("chest_fever","chest_symptoms","diff_fever","diff_breathing")){
  prev <- df[,v]
  se <- sqrt(prev * (1-prev) / df$sample_size)
  df[,paste0(v,"_std")] <- se
}

## Define standard error for each ratio
df$chest_fever_chest_se <- sqrt(df$chest_fever^2 / df$chest_symptoms^2 * (df$chest_fever_std^2/df$chest_symptoms^2 + df$chest_symptoms_std^2/df$chest_fever^2))
df$chest_fever_diff_fever_se <- sqrt(df$chest_fever^2 / df$diff_fever^2 * (df$chest_fever_std^2/df$diff_fever^2 + df$diff_fever_std^2/df$chest_fever^2))
df$chest_fever_diff_se <- sqrt(df$chest_fever^2 / df$diff_breathing^2 * (df$chest_fever_std^2/df$diff_breathing^2 + df$diff_breathing_std^2/df$chest_fever^2))
df$chest_diff_se <- sqrt(df$chest_symptoms^2 / df$diff_breathing^2 * (df$chest_symptoms_std^2/df$diff_breathing^2 + df$diff_breathing_std^2/df$chest_symptoms^2))

## The network meta-analysis must be long (currently wide)
## and must have dummies for each of the definitions that are crosswalked.
df_chest_chest <- df[,c("age_year","nid","end_year","location_id","chest_fever_chest","chest_fever_chest_se")]
df_chest_diff_fever <- df[,c("age_year","nid","end_year","location_id","chest_fever_diff_fever","chest_fever_diff_fever_se")]
df_chest_diff <- df[,c("age_year","nid","end_year","location_id","chest_fever_diff","chest_fever_diff_se")]

# rename
colnames(df_chest_chest)[5:6] <- c("ratio","standard_error")
colnames(df_chest_diff_fever)[5:6] <- c("ratio","standard_error")
colnames(df_chest_diff)[5:6] <- c("ratio","standard_error")

# set dummy indicator
df_chest_chest$indictor <- "chest"
df_chest_diff_fever$indictor <- "diff_fever"
df_chest_diff$indictor <- "diff"

cv_survey <- rbind(df_chest_chest, df_chest_diff_fever, df_chest_diff)

# Create dummies for network analysis
cv_survey$cv_chest <- ifelse(cv_survey$indictor=="chest",1,0)
cv_survey$cv_diff_fever <- ifelse(cv_survey$indictor=="diff_fever",1,0)
cv_survey$cv_diff <- ifelse(cv_survey$indictor=="diff",1,0)

# remove zeros and NAs
cv_survey <- subset(cv_survey, ratio>0)
cv_survey <- subset(cv_survey, standard_error!="NaN")

# ratios in log space
cv_survey$log_ratio <- log(cv_survey$ratio)
cv_survey$log_se <- sapply(1:nrow(cv_survey), function(i) {
  ratio_i <- cv_survey[i, "ratio"]
  ratio_se_i <- cv_survey[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

# Save
write.csv(cv_survey, paste0("filepath"), row.names=F)

# Now run in MR-BRT
cov_names <- c("cv_chest","cv_diff_fever","cv_diff")
covs1 <- list()
for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))

fit2 <- run_mr_brt(
  output_dir = paste0("filepath"),
  model_label = "survey_chest_2020_final_4",
  data = cv_survey[cv_survey$cv_chest==1,], 
  mean_var = "log_ratio",
  se_var = "log_se",
  #covs = list(cov_info("age_year","X")),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

fit3 <- run_mr_brt(
  output_dir = paste0("filepath"),
  model_label = "survey_diff_fever_2020_final_4",
  data = cv_survey[cv_survey$cv_diff_fever==1,], 
  mean_var = "log_ratio",
  se_var = "log_se",
  #covs = list(cov_info("age_year","X")),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

fit4 <- run_mr_brt(
  output_dir = paste0("filepath"),
  model_label = "survey_diff_2020_final_4",
  data = cv_survey[cv_survey$cv_diff==1,], 
  mean_var = "log_ratio",
  se_var = "log_se",
  #covs = list(cov_info("age_year","X")),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

df_pred <- data.frame(intercept=1, age_year = c(0,1,2,3,4))
# Chest
pred2 <- predict_mr_brt(fit2, newdata = df_pred)
check_for_preds(pred2)
pred_object <- load_mr_brt_preds(pred2)
preds_chest <- pred_object$model_summaries
# Diff + fever
pred3 <- predict_mr_brt(fit3, newdata = df_pred)
check_for_preds(pred3)
pred_object <- load_mr_brt_preds(pred3)
preds_diff_fever <- pred_object$model_summaries
# Just diff
pred4 <- predict_mr_brt(fit4, newdata = df_pred)
check_for_preds(pred4)
pred_object <- load_mr_brt_preds(pred4)
preds_diff <- pred_object$model_summaries


## Collect outputs
survey_plot <- data.frame(age_start = c(0,1,2,3,4),
                          log_chest_ratio = preds_chest$Y_mean,
                          log_diff_fever_ratio = preds_diff_fever$Y_mean,
                          log_diff_ratio = preds_diff$Y_mean,
                          log_chest_se = (preds_chest$Y_mean_hi - preds_chest$Y_mean_lo)/2/qnorm(0.975),
                          log_diff_fever_se = (preds_diff_fever$Y_mean_hi - preds_diff_fever$Y_mean_lo)/2/qnorm(0.975),
                          log_diff_se = (preds_diff$Y_mean_hi - preds_diff$Y_mean_lo)/2/qnorm(0.975))

fit2$model_coefs$crosswalk <- "cv_chest"
fit3$model_coefs$crosswalk <- "cv_diff_fever"
fit4$model_coefs$crosswalk <- "cv_diff"
survey_outputs <- rbind(fit2$model_coefs, fit3$model_coefs, fit4$model_coefs)

# beta_soln +/- 1.96 * sqrt(beta_var + gamma_soln)

ggplot(cv_survey, aes(x=factor(age_year), y=log_ratio, col=indictor)) + geom_boxplot(position=position_dodge())

lplot <- ggplot(survey_plot, aes(x=age_start)) +
  geom_line(aes(y=log_chest_ratio), col="purple") + geom_ribbon(aes(ymin=log_chest_ratio - log_chest_se*1.96, ymax=log_chest_ratio + log_chest_se*1.96), alpha=0.2, fill="purple") +
  geom_line(aes(y=log_diff_fever_ratio), col="blue") + geom_ribbon(aes(ymin=log_diff_fever_ratio - log_diff_fever_se*1.96, ymax=log_diff_fever_ratio + log_diff_fever_se*1.96), alpha=0.2, fill="blue") +
  geom_line(aes(y=log_diff_ratio), col="green") + theme_bw() + geom_ribbon(aes(ymin=log_diff_ratio - log_diff_se*1.96, ymax=log_diff_ratio + log_diff_se*1.96), alpha=0.2, fill="green") +
  geom_hline(yintercept=0, lty=2)
lplot


tmp_df <- data.frame(crosswalk=c("Chest symptoms","Difficulty breathing and fever","Difficulty breathing"),
                     ratio = c(survey_plot$log_chest_ratio[1], survey_plot$log_diff_fever_ratio[1], survey_plot$log_diff_ratio[1]),
                     se = c(survey_plot$log_chest_se[1], survey_plot$log_diff_fever_se[1], survey_plot$log_diff_se[1]))
tmp_df$crosswalk <- factor(tmp_df$crosswalk, rev(c("Chest symptoms","Difficulty breathing and fever","Difficulty breathing")))
pplot <- ggplot(tmp_df, aes(x=crosswalk ,y=ratio)) + geom_point() + geom_errorbar(aes(ymin = ratio - se*1.96, ymax = ratio + se*1.96), width=0) + theme_bw() +
  coord_flip() + xlab("") + ylab("Log ratio") + geom_hline(yintercept=0, lty=2)
pplot

###################################################################
## Pull in all the survey data, merge, and make data adjustments ##
###################################################################

period <- read.csv("filepath")

# Append new data

table(period$had_fever_survey)

period$had_fever_survey <- ifelse(period$gbd_2020_new == 1 & period$cv_diag_selfreport == 1, 1, period$had_fever_survey)

table(is.na(period$had_fever_survey))
table(period$had_fever_survey)
table(period$cv_diag_selfreport)

period$cv_diag_selfreport[is.na(period$cv_diag_selfreport)] <- 0
period <- subset(period, cv_diag_selfreport==1)
period <- subset(period, !is.na(had_fever_survey))

period$mean_original <- period$mean
period$standard_error_original <- period$standard_error

period$log_mean <- log(period$mean)
period$log_mean_original <- period$log_mean
period$cv_had_fever <- period$had_fever_survey
period$cv_no_fever <- (1-period$had_fever_survey)
period$indicator <- with(period, ifelse(cv_diag_valid_good==1 & cv_had_fever==1, "Reference", ifelse(cv_diag_valid_good==1 & cv_had_fever==0,"Chest only",
                                                                                                     ifelse(cv_diag_valid_good==0 & cv_had_fever==1,"Difficulty and fever",
                                                                                                            "Difficulty only"))))

pcomp <- join(period, survey_plot, by=c("age_start"))

period[is.na(cv_diag_valid_poor), cv_diag_valid_poor := 0]

#########################################################
## ADJUST MEANS BY THE SURVEY RATIOS
#########################################################

pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==1, ifelse(pcomp$cv_had_fever==0, pcomp$log_mean + pcomp$log_chest_ratio, pcomp$log_mean), pcomp$log_mean)
pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==0, ifelse(pcomp$cv_had_fever==1, pcomp$log_mean + pcomp$log_diff_fever_ratio, pcomp$log_mean), pcomp$log_mean)
pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==0, ifelse(pcomp$cv_had_fever==0, pcomp$log_mean + pcomp$log_diff_ratio, pcomp$log_mean), pcomp$log_mean)

## Get the linear standard errors ##
log_standard_error <- sapply(1:nrow(pcomp), function(i) {
  ratio_i <- pcomp[i, "mean"]
  ratio_se_i <- pcomp[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})
se_xw_lri2 <- sqrt(log_standard_error^2 + pcomp$log_chest_se^2)
se_xw_lri3 <- sqrt(log_standard_error^2 + pcomp$log_diff_fever_se^2)
se_xw_lri4 <- sqrt(log_standard_error^2 + pcomp$log_diff_se^2)

final_se <- ifelse(pcomp$indicator=="Reference", pcomp$standard_error, ifelse(pcomp$indicator=="Chest only", se_xw_lri2, ifelse(pcomp$indicator=="Difficulty and fever", se_xw_lri3, se_xw_lri4)))
final_se <- sapply(1:nrow(pcomp), function(i) {
  ratio_i <- pcomp[i, "log_mean"]
  ratio_se_i <- final_se[i]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})

pcomp$mean <- exp(pcomp$log_mean)
pcomp$standard_error <- ifelse(pcomp$indicator=="Reference", pcomp$standard_error, final_se)

#setnames(pcomp, c("start_year","end_year"), c("year_start","year_end"))
pcomp$cases <- pcomp$mean * pcomp$sample_size
table(pcomp$cases < 0)

###### Save! ######
write.csv(pcomp, "filepath")

lplot
ggplot(pcomp, aes(x=mean_original, y=mean, col=indicator)) + geom_point() + geom_abline(intercept=0, slope=1) + theme_bw()
ggplot(pcomp, aes(x=standard_error_original, y=(standard_error), col=indicator)) + geom_point() + geom_abline(intercept=0, slope=1) + theme_bw()

#######################################################################
## STEP 4 -- HOSPITAL 
## The purpose of this part of the code is to generate input data sheets for MR-BRT
## for self-reported to clinical definition of LRI
## for the surveys. We will also use this code for prepping sex splitting
## and for determining a ratio scalar for hospital data (not prepared
## by the clinical data team) and for the inpatient data (prepared by clinical team).
## Some of the decisions regarding data processing and how the data
## will be merged come from "estimate_crosswalks_selfreport_hosp.R"
#######################################################################

## get SDI, HAQI as a potential predictor
sdi <- get_covariate_estimates(location_id="all", covariate_id=881, decomp_step="iterative", year_id=1980:2020)
sdi <- join(sdi, locs[,c("location_id","ihme_loc_id")], by="location_id")
sdi$sdi <- sdi$mean_value

haqi <- get_covariate_estimates(location_id="all", covariate_id=1099, decomp_step="iterative", year_id=1980:2020)
haqi <- join(haqi, locs[,c("location_id","ihme_loc_id")], by="location_id")
haqi$haqi <- haqi$mean_value

## Bring in full data set

lri <- read.csv("filepath")

# These data work for the reference and cv_hospital

lri$group_review[is.na(lri$group_review)] <- 1
lri <- subset(lri, is_outlier==0 & lri$group_review==1)
lri$cv_inpatient <- ifelse(lri$clinical_data_type != "",1,0)

lri$age_mid <- floor((lri$age_end + lri$age_start) / 2)

# Hospital and claims data added later
lri <- subset(lri, cv_inpatient == 0)

# Remove survey (self-reported data)
lri <- subset(lri, cv_diag_selfreport == 0)

lri$is_reference <- ifelse(lri$cv_hospital==0 & lri$cv_diag_selfreport==0 & lri$cv_marketscan==0 & lri$cv_inpatient==0 & lri$cv_inpatient_sample==0, 1, 0)
lri[is.na(is_reference), is_reference := 0]

# Join with SDI as it is a predictor for cv_hospital
lri$year_id <- floor((lri$year_start + lri$year_end) / 2)
lri <- join(lri, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
lri$sdi <- round(lri$sdi,2)

## Set some parameters ##
age_bins <- c(0,1,5,20,40,60,80,100)
age_bins <- c(0,1,seq(5,100,5))

## Subset to working data frame ##
df <- as.data.frame(lri[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                           "cv_diag_selfreport","cv_hospital","cv_inpatient","is_reference","group_review","is_outlier")])
df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")

## Some rows don't have sample size and cases, use closed form estimate of SE to get them.
sample_size <- with(df, mean*(1-mean)/standard_error^2)
cases <- df$mean * sample_size
df$cases <- ifelse(is.na(df$cases), cases, df$cases)
df$sample_size <- ifelse(is.na(df$sample_size), sample_size, df$sample_size)

######################################################################
## Hospital data crosswalk. This is for data that report the incidence
## of severe or hospitalized LRI. There are plenty of sources that
## report the incidence of both clinician-diagnosed (reference) and
## hospitalized LRI so we are going to use within study-age-location
## matching for preparing these data for the meta-regression.
######################################################################

cv_hospital <- bundle_crosswalk_collapse(df, covariate_name="cv_hospital", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="within", location_match="exact")

cv_hospital$year_id <- floor((cv_hospital$year_end + cv_hospital$year_start)/2)
cv_hospital$location_id <- cv_hospital$location_match
cv_hospital$age_mid <- floor((cv_hospital$age_end + cv_hospital$age_start)/2)
cv_hospital$num_age_bin <- as.numeric(cv_hospital$age_bin)

cv_hospital <- join(cv_hospital, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
cv_hospital <- join(cv_hospital, locs[,c("location_id","super_region_name","location_name")], by="location_id")
cv_hospital$high_income <- ifelse(cv_hospital$super_region_name=="High-income",1,0)

write.csv(cv_hospital[cv_hospital$ratio>1,], "filepath", row.names=F)

# Test a couple linear models for predictors
sr_hosp <-  ggplot(cv_hospital, aes(x=super_region_name, y=ratio, col=super_region_name)) + geom_boxplot() + 
  theme_bw() + theme(axis.text.x =element_text(angle=90, hjust=1)) + xlab("") + ylab("Ratio") +
  ggtitle("LRI hospital ratio") + scale_color_discrete("")
sr_hosp
age_hosp <- ggplot(cv_hospital, aes(x=age_mid, y=log_ratio)) + geom_point() + stat_smooth(method="loess", se=F) + 
  theme_bw() + xlab("Age midpoint") + ylab("Log ratio") + ggtitle("LRI hospital ratio")
age_hosp

lm_age <- lm(log_ratio ~ age_mid, data=cv_hospital)
summary(lm(log_ratio ~ age_mid, data=cv_hospital))
plot(cv_hospital$age_mid, resid(lm_age))

summary(lm(log_ratio ~ age_mid, data=cv_hospital))
summary(lm(log_ratio ~ sdi^3 + sdi^2 + sdi, data=cv_hospital))
summary(lm(log_ratio ~ high_income + age_mid, data=cv_hospital))


## No covariates
##-------------------------------------------------------------------------------------------------
fit2 <- run_mr_brt(
  output_dir = paste0("filepath"),
  model_label = "cv_hospital_2020_final_4",
  data = cv_hospital,
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  covs = list(cov_info("age_mid","X",
                       degree = 2, n_i_knots = 3,
                       l_linear=TRUE, r_linear = TRUE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf")),
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

fit2$model_coefs$crosswalk <- "cv_hospital"
df_pred <- data.frame(expand.grid(intercept = 1, age_mid= seq(0,100,1)))
pred1 <- predict_mr_brt(fit2, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

# Calculate log se
preds$log_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
preds$hospital_linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "log_se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
preds$hospital_ratio <- exp(preds$Y_mean)
# rename
preds$log_hospital_ratio <- preds$Y_mean
preds$log_hospital_se <- preds$log_se
preds$age_mid <- preds$X_age_mid

mod_data <- fit2$train_data
mod_data <- join(mod_data, locs[,c("location_id","location_name","super_region_name")], by="location_id")

## Look at the age curve ##
mod_data$outlier <- floor(abs(mod_data$w - 1))
hplot <- ggplot(preds, aes(x=X_age_mid, y=(Y_mean))) + geom_line() +
  geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
  geom_point(data=mod_data, aes(x=age_mid, y=log_ratio, size=1/se^2, shape=factor(outlier))) + guides(size=F) +
  scale_shape_manual(values=c(19,4)) + #guides(col=F) +
  geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age mid") + theme_bw() + ggtitle("Literature inpatient ratio")
hplot

# Keep a record of the values
p <- preds[,c("age_mid","hospital_ratio","hospital_linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
p <- unique(p)
p$count_obs <- c(length(cv_hospital$ratio))
p$variable <- "cv_hospital"

# Documentation needs betas, gamma_soln
#  xwvals <- rbind.fill(xwvals, p)
xwvals <- rbind(xwvals, fit2$model_coefs)

# Join back with master data_frame
# preds$sdi <- preds$X_sdi
lri <- join(lri, preds[,c("age_mid","hospital_ratio","hospital_linear_se","log_hospital_ratio","log_hospital_se")], by="age_mid")

table(is.na(lri$is_reference))

# save preds

preds$variable <- "cv_hospital"
predvals <- data.frame()
predvals <- rbind(predvals, preds)
setnames(predvals, c("hospital_linear_se","hospital_ratio","log_hospital_ratio","log_hospital_se"), c("linear_se","ratio","log_ratio","log_se"))

######################################################################
## Self-reported crosswalk. This is for data that report the prevalence
## of self-reported LRI (from surveys). There are no sources, by definition,
## that reported both the prevalence in self reported and reference definition.
## There could be some survey validation studies but they aren't being used currently.
######################################################################

surveys <- read.csv("filepath")
surveys <- surveys[, -which(names(surveys) %in% c("location","location_update","X","child_sex","age_year","scalar_lri","scalar_se",
                                                  "base_lri","good_no_fever","poor_no_fever","missing","duration","duration_lower",
                                                  "duration_upper","urban","location_ascii_name","log_mean","log_mean_original",
                                                  "cv_no_fever","log_chest_ratio","log_diff_fever_ratio","log_diff_ratio","log_chest_se",
                                                  "log_diff_fever_se","log_diff_se"))]
surveys$group_review <- 1
surveys$is_reference <- 0
surveys$cv_hospital <- 0
surveys <- join(surveys, locs[,c("location_id","ihme_loc_id","location_name")], by="ihme_loc_id")

lri <- rbind.fill(lri, surveys)
lri <- join(lri, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")

cv_selfreport <- bundle_crosswalk_collapse(lri, covariate_name="cv_diag_selfreport", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="between", location_match="country")

cv_selfreport$year_id <- floor((cv_selfreport$year_end + cv_selfreport$year_start)/2)
cv_selfreport$age_mid <- floor((cv_selfreport$age_end + cv_selfreport$age_start)/2)
cv_selfreport$ihme_loc_id <- cv_selfreport$location_match

cv_selfreport <- join(cv_selfreport, locs[,c("location_id","super_region_name","ihme_loc_id")], by="ihme_loc_id")
cv_selfreport <- join(cv_selfreport, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
cv_selfreport$row <- 1:length(cv_selfreport$location_id)

write.csv(cv_selfreport[cv_selfreport$ratio<1,], "filepath", row.names=F)

# Test a couple linear models for predictors
summary(lm(log_ratio ~ age_mid, data=cv_selfreport))
summary(lm(log_ratio ~ sdi, data=cv_selfreport))
summary(lm(log_ratio ~ super_region_name, data=cv_selfreport))

## Run an MR-BRT model. No covariates used##
# Including NID as a study id
fit1 <- run_mr_brt(
  output_dir = paste0("filepath"),
  model_label = "cv_selfreport_2020_final_4",
  data = paste0("data_filepath"),
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1
)

check_for_outputs(fit1)
df_pred <- data.frame(expand.grid(intercept = 1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)

# Convert the mean and standard_error to linear space
preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
preds$ratio <- exp(preds$Y_mean)

mod_data <- fit1$train_data
mod_data <- join(mod_data, locs[,c("location_id","location_name","super_region_name")], by="location_id")

## Create essentially a forest plot
mod_data$outlier <- ceiling(abs(mod_data$w - 1))
mod_data$row_num <- 1:length(mod_data$age_bin)
mod_data$label <- with(mod_data, paste0(location_name,"_",age_start,"-",age_end,"_",year_start,"-",year_end,"_",row_num))
f2 <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=log_ratio + delta_log_se*1.96, ymin=log_ratio - delta_log_se*1.96)) + geom_point(aes(y=log_ratio, x=label)) + geom_errorbar(aes(x=label), width=0) +
  theme_bw() + ylab("Log ratio") + xlab("") + coord_flip() + ggtitle(paste0("Self-report ratio")) +
  geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
  geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
print(f2)

## Inlcude the value from DisMod 2017 for comparison
dismod <- data.frame(mean=1/3.8, lower=1/3.58, upper=1/3.99)
# Calculate log se
preds$log_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
preds$selfreport_linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "log_se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
preds$selfreport_ratio <- exp(preds$Y_mean)
# rename
preds$log_ratio <- preds$Y_mean

# Keep a record of the values
p <- preds[,c("selfreport_ratio","selfreport_linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
p <- unique(p)
p$count_obs <- c(length(cv_selfreport$ratio))
p$variable <- "cv_hospital"

# Documentation needs betas, not predictions
#  xwvals <- rbind.fill(xwvals, p)
fit1$model_coefs$crosswalk <- "cv_selfreport"
xwvals <- rbind(xwvals, fit1$model_coefs)

# Join back with master data_frame
lri$selfreport_ratio <- preds$selfreport_ratio
lri$selfreport_linear_se <- preds$selfreport_linear_se

preds$variable <- "cv_selfreport"
preds <- preds[,c(1:14,18)]
predvals <- rbind.fill(predvals, preds)

################################################################################
## Perform actual crosswalk, save results for upload ##
################################################################################
lri$raw_mean <- lri$mean
lri$raw_standard_error <- lri$standard_error
lri$crosswalk_type <- ifelse(lri$cv_hospital==1, "Hospitalized", ifelse(lri$cv_diag_selfreport==1, "Self-reported","No crosswalk"))

## calculate the adjusted standard_errors outside of ifelse statement
std_hospital <- sqrt(with(lri, standard_error^2 * hospital_linear_se^2 + standard_error^2*hospital_ratio^2 + hospital_linear_se^2*mean^2))
std_selfreport <- sqrt(with(lri, standard_error^2 * selfreport_linear_se^2 + standard_error^2*selfreport_ratio^2 + selfreport_linear_se^2*mean^2))

#####################################
## Convert the mean by the crosswalk!
# Hospital
lri$mean <- ifelse(lri$cv_hospital==1, lri$mean * lri$hospital_ratio, lri$mean)
lri$standard_error <- ifelse(lri$cv_hospital==1, std_hospital, lri$standard_error)
lri$note_modeler <- ifelse(lri$cv_hospital==1, paste0("This data point was adjusted for being in a hospitalized sample population. The code to produce
                                                      this estimated ratio is from prepare_crosswalks_selfreport_hosp_mr-brt.R. This occurred in MR-BRT
                                                      during decomposition step 2. The original mean was ", round(lri$raw_mean,3),". ", lri$note_modeler), lri$note_modeler)
# Self-report
lri$mean <- ifelse(lri$cv_diag_selfreport==1, lri$mean * lri$selfreport_ratio, lri$mean)
lri$standard_error <- ifelse(lri$cv_diag_selfreport==1, std_selfreport, lri$standard_error)
lri$note_modeler <- ifelse(lri$cv_diag_selfreport==1, paste0("This data point was adjusted for being from a self-reported sample population (typically surveys). The code to produce
                                                             this estimated ratio is from prepare_crosswalks_selfreport_hosp_mr-brt.R. This occurred in MR-BRT
                                                             during decomposition step 2. The original mean was ", round(lri$raw_mean,3),". ", lri$note_modeler), lri$note_modeler)

## Pull out data only used to inform crosswalk
lri <- subset(lri, nid!=999999)

## Clear lower/upper if adjusted
lri$lower <- ifelse(lri$cv_hospital==1, "", ifelse(lri$cv_diag_selfreport==1,"",lri$lower))
lri$upper <- ifelse(lri$cv_hospital==1, "", ifelse(lri$cv_diag_selfreport==1,"",lri$upper))

## Make sure data changed

lri$is_reference <- as.factor(lri$is_reference)

## Pull out the data from this crosswalk
lit_survey_output <- lri

ggplot(lit_survey_output, aes(x=raw_mean, y=mean, col=is_reference)) + geom_point() + geom_abline()
ggplot(lit_survey_output, aes(x=raw_mean, y=mean, col=source_type)) + geom_point() + geom_abline()

table(lit_survey_output$raw_mean == lit_survey_output$mean, lit_survey_output$source_type)
table(lit_survey_output$raw_mean == lit_survey_output$mean, lit_survey_output$is_reference)

View(lit_survey_output[lit_survey_output$is_reference == 0 & lit_survey_output$mean == lit_survey_output$raw_mean
                       , c("mean","raw_mean", "is_reference","source_type","cv_hospital","cv_diag_selfreport")
                       ])

## Save!
write.csv(lri, "filepath", row.names=F)

######################################
## Plot these changes ##
# Pull the results from DisMod for comparison
dismod <- data.frame(crosswalk=c("Hospital","Selfreport"), value=c(0.78, 3.8))

print(sr_hosp)
print(age_hosp)
#print(f1)
print(f2)
ggplot(lit_survey_output, aes(x=raw_mean, y=mean, col=crosswalk_type)) + geom_point() + geom_abline(intercept=0, slope=1) + theme_bw() +
  xlab("Unadjusted mean") + ylab("Mean after crosswalk") + scale_color_discrete("") +
  geom_abline(intercept=0, slope = 1/dismod$value[1], col="red", lty=2) +
  geom_abline(intercept=0, slope = 1/dismod$value[2], col="blue", lty=2) +
  ggtitle(paste0("LRI crosswalks\nThe dashed line is what the values would be after DisMod decomp 1\nDisMod value hospital= ",round(1/dismod$value[1],3),
                 "\nDisMod value self-report= ",round(1/dismod$value[2],3)))


#######################################################################
## CLINICAL - Inpatient and Claims
#######################################################################

duration <- read.csv("filepath")

lri <- read.csv("filepath")
clin_df <- subset(lri, clinical_data_type!="")
clin_df <- clin_df[clinical_data_type == "claims, inpatient only", clinical_data_type := "claims"]

clin_df <- clin_df[clinical_data_type == "inpatient", cv_inpatient := 1]
clin_df <- clin_df[clinical_data_type == "claims", cv_marketscan := 1]

clin_df <- clin_df[clinical_data_type =="claims" | clinical_data_type == "inpatient", gbd_2020_new := 1]
clin_df <- clin_df[clinical_data_type =="claims" | clinical_data_type == "inpatient", gbd_round := 2020]

clin_df$is_reference <- 0
clin_df$extractor <- "clinical_team"

table(clin_df$clinical_data_type)
table(is.na(clin_df$is_reference))
table(clin_df$is_reference)

## subset clinical data based on IQR

quint <- data.frame(ncol = 6)

quintiles <- summary(clin_df[,mean])
print(quintiles)

quint <- rbind.data.frame(quintiles, make.row.names = TRUE)

setnames(quint, c("min","first","median","mean","third","max"))

iqr <- IQR(clin_df$mean) * 1.5
upper <- quint$third + iqr
lower <- quint$first - iqr

clin_df_1 <- clin_df[mean < 0.001285]
clin_df_1 <- subset(clin_df, clin_df$mean < "upper" | clin_df$mean > "lower")

summary(clin_df_1$mean)


## subset clin_df based on NID

`%notin%` <- Negate(`%in%`)

nid_out <- c(281819, 337219, 354896, 292575, 292437)

clin_df <- clin_df[nid %notin% nid_out,]

table(clin_df$nid %in% nid_out)

## Separate out non-clinical data and recode

lri_bundle <- subset(lri, clinical_data_type == "")
table(is.na(lri_bundle$is_reference))

lri_bundle$cv_hospital[is.na(lri_bundle$cv_hospital)] <- 0
lri_bundle$cv_inpatient <- 0
lri_bundle$cv_diag_selfreport[is.na(lri_bundle$cv_diag_selfreport)] <- 0
lri_bundle$is_reference <- with(lri_bundle, ifelse(cv_hospital == 0 & cv_inpatient == 0 & cv_diag_selfreport == 0 & cv_marketscan == 0 & cv_inpatient_sample == 0, 1, 0))

## check to make sure there are no NAs in the reference column

table(is.na(lri_bundle$is_reference))
table(lri_bundle$is_reference) 
nrow(lri[cv_hospital == 0 & cv_inpatient == 0 & cv_diag_selfreport == 0 & cv_marketscan == 0 & cv_inpatient_sample == 0]) ## -----> matches number of 'is_reference'

lri_bundle[is.na(is_reference), is_reference := 0]

table(lri_bundle$source_type)
table(lri_bundle$cv_inpatient)
table(lri_bundle$cv_marketscan)

## Use a function to convert incidence to prevalence
prevalence_means <- convert_inc_prev(clin_df, duration$mean, duration$lower, duration$upper)
clin_df$mean <- prevalence_means$mean_prevalence
clin_df$standard_error <- prevalence_means$standard_error_prevalence
clin_df$lower <- ""
clin_df$upper <- ""
clin_df$cv_inpatient <- ifelse(clin_df$clinical_data_type == "inpatient", 1, 0)
clin_df$cv_marketscan <- ifelse(clin_df$clinical_data_type == "claims", 1, 0)
clin_df$note_modeler <- "Converted from incidence to prevalence using mean duration 7.79"
clin_df$measure <- "prevalence"
clin_df$age_end <- ifelse(clin_df$age_end > 99, 99, clin_df$age_end)

# Used in crosswalk
lri_full <- rbind.fill(lri_bundle, clin_df)

# Data to be saved
output_data <- rbind.fill(lri_bundle, clin_df)
write.csv(output_data, "filepath", row.names=F)

# Join with SDI as it is a predictor for cv_hospital
lri_full$year_id <- floor((lri_full$year_start + lri_full$year_end) / 2)
lri_full <- join(lri_full, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
lri_full$sdi <- round(lri_full$sdi,2)

## Set some parameters ## Same as already used; see bins created in survey section
age_bins <- c(0,1,5,10,20,40,60,80,100)
age_bins <- c(0,1,seq(5,100,5))

## Some data don't have cases or sample size
# Calculate cases and sample size (missing in inpatient data)
sample_size <- with(lri_full, mean*(1-mean)/standard_error^2)
cases <- lri_full$mean * sample_size
lri_full$cases <- ifelse(is.na(lri_full$cases), cases, lri_full$cases)
lri_full$sample_size <- ifelse(is.na(lri_full$sample_size), sample_size, lri_full$sample_size)

## Subset to working data frame ##
df <- lri_full[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                  "cv_diag_selfreport","cv_hospital","cv_inpatient","cv_marketscan","is_reference","is_outlier","group_review")]
df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")
df$group_review[is.na(df$group_review)] <- 1
df$cv_marketscan <- as.numeric(df$cv_marketscan)

df <- as.data.table(df)
unique(df$cv_marketscan)
unique(df$cv_inpatient)
table(df$cv_marketscan)
df[is.na(cv_marketscan), cv_marketscan := 0]
df <- as.data.frame(df)

cv_inpatient <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="between", location_match="country", include_logit = T)

cv_inpatient$age_mid <- floor((cv_inpatient$age_end + cv_inpatient$age_start)/2)
cv_inpatient$match_id <- paste0(cv_inpatient$nid, "_", cv_inpatient$n_nid)
cv_inpatient$year_id <- round((cv_inpatient$year_end + cv_inpatient$year_start)/2,0)
cv_inpatient$ihme_loc_id <- cv_inpatient$location_match
cv_inpatient$location_id <- cv_inpatient$location_match
cv_inpatient <- join(cv_inpatient, sdi[,c("ihme_loc_id","sdi","year_id")], by=c("ihme_loc_id","year_id"))

cv_marketscan <- bundle_crosswalk_collapse(df, covariate_name="cv_marketscan", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="between", location_match="country", include_logit = T)

cv_marketscan$age_mid <- floor((cv_marketscan$age_end + cv_marketscan$age_start)/2)
cv_marketscan$match_id <- paste0(cv_marketscan$nid, "_", cv_marketscan$n_nid)
cv_marketscan$year_id <- round((cv_marketscan$year_end + cv_marketscan$year_start)/2,0)
cv_marketscan$ihme_loc_id <- cv_marketscan$location_match
cv_marketscan$location_id <- cv_marketscan$location_match

write.csv(cv_inpatient, "filepath", row.names=F)
write.csv(cv_marketscan, "filepath", row.names=F)

##########################################################################
## Model for clinical inpatient data ##
##########################################################################

## model with SDI and age
fit1 <- run_mr_brt(
  output_dir = "filepath",
  model_label = "cv_inpatient_2020_outlier_final",
  data = cv_inpatient,
  mean_var = "logit_ratio",
  se_var = "logit_ratio_se",
  covs = list(cov_info("sdi","X",
                       degree = 2, n_i_knots=3
                       , l_linear=TRUE
                       , r_linear = TRUE
                       , bspline_cvcv = "convex"
                       , bspline_mono = "decreasing"
                       , bspline_gprior_mean = "0,0,0,0"
                       , bspline_gprior_var = "inf,inf,inf,inf"
  )),
  study_id = "match_id",
  overwrite_previous = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1
)

df_pred <- data.frame(sdi = seq(0, 1, length.out = 101))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)

pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
preds$age_mid <- preds$X_age_mid
preds$sdi <- preds$X_sdi

## Look at the age curve ##
mod_data <- fit1$train_data
mod_data$outlier <- floor(abs(mod_data$w - 1))
ciplot <- ggplot(preds, aes(x=sdi, y=(Y_mean))) + geom_line() +
  geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
  geom_point(data=mod_data, aes(x=sdi, y=logit_ratio, size=1/se^2, col=factor(location_match), shape=factor(outlier))) + guides(size=F) + scale_shape_manual(values=c(19,4)) + guides(shape=F) +
  geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("SDI") + theme_bw() + ggtitle("Inpatient ratio - reed settings")
ciplot

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)

# # Convert the mean and standard_error to linear space
preds$inpatient_linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
})
preds$inpatient_ratio <- inv.logit(preds$Y_mean)
preds$inpatient_logit <- preds$Y_mean

### Rejoin inpatient ratio with clinical data  ###
# Data to be saved
rm(output_data)
output_data <- rbind.fill(lri_bundle, clin_df) ## -----------> already done above
output_data$age_mid <- ceiling((output_data$age_end + output_data$age_start)/2)

output_data$year_id <- round((output_data$year_start + output_data$year_end)/2,0)
output_data <- join(output_data, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
output_data$sdi <- round(output_data$sdi, 2)
preds$sdi <- round(preds$sdi, 2)
output_data <- join(output_data, preds[,c("sdi","inpatient_ratio","inpatient_linear_se","inpatient_logit")], by=c("sdi"))

# Keep a record of the values
p <- preds[,c("inpatient_logit","Y_mean","Y_mean_lo","Y_mean_hi")]
p <- unique(p)
p$variable <- c("cv_inpatient")
p$count_obs <- c(length(cv_inpatient$ratio))

xwvals <- rbind.fill(xwvals, p)

## see the adjusted age pattern. 
adf <- output_data[output_data$cv_inpatient==1,c("mean","age_start","age_end","inpatient_logit","location_id","year_start")]
bdf <- adf
cdf <- output_data[output_data$is_reference==1,c("mean","age_start","age_end","location_id","year_start")]
cdf$age_start <- round(cdf$age_start, 0)
adf$type <- "Unadjusted clinical"
bdf$type <- "Adjusted clinical"
cdf$type <- "Reference"
bdf$mean <- inv.logit(logit(bdf$mean) + bdf$inpatient_logit)

ddf <- data.frame(type="Filler", age_start=seq(0,98,2), age_end=seq(1,99,2), mean=0.001, location_id=1)

mean_ref <- median(output_data$mean[output_data$is_reference==1], na.rm=T)

rbind_abc <- rbind.fill(adf, bdf, cdf)
rbind_abc$year_id <- rbind_abc$year_start
rbind_abc <- join(rbind_abc, sdi[,c("location_id","year_id","sdi")])

rbind_abc$age_bins <- cut(rbind_abc$age_start, c(-1,1,5,20,40,60,80,100), labels = c("0-0.9","1-4","5-19","20-39","40-59","60-79","80+"))
rbind_abc$sdi_bins <- cut(rbind_abc$sdi, seq(0.2,1,0.1))

ciplot_age  <- ggplot(rbind_abc[!is.na(rbind_abc$age_bins),], aes(x=factor(age_bins), y=mean, col=type)) + geom_boxplot() + 
  theme_bw() + scale_y_log10("Prevalence") +
  xlab("Age start (binned)") + scale_color_discrete("") + ggtitle("All locations, SDI adjusted")
ciplot_age

ciplot_sdi <- ggplot(rbind_abc[!is.na(rbind_abc$sdi_bins),], aes(x=factor(sdi_bins), y=mean, col=type)) + geom_boxplot() + 
  theme_bw() + scale_y_log10("Prevalence") +
  xlab("SDI (binned)") + scale_color_discrete("") + ggtitle("All locations, SDI adjusted")
ciplot_sdi

ggplot(rbind.fill(adf,bdf,cdf,ddf), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + 
  ggtitle("All locations, SDI adjusted")

ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==520), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Yunnan")
ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==570), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Washington")
ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==4767), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=0.0005, lty=2) + geom_hline(yintercept=0.0001, lty=2) + geom_hline(yintercept=mean_ref) + ggtitle("Piaui")

##########################################################################
## Model for Claims data ##
##########################################################################

## Run an MR-BRT model. ##
fit1 <- run_mr_brt(
  output_dir = "filepath",
  model_label = "cv_marketscan_2020_outlier_final",
  data = cv_marketscan,
  mean_var = "logit_ratio",
  se_var = "logit_ratio_se",
  study_id = "match_id",
  overwrite_previous = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1
)

df_pred <- data.frame(intercept=1, age_mid=seq(0,100,1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)

pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
preds$age_mid <- 0:100

## Look at the age curve ##
mod_data <- fit1$train_data
mod_data$outlier <- floor(abs(mod_data$w - 1))
mod_data$row_num <- 1:length(mod_data$mean)

cmplot <- ggplot(preds, aes(x=age_mid, y=(Y_mean))) + geom_line() +
  geom_ribbon(aes(ymin=(Y_mean_lo), ymax=(Y_mean_hi)), alpha=0.3) +
  geom_point(data=mod_data, aes(x=age_mid, y=logit_ratio, size=1/se^2, col=factor(outlier))) + 
  guides(size=F) + scale_color_manual(values=c("black","red")) + guides(col=F) +
  geom_hline(yintercept=0, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age", limits=c(0,100)) + 
  theme_bw() + ggtitle("Claims ratio")
cmplot

## Create essentially a forest plot
mod_data$label <- with(mod_data, paste0(match_id,"_",location_match,"_",age_bin,"_",year_bin,"_",row_num))
claimsplot <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=logit_ratio + logit_ratio_se*1.96, ymin=logit_ratio - logit_ratio_se*1.96)) + 
  geom_point(aes(y=logit_ratio, x=label)) + geom_errorbar(aes(x=label), width=0) +
  theme_bw() + ylab("Transformed ratio") + xlab("") + coord_flip() + ggtitle("Claims data") +
  geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean[1], col="purple") +
  geom_rect(data=preds[1,], aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
claimsplot

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)

# # Convert the mean and standard_error to linear space
preds$marketscan_linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "se"]
  deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
})
preds$marketscan_ratio <- inv.logit(preds$Y_mean)
preds$marketscan_logit <- preds$Y_mean

### save claims data

output_claims <- preds

### Rejoin marketscan ratio with clinical data  ###
output_data$age_mid <- ceiling((output_data$age_end + output_data$age_start)/2)
#output_data <- join(output_data, preds[,c("age_mid","marketscan_ratio","marketscan_linear_se","marketscan_logit")], by="age_mid")
output_data <- join(output_data, output_claims[,c("age_mid","marketscan_ratio","marketscan_linear_se","marketscan_logit")], by="age_mid")

# Keep a record of the values
p <- preds[,c("marketscan_logit","Y_mean","Y_mean_lo","Y_mean_hi")]
p <- unique(p)
p$variable <- c("cv_marketscan")
p$count_obs <- c(length(cv_marketscan$ratio))
xwvals <- rbind.fill(xwvals, p)

################################################################################
## Perform actual crosswalk, save results for upload ##
################################################################################
output_data$raw_mean <- output_data$mean
output_data$raw_standard_error <- output_data$standard_error

## calculate the adjusted standard_errors outside of ifelse statement
std_marketscan <- sqrt(with(output_data, standard_error^2 * marketscan_linear_se^2 + standard_error^2*marketscan_ratio^2 + marketscan_linear_se^2*mean^2))
std_inpatient <- sqrt(with(output_data, standard_error^2 * inpatient_linear_se^2 + standard_error^2*inpatient_ratio^2 + inpatient_linear_se^2*mean^2))

#####################################
## Convert the mean by the crosswalk!

output_data$logit_mean <- logit(output_data$mean)

## Back to crosswalking claims data. 
# Claims

output_data$mean <- ifelse(output_data$clinical_data_type=="claims", inv.logit(output_data$logit_mean + output_data$marketscan_logit)
                           , output_data$mean)
output_data$standard_error <- ifelse(output_data$clinical_data_type=="claims", std_marketscan, output_data$standard_error)

# Inpatient
output_data$mean <- ifelse(output_data$clinical_data_type=="inpatient", inv.logit(output_data$logit_mean + output_data$inpatient_logit)
                           , output_data$mean)
output_data$standard_error <- ifelse(output_data$clinical_data_type=="inpatient", std_inpatient, output_data$standard_error)
output_data$note_modeler <- ifelse(output_data$clinical_data_type!="", paste0("This data point was adjusted for being in a clinical dataset. The code to produce this estimated ratio is from crosswalk_clinical_inpatient_mr-brt.R. This occurred in MR-BRT during decomposition step 2. The original mean was ",
                                                                              round(output_data$raw_mean,3),". ", output_data$note_modeler), output_data$note_modeler)

output_data <- as.data.table(output_data)

ggplot(output_data[clinical_data_type != ""], aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline()
ggplot(output_data[clinical_data_type == ""], aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline()
ggplot(output_data[clinical_data_type == "inpatient"], aes(x=raw_mean, y=mean, col=cv_inpatient)) + geom_point() + geom_abline()
ggplot(output_data[clinical_data_type == "claims"], aes(x=raw_mean, y=mean, col=is_reference)) + geom_point() + geom_abline()

ggplot(output_data, aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline()
ggplot(output_data, aes(x=raw_mean, y=mean, col=source_type)) + geom_point() + geom_abline()
ggplot(output_data[source_type == "Facility - inpatient"], aes(x=raw_mean, y=mean, col=source_type)) + geom_point() + geom_abline()


nrow(output_data[clinical_data_type != "" & output_data$mean == output_data$raw_mean,])

View(output_data[mean != raw_mean, c("nid","source_type", "mean","raw_mean")])
View(output_data[mean == raw_mean, c("nid","source_type", "mean","raw_mean", "is_reference")])


## Save!
write.csv(output_data, "filepath", row.names=F)

## Save crosswalk values
xwvals <- rbind.fill(xwvals, survey_outputs)
write.csv(xwvals, "filepath", row.names=F)

## Collect output from this crosswalk.
clinical_output <- subset(output_data, clinical_data_type != "")

output_data <- as.data.table(output_data)
output_data[is.na(output_data)] <- ""

write.xlsx(clinical_output, "filepath", row.names=F)
write.csv(output_data, "filepath")

#############################################
## Step 4: upload everything!! ##
#############################################

rm(data_for_upload)
data_for_upload <- rbind.fill(clinical_output, lit_survey_output)

ggplot(clinical_output, aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline() +
  theme_bw() + ylab("adjusted mean") + xlab("raw mean") + ggtitle("Clinical Adjustments")
ggplot(lit_survey_output, aes(x=raw_mean, y=mean, col=source_type)) + geom_point() + geom_abline() +
  theme_bw() + ylab("adjusted mean") + xlab("raw mean") + coord_flip() + ggtitle("Non-Clinical Adjustments")
ggplot(data_for_upload, aes(x=raw_mean, y=mean, col=source_type)) + geom_point() + geom_abline() +
  theme_bw() + ylab("adjusted mean") + xlab("raw mean") + coord_flip() + ggtitle("All Data Adjustments")
ggplot(data_for_upload, aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline() +
  theme_bw() + ylab("adjusted mean") + xlab("raw mean") + coord_flip() + ggtitle("All Data Adjustments")


## Both sex data should be duplicated ##
source("filepath/sex_split_mrbrt_weights.R")

data_for_upload$seq_parent <- ""
data_for_upload$crosswalk_parent_seq <- ""
sex_df <- duplicate_sex_rows(data_for_upload)

sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- as.numeric(sex_df$cases)

sex_df$mean <- ifelse(is.na(sex_df$mean), sex_df$cases / sex_df$sample_size, sex_df$mean)
sex_df$group_review[is.na(sex_df$group_review)] <- ""
sex_df <- subset(sex_df, group_review != 0)
sex_df[is.na(sex_df)] <- ""
sex_df$group_review <- ifelse(sex_df$specificity=="","",1)
sex_df$uncertainty_type <- ifelse(sex_df$lower=="","", as.character(sex_df$uncertainty_type))
sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="","",sex_df$uncertainty_type_value)
sex_df$uncertainty_type <- ifelse(sex_df$lower=="", "", sex_df$uncertainty_type)
sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="", "", sex_df$uncertainty_type_value)

ggplot(sex_df, aes(x=raw_mean, y=mean, col=clinical_data_type)) + geom_point() + geom_abline()
ggplot(sex_df, aes(x=raw_mean, y=mean, col=source_type)) + geom_point() + geom_abline()


# After age splitting, sometimes cases > sample size

sex_df$cases <- as.numeric(sex_df$cases)
sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.99, sex_df$cases)
sex_df$mean <- as.numeric(sex_df$mean)

# Subset if data are greater than 1

table(sex_df$mean > 1)

sex_df <- subset(sex_df, mean < 1)

# Make sure cases < sample size

sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- as.numeric(sex_df$cases)
sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.9, sex_df$cases)
sex_df$xwalk_type <- ifelse(sex_df$clinical_data_type!="","Clinical", ifelse(sex_df$cv_hospital==1,"Literature hospital", ifelse(sex_df$cv_diag_selfreport==1,"Survey","None")))

ggplot(sex_df, aes(x=raw_mean, y=mean, col=xwalk_type)) + geom_point() + geom_abline()
ggplot(sex_df, aes(x=raw_mean, y=mean, col=super_region_name)) + geom_point() + geom_abline()

sex_df <- sex_df[, -which(names(sex_df) %in% c("cv_dhs","cv_whs","cv_nine_plus_test","cv_explicit_test","duration","duration_lower","duration_upper",
                                               "cv_clin_data","unnamed..75","unnamed..75.1","survey","original_mean","is_reference",
                                               "age_mid","ratio","linear_se","std_clinical","cv_inpatient_lit","hospital_ratio","hospital_linear_se",
                                               "log_hospital_ratio","log_hospital_se","inpatient_lit_ratio","cv_miscoded","inpatient_lit_linear_se","log_inpatient_lit_ratio",
                                               "log_inpatient_lit_se", "raw_mean","raw_standard_error","crosswalk_type","parent_id","sdi","mean_original",
                                               "standard_error_original","cv_had_fever","indicator","region_name","super_region_name","ihme_loc_id.1",
                                               "selfreport_rateio","selfreport_linear_se","exists_subnational","year_id","note_SR", "cv_diag_x.ray","cv_diag_x.ray.1"))]

# Assign seq_parent and crosswalk_parent_seq
sex_df$seq_parent <- ifelse(sex_df$seq_parent=="", sex_df$seq, sex_df$seq_parent)
sex_df$crosswalk_parent_seq <- ifelse(sex_df$crosswalk_parent_seq=="", sex_df$seq, sex_df$crosswalk_parent_seq)

## Opportunity to quickly look for outliers ##
source("filepath/map_dismod_input_function.R")
source("filepath/input_data_scatter_sdi_function.R")
library(scales)

sex_df <- join(sex_df, locs[,c("location_id","parent_id","level","region_name","super_region_name")], by="location_id")

map_dismod_input(sex_df, type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Unadjusted data")
map_dismod_input(sex_df[sex_df$cv_inpatient==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Clinical inpatient")
map_dismod_input(sex_df[sex_df$cv_diag_selfreport==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Survey data")

input_scatter_sdi(sex_df, error = (-0.75), title = "All data")
input_scatter_sdi(sex_df[sex_df$cv_inpatient==1,], error = (-0.75), title = "Inpatient data")
input_scatter_sdi(sex_df[sex_df$cv_diag_selfreport==1,], error = (-0.75), title = "Survey data")

for(s in unique(sex_df$region_name)){
  p <- ggplot(subset(sex_df, level ==3 & region_name==s), aes(x=location_name, y=mean)) + geom_boxplot() + geom_point(alpha=0.4 , aes(col=xwalk_type)) + scale_y_log10(label=percent) + ggtitle(s) + theme_bw() +
    geom_hline(yintercept = median(sex_df$mean), col="red") +
    geom_hline(yintercept = quantile(sex_df$mean, 0.25), col="red", lty=2) +
    geom_hline(yintercept = quantile(sex_df$mean, 0.75), col="red", lty=2)
  print(p)
}

for(l in unique(sex_df$parent_id[sex_df$level==4])){
  p <- ggplot(subset(sex_df, level==4 & parent_id==l), aes(x=location_name, y=mean)) + geom_boxplot() + geom_point(alpha=0.4, aes(col=xwalk_type)) + scale_y_log10(label=percent) + theme_bw() +
    theme(axis.text.x=element_text(angle=90, hjust=1)) + geom_hline(yintercept = median(sex_df$mean), col="red") +
    geom_hline(yintercept = quantile(sex_df$mean, 0.25), col="red", lty=2) +
    geom_hline(yintercept = quantile(sex_df$mean, 0.75), col="red", lty=2)
  print(p)
}

#### OUTLIER DATA AND FINAL CLEAN ####

sex_df_b <-copy(sex_df)

table(sex_df$is_outlier)

## Add outliers ##

summary(sex_df$mean)

sex_df$is_outlier <- ifelse(log(sex_df$mean) < (-12), 1, ifelse(log(sex_df$mean) > (-4), 1, 0))

## Add outliers from current best crosswalk version

crosswalk_version_id <- 25466

df_best <- get_crosswalk_version(crosswalk_version_id)

# get outliers from current best version

out_seq <- df_best[is_outlier == 1, seq]
out_seq_2019 <- df_2019[is_outlier ==1, seq]

sex_df <- as.data.table(sex_df)

sex_df_2019 <- as.data.table(sex_df)

sex_df[seq %in% c(out_seq), is_outlier := 1]

sex_df_2019[seq %in% c(out_seq_2019), is_outlier := 1]

table(sex_df$is_outlier)
table(sex_df_2019$is_outlier)

# Check mean, median, and quantile

quint <- data.frame(ncol = 6)

quintiles <- summary(sex_df[,mean])
print(quintiles)

quint <- rbind.data.frame(quintiles, make.row.names = TRUE)

setnames(quint, c("min","1st","median","mean","3rd","max"))

iqr <- IQR(sex_df$mean) * 1.5
upper <- quint$`3rd` + iqr
lower <- quint$`1st` - iqr


## outlier SUSENAS ##

table(sex_df$nid[sex_df$nid %in% c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)])

sex_df <- subset(sex_df, !(nid %in% c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)))

View(sex_df[nid %in% c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)])


## Adhoc upload fixes

sex_df <- as.data.table(sex_df)

sex_df$unit_value_as_published[is.na(sex_df$unit_value_as_publsihed)] <- 1
sex_df[is.na(unit_value_as_published), unit_value_as_published := 1]
sex_df[unit_value_as_published == "", unit_value_as_published := 1]


sex_df[recall_type == "", recall_type := "Point"]

sex_df[unit_type == "", unit_type := "Person"]

sex_df[urbanicity_type == "", urbanicity_type := "Unknown"]
sex_df[urbanicity_type == "unknown", urbanicity_type := "Unknown"]

sex_df[standard_error > 1.0, standard_error := 0.99]

sex_df[ihme_loc_id %in% c("IND_43907","IND_43914", "IND_43915", "IND_43909"), ihme_loc_id := "IND_44539"]
sex_df[location_id %in% c("43907","43914", "43915", "43909"), location_id := 44539]

sex_df[location_name == "Daman and Diu, Rural" | location_name == "Arunachal Pradesh, Rural", ihme_loc_id := "IND_44539"]
sex_df[location_name == "Daman and Diu, Rural" | location_name == "Arunachal Pradesh, Rural", location_id := 44539]

sex_df[ihme_loc_id %in% c("IND_43878", "IND_43873", "IND_43897", "IND_43879", "IND_43871"), ihme_loc_id := "IND_44540"]
sex_df[location_id %in% c("43878", "43873", "43897", "43871"), location_id := 44540]

sex_df[location_name == "Daman and Diu, Urban" | location_name == "Arunachal Pradesh, Urban", ihme_loc_id := "IND_44540"]
sex_df[location_name == "Daman and Diu, Urban" | location_name == "Arunachal Pradesh, Urban", location_id := 44540]

sex_df[location_name == "Pruducherry, Rural", ihme_loc_id := "IND_44539"]
sex_df[location_name == "Pruducherry, Rural", location_id := 44539]

sex_df[location_name == "Pruducherry, Urban", ihme_loc_id := "IND_44540"]
sex_df[location_name == "Pruducherry, Urban", location_id := 44540]

sex_df[location_id == 43933, location_id := 44539]

setnames(sex_df, c("cv_diag_x.ray"), c("cv_diag_xray"))
setnames(sex_df, c("cv_diag_x-ray"), c("cv_diag_xray"))
setnames(sex_df, c("cv_diag_xray"), c("cv1_diag_xray"))


### UPLOAD! ###

## Save a version with old data only
write.xlsx(sex_df[sex_df$gbd_2020_new == 0,], "filepath_old", sheetName="extraction")

## Save a version with new data
write.xlsx(sex_df[sex_df$gbd_2020_new == 1,], "filepath_new", sheetName="extraction")

## Save a version with all data
write.xlsx(sex_df, "filepath", sheetName="extraction")


## UPLOAD CROSSWALK VERSION
save_crosswalk_version(bundle_version_id=bundle_version_id
                       , data_filepath="data_filepath"
                       , description = "Iterative, all data; age groups and locations updated; surveys updated; best model outliers; new clinical spline constraints"
)


table(dt_1$is_outlier)
dt_1 <- as.data.table(read_excel("filepath"))
table(dt_1$nid[dt_1$nid == 270581])

table(emr_2019$source_type)
table(emr_2019$measure)



## CF2 METHOD


## Both sex data should be duplicated ##
data_for_upload$seq_parent <- ""
data_for_upload$crosswalk_parent_seq <- ""
sex_df <- clinical_output

## fix formatting 
sex_df$lower <- as.numeric(sex_df$lower)
sex_df$upper <- as.numeric(sex_df$upper)
sex_df$standard_error <- as.numeric(sex_df$standard_error)

sex_df$mean <- ifelse(is.na(sex_df$mean), sex_df$cases / sex_df$sample_size, sex_df$mean)
sex_df$group_review[is.na(sex_df$group_review)] <- ""
sex_df <- subset(sex_df, group_review != 0)
sex_df[is.na(sex_df)] <- ""
sex_df$group_review <- ifelse(sex_df$specificity=="","",1)
sex_df$uncertainty_type <- ifelse(sex_df$lower=="","", as.character(sex_df$uncertainty_type))
sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="","",sex_df$uncertainty_type_value)
sex_df$uncertainty_type <- ifelse(sex_df$lower=="", "", sex_df$uncertainty_type)
sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="", "", sex_df$uncertainty_type_value)

# After age splitting, sometimes cases > sample size
sex_df$cases <- as.numeric(sex_df$cases)
sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.99, sex_df$cases)
sex_df$mean <- as.numeric(sex_df$mean)

# Subset if data are greater than 1
sex_df <- subset(sex_df, mean < 1)

# Make sure cases < sample size
sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- as.numeric(sex_df$cases)
sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.9, sex_df$cases)
sex_df$xwalk_type <- ifelse(sex_df$clinical_data_type!="","Clinical", ifelse(sex_df$cv_hospital==1,"Literature hospital", ifelse(sex_df$cv_diag_selfreport==1,"Survey","None")))
ggplot(sex_df, aes(x=raw_mean, y=mean, col=xwalk_type)) + geom_point() + geom_abline()

sex_df <- sex_df[, -which(names(sex_df) %in% c("cv_dhs","cv_whs","cv_nine_plus_test","cv_explicit_test","duration","duration_lower","duration_upper",
                                               "cv_clin_data","unnamed..75","unnamed..75.1","survey","original_mean","is_reference",
                                               "age_mid","ratio","linear_se","std_clinical","cv_inpatient_lit","hospital_ratio","hospital_linear_se",
                                               "log_hospital_ratio","log_hospital_se","inpatient_lit_ratio","cv_miscoded","inpatient_lit_linear_se","log_inpatient_lit_ratio",
                                               "log_inpatient_lit_se", "raw_mean","raw_standard_error","crosswalk_type","parent_id","sdi","mean_original",
                                               "standard_error_original","cv_had_fever","indicator","region_name","super_region_name","ihme_loc_id.1",
                                               "selfreport_ratio","selfreport_linear_se","exists_subnational","year_id","note_SR", "cv_diag_xray","cv_diag_xray1", "cv_diag_xray","cv_diag_xray.1"))]


# Assign seq_parent and crosswalk_parent_seq
sex_df$seq_parent <- ifelse(sex_df$seq_parent=="", sex_df$seq, sex_df$seq_parent)
sex_df$crosswalk_parent_seq <- ifelse(sex_df$crosswalk_parent_seq=="", sex_df$seq, sex_df$crosswalk_parent_seq)

## Opportunity to quickly look for outliers (probably some in Eastern Europe, Brazil) ##
source("/filepath/map_dismod_input_function.R")
source("/filepath/input_data_scatter_sdi_function.R")
sex_df <- join(sex_df, locs[,c("location_id","parent_id","level","region_name","super_region_name")], by="location_id")

input_scatter_sdi(sex_df, error = (-0.75), title = "All data")
input_scatter_sdi(sex_df[sex_df$cv_inpatient==1,], error = (-0.75), title = "Inpatient data")
input_scatter_sdi(sex_df[sex_df$cv_diag_selfreport==1,], error = (-0.75), title = "Survey data")

## Outlier extreme values. ##
sex_df$is_outlier <- ifelse(log(sex_df$mean) < (-12), 1, ifelse(log(sex_df$mean) > (-4), 1, 0))

## No prevalence SE greater than 1
sex_df_1 <- sex_df
sex_df$standard_error <- as.numeric(sex_df$standard_error)
sex_df <- subset(sex_df, standard_error < 1)


#############
### SAVE! ###
#############

write.xlsx(sex_df, "filepath", sheetName = "extraction")
description = "Step 2 initial crosswalk version; gbd 2019 step 4 data, gbd 2020 clinical"

result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath="data_filepath",
  description=description)
