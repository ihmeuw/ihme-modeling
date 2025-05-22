#######################################################################
## STEP 4 -- LITERATURE, SURVEY (SELF-REPORT), AND CLINICAL DATA CROSSWALKS + UPLOAD SCRIPT  
#######################################################################

## set up
rm(list = ls())

set.seed(123)

pacman::p_load(plyr, openxlsx, ggplot2, metafor, msm, lme4, scales, data.table, boot, readxl, magrittr, RColorBrewer)
# Source all GBD shared functions at once
invisible(sapply(list.files("/FILEPATH/", full.names = T), source))

# Pull in crosswalk packages
Sys.setenv("RETICULATE_PYTHON" = "/FILEPATH") 
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

# Source utilities
source(paste0("/FILEPATH/", user,"/FILEPATH/bundle_crosswalk_collapse.R")) 
source(paste0("/FILEPATH/", user,"/FILEPATH/convert_inc_prev_function.R")) 
source(paste0("/FILEPATH/", user,"/FILEPATH/sex_split_mrbrt_weights.R")) 
source(paste0("/FILEPATH/", user,"/FILEPATH/map_dismod_input_function.R")) 
source(paste0("/FILEPATH/", user,"/FILEPATH/input_data_scatter_sdi_function.R")) 

# Set-up variables
date <- gsub("-", "_", Sys.Date())
user <- Sys.info()["user"]
release <- 16
bundle_version_id <- 47889 
best_cw <- 46209 # Current best crosswalk version for adding in additional outliers

# Save directory
save_dir <- paste0("/FILEPATH/", date, "/")
dir.create(paste0(save_dir), recursive = TRUE)

# Upload directory
upload_dir <- paste0("/FILEPATHd/", date, "/")
dir.create(paste0(upload_dir), recursive = TRUE)

# Years
years <- get_demographics(gbd_team = 'epi',
                          release_id = release)[["year_id"]]

cov_years <- years

# Pull locations
locs <- as.data.table(get_location_metadata(location_set_id = 35, release_id = release))

## get SDI, HAQI as a potential predictor
sdi <- get_covariate_estimates(location_id="all", covariate_id=881, release_id = release, year_id=seq(min(cov_years), max(cov_years)))
sdi <- join(sdi, locs[,c("location_id","ihme_loc_id")], by="location_id")
sdi$sdi <- sdi$mean_value

haqi <- get_covariate_estimates(location_id="all", covariate_id=1099, release_id = release, year_id=seq(min(cov_years), max(cov_years)))
haqi <- join(haqi, locs[,c("location_id","ihme_loc_id")], by="location_id")
haqi$haqi <- haqi$mean_value

new_bundle <- get_bundle_version(bundle_version_id = bundle_version_id)

lri <- fread(paste0(save_dir,"/FILEPATH", bundle_version_id,"_", date, ".csv"))

## Outlier Nepal NID as it represents rural Nepal, not all of Nepal
lri <- lri[(nid == 107299), is_outlier := 1]

## Outlier severe pneumonia row from NID 108883 as the pneumo row already contains severe pneumo
lri <- lri[!(nid == 108883 & case_name == "severe pneumonia")]

## Outlier row with a rural population (that would later become a reference study
lri <- lri[nid == 108889, is_outlier := 1]

##  We only xwalk incidence and prevalence.
lri <- lri[measure %in% c("incidence", "prevalence")]

# These data work for the reference and cv_hospital
lri$group_review[is.na(lri$group_review)] <- 1
lri <- subset(lri, is_outlier==0 & lri$group_review==1)
lri$cv_inpatient <- ifelse(lri$clinical_data_type != "",1,0)

lri$age_mid <- floor((lri$age_end + lri$age_start) / 2)

####### Convert incidence to prevalence for rows with incidence data
inc <- lri[measure == "incidence"]

# meta-analysis for duration of LRI
duration <- read.csv(paste0(h,"/FILEPATH")) 

# Convert everything to duration (divide by 1 year)
duration_val   <- duration$mean/365.2422

duration <- as.data.table(duration)
test <- duration[, c(paste0("duration_", 1:1000))]
test <- test/365.2422
duration[,  c(paste0("duration_", 1:1000)) := test[, c(paste0("duration_", 1:1000))]]

duration[, duration_lower:= apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = (paste0("duration_", 1:1000))]
duration[, duration_upper:= apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = (paste0("duration_", 1:1000))]

duration_upper <- duration$duration_upper
duration_lower <- duration$duration_lower

duration_se <- (duration_upper - duration_lower) / (2*1.96)

# Convert to prevalence
inc$prev <- inc$mean * duration_val
inc$standard_error <- inc$prev*sqrt((inc$standard_error/inc$mean)^2+(duration_se/duration_val)^2)

inc$mean <- inc$prev
inc <- subset(inc, select = c(-prev))
inc$measure <- "prevalence"
inc$cases <- inc$mean * inc$sample_size # also convert cases after the prevalence conversion
inc$note_modeler <- paste0("Converted from incidence to prevalence using mean duration ", duration_val)

# Compute upper, lower UIs
inc$lower <- inc$mean - (1.96 * inc$standard_error)
inc$upper <- inc$mean + (1.96 * inc$standard_error)

# remove rows that were converted from overall bundle variable
dt_lri_without_inc <- lri[!(measure == "incidence")]

# combine together
lri <- rbind(dt_lri_without_inc, inc)


# Add the prevalence data to our comparison sheet
lri_temp <- copy(lri)

# Remove inpatient. Inpatient data added later
lri <- subset(lri, (cv_inpatient != 1 | is.na(cv_inpatient)))

# Remove survey (self-reported data)
lri <- subset(lri, (cv_diag_selfreport != 1 | is.na(cv_diag_selfreport)))

lri$is_reference <- ifelse(lri$cv_hospital==0 & lri$cv_diag_selfreport==0 & lri$cv_marketscan==0 & lri$cv_inpatient==0 & lri$cv_inpatient_sample==0, 1, 0)

table(is.na(lri$is_reference))
table(lri$is_reference)
lri[is.na(is_reference), is_reference := 0]

## Two studies are marked as reference that should not be, fixing that here
lri <- lri[nid == 221153, is_reference :=0]
lri <- lri[nid == 261880, is_reference :=0]

# Join with SDI as it is a predictor for cv_hospital
lri$year_id <- floor((lri$year_start + lri$year_end) / 2)
lri <- join(lri, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
lri$sdi <- round(lri$sdi,2)

## Set some parameters ##
ages_2020 = get_age_metadata(age_group_set_id=24, release_id=release)
age_bins_2020 <- ages_2020$age_group_years_start
age_bins <- c(0,0.0192,0.0767,0.5,1,2,5,20,40,60,80,100)
age_bins <- c(0,0.0192,0.0767,0.5,1,2,seq(5,100,5))

## Subset to working data frame ##
df <- as.data.frame(lri[,c("location_name","location_id","nid", "measure" ,"age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                           "cv_diag_selfreport","cv_hospital","cv_inpatient","is_reference","group_review","is_outlier")])
df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")

## Some rows don't have sample size and cases, use closed form estimate of SE to get them.
table(is.na(df$cases))
table(is.na(df$sample_size))

sample_size <- with(df, mean*(1-mean)/standard_error^2)
cases <- df$mean * sample_size
df$cases <- ifelse(is.na(df$cases), cases, df$cases)
df$sample_size <- ifelse(is.na(df$sample_size), sample_size, df$sample_size)

##############################################
## HOSPITAL LITERATURE DATA CROSSWALK
##############################################

## During a check, we found three NIDs that should not be crosswalked (they are not representative, but they are still 
## reference/physician diagnosed) We will remove from this bundle_crosswalk_collapse and ratio predictions and re-add them 
## after the crosswalk takes place
df <- as.data.table(df)

nids <- c(108870, 108872, 251361)
non_representative_nids_hosp <- df[nid %in% nids]
df <- df[!nid %in% nids]

## We are also removing one outlier here, but it will eventually be removed from bundle
## This NID is using PPI (proton pump inhibitor) patients, and it's unclear if the control group is representative
df <- df[!nid == 324119]

## Lastly, this NID was classified wrong. This NID should be in our alternative category, not reference
df <- df[nid == 108883, is_reference := 0]
df <- df[nid == 108883, cv_hospital := 1]

# if cv_hospital is NA, make 0
df <- df[is.na(cv_hospital), cv_hospital := 0]

df <- as.data.frame(df)

## Start finding matched pairs
cv_hospital <- bundle_crosswalk_collapse(df, covariate_name="cv_hospital", age_cut=age_bins, year_cut=c(years), merge_type="within", location_match="exact", release = release)

cv_hospital$year_id <- floor((cv_hospital$year_end + cv_hospital$year_start)/2)
cv_hospital$location_id <- cv_hospital$location_match
cv_hospital$age_mid <- floor((cv_hospital$age_end + cv_hospital$age_start)/2)
cv_hospital$num_age_bin <- as.numeric(cv_hospital$age_bin)

cv_hospital <- join(cv_hospital, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))

write.csv(cv_hospital[cv_hospital$ratio>1,], paste0(save_dir,"/FILEPATH", bundle_version_id,"_", date, ".csv"), row.names=F)

## mr-brt model for crosswalking literature (hospital based) to clinician diagnosed
##-------------------------------------------------------------------------------------------------
#New mr-brt
dat2 <- mr$MRData()

#No spline
dat2$load_df(data = cv_hospital,
             col_obs = "log_ratio",
             col_obs_se = "delta_log_se",
             col_study_id = "nid")

mod2 <- mr$MRBRT(data = dat2,
                 inlier_pct = 0.9,
                 cov_models = list(   
                   mr$LinearCovModel("intercept", use_re = TRUE)))

# Fit model
mod2$fit_model()

# Add summary to a save spot
mod_save <- mod2$summary()

# Check variance (Get draws from beta_soln, take standard deviation of draws, square result)
n_samples1 <- as.integer(1000)

samples1 <- mod2$sample_soln(
  sample_size = n_samples1
)

# Take standard deviation of draws
SD <- sd(samples1[[1]])

# square to get variance
SD2 <- SD^2

# Create save df
save_betas <- data.table()
save_betas <-cbind(save_betas, mod_save[[1]])
save_betas$beta_var <- SD2
setnames(save_betas, old = "intercept", new = "beta")
save_betas$crosswalk <- "cv_hospital"

# Save fitted model
dir.create(paste0(save_dir, "/FILEPATH/"), recursive = TRUE)
py_save_object(
  object=mod2,
  filename=file.path(paste0(save_dir, "/FILEPATH.pkl")),
  pickle="dill"
)

# Create beta summary
beta_samples_log <- as.data.table(samples1[[1]])
setnames(beta_samples_log, names(beta_samples_log), names(mod2$summary()[[1]]))
beta_samples_log <- melt.data.table(
  beta_samples_log,
  variable.name = "covariate",
  value.name = "beta_sample_val"
)
beta_samples_lin <- copy(beta_samples_log)
beta_samples_lin[, beta_sample_val := exp(beta_sample_val)]
beta_samples_log[, scale := "log"]
beta_samples_lin[, scale := "lin"]
beta_samples <- rbind(beta_samples_log, beta_samples_lin)
beta_summary <- beta_samples[
  ,
  .(
    mean = mean(beta_sample_val),
    lower = quantile(beta_sample_val, 0.025),
    upper = quantile(beta_sample_val, 0.975)
  ),
  by = c("covariate", "scale")
]

beta_summary <- beta_summary[scale == "log", gamma := mod2$gamma_soln]
beta_summary <- beta_summary[scale == "lin", gamma := exp(mod2$gamma_soln)]

dir.create(paste0(save_dir, "/FILEPATH/"), recursive = TRUE)
data.table::fwrite(
  x = beta_summary,
  file = paste0(save_dir, "/FILEPATH.csv"),
  row.names = F
)


# Make prediction dataframe
df_pred <- data.frame(expand.grid(intercept = 1, age_mid= seq(0,100,1)))
dat_pred <- mr$MRData()

dat_pred$load_df(
  data = df_pred, 
  col_covs=list('age_mid') # Covariate
)

# Make predictions using new model
df_pred$pred1 <- mod2$predict(data = dat_pred)
head(df_pred, n = 5)

# Get uncertainty using mod2 draws
draws1 <- mod2$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

df_pred$pred_pt <- mod2$predict(data = dat_pred)
df_pred$pred_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
df_pred$pred_up <- apply(draws1, 1, function(x) quantile(x, 0.975))

# Calculate log se
df_pred$log_se <- (df_pred$pred_up - df_pred$pred_lo) / 2 / qnorm(0.975)

# Convert the mean and standard_error to linear space
df_pred$hospital_linear_se <- sapply(1:nrow(df_pred), function(i) {
  ratio_i <- df_pred[i, "pred_pt"]
  ratio_se_i <- df_pred[i, "log_se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
df_pred$hospital_ratio <- exp(df_pred$pred_pt)

# rename
df_pred$log_hospital_ratio <- df_pred$pred_pt
df_pred$log_hospital_se <- df_pred$log_se

# get a data frame with estimated weights
df_mod2 <- cbind(mod2$data$to_df(), data.frame(w = mod2$w_soln)) 

# merge data frame to cv_hospital
setnames(df_mod2, old = c("study_id", "obs", "obs_se"), new = c("nid", "log_ratio", "delta_log_se"))
df_mod2 <- as.data.table(df_mod2)

cv_hospital <- merge(cv_hospital, df_mod2[,.( nid, log_ratio, delta_log_se, w)], 
                     by = c("nid", "log_ratio", "delta_log_se"))


# Keep a record of the values
p <- df_pred[,c("hospital_ratio","hospital_linear_se","pred_pt","pred_lo","pred_up")]
p <- unique(p)
p$count_obs <- c(length(cv_hospital$ratio))
p$variable <- "cv_hospital"

# betas, gamma_soln
xwvals <- data.table()
xwvals <- rbind(xwvals, save_betas) # betas
xwvals$gamma_soln <- mod2$gamma_soln

# Compute the UIs
lower <- as.numeric(quantile(samples1[[1]], .025, na.rm=TRUE))
upper <- as.numeric(quantile(samples1[[1]], .975, na.rm=TRUE))

# Add to save sheet
xwvals$beta_lower <- lower
xwvals$beta_upper <- upper

# Join back with master data_frame
lri$hospital_ratio <- unique(df_pred$hospital_ratio)
lri$hospital_linear_se <- unique(df_pred$hospital_linear_se)
lri$log_hospital_ratio <- unique(df_pred$log_hospital_ratio)
lri$log_hospital_se <- unique(df_pred$log_hospital_se)

# Make sure that the master data frame keeps our changes in df (regarding NIDs)
lri <- as.data.table(lri)
lri <- lri[nid == 108883, is_reference := 0]
lri <- lri[nid == 108883, cv_hospital := 1]
lri <- lri[is.na(cv_hospital), cv_hospital := 0]

# Outlier
lri <- lri[!nid == 324119]

# Return to data.frame. 
lri <- as.data.frame(lri)

# Check for no NAs in reference
table(is.na(lri$is_reference))
table((lri$is_reference))

# Check for NAs in mean
table(is.na(lri$mean))

# save preds
df_pred$variable <- "cv_hospital"
predvals <- data.frame()
predvals <- rbind(predvals, df_pred)
setnames(predvals, c("hospital_linear_se","hospital_ratio","log_hospital_ratio","log_hospital_se"), c("linear_se","ratio","log_ratio","log_se"))

######################################################################
## Self-reported crosswalk. 
######################################################################


surveys <- read.csv(paste0(save_dir,"/FILEPATH", bundle_version_id,"_", date, ".csv"))
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

cv_selfreport <- bundle_crosswalk_collapse(lri, covariate_name="cv_diag_selfreport", age_cut=age_bins, year_cut=c(years), merge_type="between", location_match="country", release = release)

cv_selfreport$year_id <- floor((cv_selfreport$year_end + cv_selfreport$year_start)/2)
cv_selfreport$age_mid <- floor((cv_selfreport$age_end + cv_selfreport$age_start)/2)
cv_selfreport$ihme_loc_id <- cv_selfreport$location_match

cv_selfreport <- join(cv_selfreport, locs[,c("location_id","super_region_name","ihme_loc_id")], by="ihme_loc_id")
cv_selfreport <- join(cv_selfreport, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
cv_selfreport$row <- 1:length(cv_selfreport$location_id)

# Save this file.
write.csv(cv_selfreport[cv_selfreport$ratio<1,], paste0(save_dir,"/FILEPATH", bundle_version_id,"_", date, ".csv"), row.names=F)
## Run an MR-BRT model. No covariates used##
## Covariate: none
##-------------------------------------------------------------------------------------------------

dat3 <- mr$MRData()
dat3$load_df(data = cv_selfreport[cv_selfreport$ratio<1,],
             col_obs = "log_ratio",
             col_obs_se = "delta_log_se",
             col_study_id = "nid")

mod3 <- mr$MRBRT(data = dat3,
                 inlier_pct = 0.9,
                 cov_models = list(mr$LinearCovModel("intercept", use_re = TRUE)))

# Fit model
mod3$fit_model()

# Add summary to a save spot
mod_save <- mod3$summary()

# Check variance (Get draws from beta_soln, take standard deviation of draws, square result)
n_samples1 <- as.integer(1000)

samples1 <- mod3$sample_soln(
  sample_size = n_samples1
)

# Take standard deviation of draws
SD <- sd(samples1[[1]])

# square to get variance
SD2 <- SD^2

# Create save df
save_betas <- data.table()
save_betas <-cbind(save_betas, mod_save[[1]])
save_betas$beta_var <- SD2
setnames(save_betas, old = "intercept", new = "beta")
save_betas$crosswalk <- "cv_selfreport"

# Save fitted model
py_save_object(
  object=mod3,
  filename=file.path(paste0(save_dir, "FILEPATH.pkl")),
  pickle="dill"
)

# Create beta summary
beta_samples_log <- as.data.table(samples1[[1]])
setnames(beta_samples_log, names(beta_samples_log), names(mod3$summary()[[1]]))
beta_samples_log <- melt.data.table(
  beta_samples_log,
  variable.name = "covariate",
  value.name = "beta_sample_val"
)
beta_samples_lin <- copy(beta_samples_log)
beta_samples_lin[, beta_sample_val := exp(beta_sample_val)]
beta_samples_log[, scale := "log"]
beta_samples_lin[, scale := "lin"]
beta_samples <- rbind(beta_samples_log, beta_samples_lin)
beta_summary <- beta_samples[
  ,
  .(
    mean = mean(beta_sample_val),
    lower = quantile(beta_sample_val, 0.025),
    upper = quantile(beta_sample_val, 0.975)
  ),
  by = c("covariate", "scale")
]

beta_summary <- beta_summary[scale == "log", gamma := mod3$gamma_soln]
beta_summary <- beta_summary[scale == "lin", gamma := exp(mod3$gamma_soln)]

dir.create(paste0(save_dir, "/FILEPATH/"), recursive = TRUE)
data.table::fwrite(
  x = beta_summary,
  file = paste0(save_dir, "/FILEPATH.csv"),
  row.names = F
)


# Make prediction dataframe
df_pred <- data.frame(expand.grid(intercept = 1, age_mid= seq(0,5,1)))
dat_pred <- mr$MRData()

dat_pred$load_df(
  data = df_pred,
  col_covs = list("age_mid")
)

# Make predictions using draws
df_pred$pred1 <- mod3$predict(data = dat_pred)
head(df_pred, n = 5)


# Get uncertainty 
draws1 <- mod3$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

df_pred$pred_pt <- mod3$predict(data = dat_pred)
df_pred$pred_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
df_pred$pred_up <- apply(draws1, 1, function(x) quantile(x, 0.975))

# Calculate log se
df_pred$log_se <- (df_pred$pred_up - df_pred$pred_lo) / 2 / qnorm(0.975)

# Convert the mean and standard_error to linear space
df_pred$selfreport_linear_se <- sapply(1:nrow(df_pred), function(i) {
  ratio_i <- df_pred[i, "pred_pt"]
  ratio_se_i <- df_pred[i, "log_se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
df_pred$selfreport_ratio <- exp(df_pred$pred_pt)

# rename
df_pred$log_ratio <- df_pred$pred_pt

# get a data frame with estimated weights
df_mod3 <- cbind(mod3$data$to_df(), data.frame(w = mod3$w_soln)) 

# set names
setnames(df_mod3, old = c("study_id", "obs", "obs_se"), new = c("nid", "log_ratio", "delta_log_se"))
df_mod3 <- as.data.table(df_mod3)

# Add outliers trimmed using df_mod3 weights
df_mod3$outlier <- ceiling(abs(df_mod3$w - 1))

# Merge back onto cv_selfreport
cv_selfreport <- as.data.table(cv_selfreport)

# rounding to 7 places. 
cv_selfreport <- cv_selfreport[, log_ratio := round(log_ratio, 7)]
cv_selfreport <- cv_selfreport[, delta_log_se := round(delta_log_se, 7)]

df_mod3 <- df_mod3[, log_ratio := round(log_ratio, 7)]
df_mod3 <- df_mod3[, delta_log_se := round(delta_log_se, 7)]

cv_selfreport <- merge(cv_selfreport, df_mod3[,.(nid, log_ratio, delta_log_se, w, outlier)],
                       by = c("nid", "log_ratio", "delta_log_se"))

# Join on location information
cv_selfreport <- join(cv_selfreport, locs[,c("location_id","location_name")], by="location_id")
mod_data <- copy(cv_selfreport)

# Keep a record of the values
p <- df_pred[,c("selfreport_ratio","selfreport_linear_se","pred_pt","pred_lo","pred_up")]
p <- unique(p)
p$count_obs <- c(length(cv_selfreport$ratio))
p$variable <- "cv_selfreport"

# Documentation needs betas, gamma_soln
xwvals <- rbind.fill(xwvals, save_betas) # betas
xwvals$gamma_soln <- mod3$gamma_soln # gamma

# Compute the UIs
lower <- as.numeric(quantile(samples1[[1]], .025, na.rm=TRUE))
upper <- as.numeric(quantile(samples1[[1]], .975, na.rm=TRUE))

# Add to save sheet
xwvals <- as.data.table(xwvals)
xwvals[crosswalk == "cv_selfreport" , "beta_lower"] <- lower
xwvals[crosswalk == "cv_selfreport" , "beta_upper"] <- upper

# Join back with master data_frame
lri$selfreport_ratio <- unique(df_pred$selfreport_ratio)
lri$selfreport_linear_se <- unique(df_pred$selfreport_linear_se)

df_pred$variable <- "cv_selfreport"
setnames(df_pred, old = c("selfreport_linear_se", "selfreport_ratio"), new = c("linear_se", "ratio"))

# Join predvals to save 
predvals <- rbind.fill(predvals, df_pred)

####################################################################################
## Perform actual crosswalk for hospital and self report (survey), save results for upload ##
####################################################################################
# checkpoint
lri_checkpoint <- copy(lri)
lri <- copy(lri_checkpoint)

lri$raw_mean <- lri$mean
lri$raw_standard_error <- lri$standard_error
lri$crosswalk_type <- ifelse(lri$cv_hospital==1, "Hospitalized", ifelse(lri$cv_diag_selfreport==1, "Self-reported","No crosswalk"))

#####################################
## Convert the mean by the crosswalk
# Hospital
lri <- as.data.table(lri)
lri[cv_hospital == 1, mean := mean*hospital_ratio]
lri[cv_hospital == 1, standard_error := sqrt(standard_error^2 * hospital_linear_se^2 + standard_error^2*mean^2 + hospital_linear_se^2*raw_mean^2)]
lri$note_modeler <- ifelse(lri$cv_hospital==1, paste0("This data point was adjusted for being in a hospitalized sample population. The code to produce
                                                      this estimated ratio is from prepare_crosswalks_selfreport_hosp_mr-brt.R. This occurred in MR-BRT.
                                                      The original mean was ", round(lri$raw_mean,3),". ", lri$note_modeler), lri$note_modeler)
# Self-report
lri[cv_diag_selfreport == 1, mean := mean*selfreport_ratio]
lri[cv_diag_selfreport == 1, standard_error := sqrt(standard_error^2 * selfreport_linear_se^2 + standard_error^2*mean^2 + selfreport_linear_se^2*raw_mean^2)]
lri$note_modeler <- ifelse(lri$cv_diag_selfreport==1, paste0("This data point was adjusted for being from a self-reported sample population (typically surveys). The code to produce
                                                             this estimated ratio is from prepare_crosswalks_selfreport_hosp_mr-brt.R. This occurred in MR-BRT.
                                                             The original mean was ", round(lri$raw_mean,3),". ", lri$note_modeler), lri$note_modeler)

## Pull out data only used to inform crosswalk
lri <- subset(lri, nid!=999999)

lri <- as.data.frame(lri)

## Make sure data changed
table(lri$is_reference)

## Pull out the data from this crosswalk
lit_survey_output <- lri

colnames(lit_survey_output) <- make.unique(names(lit_survey_output))

## Save
write.csv(lri, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)

#######################################################################
## CLINICAL - Inpatient and Claims
#######################################################################

# Bring in full dataset again
lri <- as.data.table(read.csv(paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date,".csv")))

## Outlier Nepal NID as it represents rural Nepal, not all of Nepal
lri <- lri[(nid == 107299), is_outlier := 1]

## Outlier severe pneumonia row from NID 108883 as the pneumo row already contains severe pneumo
lri <- lri[(nid == 108883 & case_name == "severe pneumonia"), is_outlier := 1]

## Outlier row with a rural population (that would later become a reference study
lri <- lri[nid == 108889, is_outlier := 1]

lri <- lri[clinical_data_type == "inpatient", cv_inpatient := 1]
lri <- lri[clinical_data_type == "claims", cv_marketscan := 1]

lri <- lri[measure %in% c("incidence", "prevalence")]

# Subset to inpatient and claims
clin_df <- subset(lri, clinical_data_type!="")

table(clin_df$clinical_data_type)
table(clin_df$measure)


clin_df$is_reference <- 0
clin_df$extractor <- "clinical_team"

table(is.na(clin_df$is_reference))
table(clin_df$is_reference)

# Convert inc to prevalence
inc <- clin_df[measure == "incidence"] 

# meta-analysis for duration of LRI
duration <- read.csv(paste0(h,"/FILEPATH")) 

# Convert everything to actual duration (divide by 1 year)
duration_val   <- duration$mean/365.2422

duration <- as.data.table(duration)
test <- duration[, c(paste0("duration_", 1:1000))]
test <- test/365.2422
duration[,  c(paste0("duration_", 1:1000)) := test[, c(paste0("duration_", 1:1000))]]

duration[, duration_lower:= apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = (paste0("duration_", 1:1000))]
duration[, duration_upper:= apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = (paste0("duration_", 1:1000))]

duration_upper <- duration$duration_upper
duration_lower <- duration$duration_lower

duration_se <- (duration_upper - duration_lower) / (2*1.96)

inc$prev <- inc$mean * duration_val
inc$standard_error <- inc$prev*sqrt((inc$standard_error/inc$mean)^2+(duration_se/duration_val)^2)

inc$mean <- inc$prev
inc <- subset(inc, select = c(-prev))
inc$measure <- "prevalence"
inc$cases <- inc$mean * inc$sample_size # also convert cases after the prevalence conversion

inc$note_modeler <- paste0("Converted from incidence to prevalence using mean duration ", duration_val)
inc$measure <- "prevalence"

# Compute upper, lower UIs
inc$lower <- inc$mean - (1.96 * inc$standard_error)
inc$upper <- inc$mean + (1.96 * inc$standard_error)

# remove rows that were converted from overall bundle variable
dt_lri_without_inc <- clin_df[!(measure == "incidence")]

# combine together
clin_df <- rbind(dt_lri_without_inc, inc)

table(clin_df$measure) # Should not have incidence

## subset clin_df based on NID
`%notin%` <- Negate(`%in%`)

nid_out <- c(281819, 337219, 354896, 292575, 292437)

clin_df <- clin_df[nid %notin% nid_out,]

## Unoutlier ANY clinical inpatient data
clin_df <- clin_df[!(clinical_data_type == ""), is_outlier := 0]

## Separate out non-clinical data and recode
lri_bundle <- subset(lri, clinical_data_type == "")
table(is.na(lri_bundle$is_reference))
table((lri_bundle$is_reference))

lri_bundle$cv_hospital[is.na(lri_bundle$cv_hospital)] <- 0
lri_bundle$cv_inpatient <- 0
lri_bundle$cv_diag_selfreport[is.na(lri_bundle$cv_diag_selfreport)] <- 0
lri_bundle$is_reference <- with(lri_bundle, ifelse(cv_hospital == 0 & cv_inpatient == 0 & cv_diag_selfreport == 0 & cv_marketscan == 0 & cv_inpatient_sample == 0, 1, 0))

## check to make sure there are no NAs in the reference column
table(is.na(lri_bundle$is_reference))
table(lri_bundle$is_reference) 
table(lri$is_reference)
nrow(lri[cv_hospital == 0 & cv_inpatient == 0 & cv_diag_selfreport == 0 & cv_marketscan == 0 & cv_inpatient_sample == 0]) ## -----> matches number of 'is_reference'

lri_bundle[is.na(is_reference), is_reference := 0]

table(lri_bundle$source_type)
table(lri_bundle$cv_inpatient)
table(lri_bundle$cv_marketscan)

# Used in crosswalk
lri_full <- rbind.fill(lri_bundle, clin_df)

## Two studies are marked as reference that should not be, fixing that here
lri_full <- as.data.table(lri_full)
lri_full <- lri_full[nid == 221153, is_reference :=0]
lri_full <- lri_full[nid == 261880, is_reference :=0]
lri_full <- as.data.frame(lri_full)

## Unoutlier two brazil studies
lri_full <- as.data.table(lri_full)
lri_full <- lri_full[nid == 94493, is_outlier := 0]
lri_full <- lri_full[nid == 119930 , is_outlier := 0]
lri_full <- as.data.frame(lri_full)

table(lri_full$is_reference)
table(is.na(lri_full$is_reference))
table(lri_full$cv_inpatient)

# Data to be saved
output_data <- lri_full

table(output_data$is_reference)
table(is.na(output_data$is_reference))
table(output_data$measure)

write.csv(output_data, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)


# Join with SDI as it is a predictor for cv_hospital
lri_full$year_id <- floor((lri_full$year_start + lri_full$year_end) / 2)
lri_full <- join(lri_full, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
lri_full$sdi <- round(lri_full$sdi,2)

## Set age bins
age_bins <- c(0,1,seq(5,100,5))

# Calculate cases and sample size (missing in inpatient data)
table(is.na(lri_full$cases[lri_full$clinical_data_type !=""]))
table(is.na(lri_full$sample_size[lri_full$clinical_data_type !=""]))

sample_size <- with(lri_full, mean*(1-mean)/standard_error^2)
cases <- lri_full$mean * sample_size
lri_full$cases <- ifelse(is.na(lri_full$cases), cases, lri_full$cases)
lri_full$sample_size <- ifelse(is.na(lri_full$sample_size), sample_size, lri_full$sample_size)

table(lri_full$cases[lri_full$cases < 0])

## Subset to working data frame ##
df <- lri_full[,c("location_name", "measure", "location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                  "cv_diag_selfreport","cv_hospital","cv_inpatient","cv_marketscan","is_reference","is_outlier","group_review", "clinical_data_type")]

df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")
df$group_review[is.na(df$group_review)] <- 1
df$cv_marketscan <- as.numeric(df$cv_marketscan)

df <- as.data.table(df)
unique(df$cv_marketscan)
unique(df$cv_inpatient)
table(df$cv_marketscan)
table(df$cv_inpatient)
df[is.na(cv_marketscan), cv_marketscan := 0]
df[is.na(cv_inpatient), cv_inpatient := 0]
df <- df[nid == 266178, is_reference := 0]
df <- as.data.frame(df)


cv_inpatient <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient", age_cut=age_bins, year_cut=c(years), merge_type="between", location_match="country", include_logit = T, release = release)
cv_inpatient$age_mid <- floor((cv_inpatient$age_end + cv_inpatient$age_start)/2)
cv_inpatient$match_id <- paste0(cv_inpatient$nid, "_", cv_inpatient$n_nid)
cv_inpatient$year_id <- round((cv_inpatient$year_end + cv_inpatient$year_start)/2,0)
cv_inpatient$ihme_loc_id <- cv_inpatient$location_match
cv_inpatient$location_id <- cv_inpatient$location_match
cv_inpatient <- join(cv_inpatient, sdi[,c("ihme_loc_id","sdi","year_id")], by=c("ihme_loc_id","year_id"))

# These matched pairs are to be removed
cv_inpatient <- as.data.table(cv_inpatient)
nids_exclude <- c("96699", "221135", "270407", "270573")
cv_inpatient <- cv_inpatient[!(nid %in% nids_exclude)]
cv_inpatient <- as.data.frame(cv_inpatient)

cv_marketscan <- bundle_crosswalk_collapse(df, covariate_name="cv_marketscan", age_cut=age_bins, year_cut=c(years), merge_type="between", location_match="country", include_logit = T, release = release)

cv_marketscan$age_mid <- floor((cv_marketscan$age_end + cv_marketscan$age_start)/2)
cv_marketscan$match_id <- paste0(cv_marketscan$nid, "_", cv_marketscan$n_nid)
cv_marketscan$year_id <- round((cv_marketscan$year_end + cv_marketscan$year_start)/2,0)
cv_marketscan$ihme_loc_id <- cv_marketscan$location_match
cv_marketscan$location_id <- cv_marketscan$location_match

write.csv(cv_inpatient, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)
write.csv(cv_marketscan, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)
##########################################################################
## Model for clinical inpatient data ##
##########################################################################
## mr-brt model for crosswalking inpatient to clinician diagnosed
## Covariate: sdi, spline with 3 internal knots on frequency.
##-------------------------------------------------------------------------------------------------
#New mr-brt

dat2 <- mr$MRData()
dat2$load_df(data = cv_inpatient,
             col_obs = "logit_ratio",
             col_obs_se = "logit_ratio_se",
             col_study_id = "match_id",
             col_covs = list("sdi"))

mod2 <- mr$MRBRT(data = dat2,
                 inlier_pct = 0.9,
                 cov_models = list(
                   mr$LinearCovModel("sdi", use_spline = TRUE, spline_degree = 2L, spline_knots_type="frequency", 
                                     spline_knots = array(seq(0, 1, length.out = 5)),
                                     spline_l_linear = TRUE, spline_r_linear = TRUE,
                                     prior_spline_convexity = "convex", prior_spline_monotonicity = "decreasing"), 
                   mr$LinearCovModel("intercept", use_re = TRUE)))


# Fit model
mod2$fit_model()

# Add summary to a save spot
mod_save <- mod2$summary()

# Check variance (Get draws from beta_soln, take standard deviation of draws, square result)
n_samples1 <- as.integer(1000)

samples1 <- mod2$sample_soln(
  sample_size = n_samples1
)

# Take standard deviation of draws
SD <- sd(samples1[[1]])

# square to get variance
SD2 <- SD^2

# Create save df
save_betas <- data.table()
save_betas <-cbind(save_betas, mod_save[[1]])
save_betas$beta_var <- SD2
setnames(save_betas, old = "intercept", new = "beta")
save_betas$crosswalk <- "cv_inpatient"

# Save fitted model
py_save_object(
  object=mod2,
  filename=file.path(paste0(save_dir, "/FILEPATH.pkl")),
  pickle="dill"
)

# Create beta summary
beta_samples_log <- as.data.table(samples1[[1]])
setnames(beta_samples_log, names(beta_samples_log), names(mod2$summary()[[1]]))
beta_samples_log <- melt.data.table(
  beta_samples_log,
  variable.name = "covariate",
  value.name = "beta_sample_val"
)
beta_samples_lin <- copy(beta_samples_log)
beta_samples_lin[, beta_sample_val := exp(beta_sample_val)]
beta_samples_log[, scale := "logit"]
beta_samples_lin[, scale := "lin"]
beta_samples <- rbind(beta_samples_log, beta_samples_lin)
beta_summary <- beta_samples[
  ,
  .(
    mean = mean(beta_sample_val),
    lower = quantile(beta_sample_val, 0.025),
    upper = quantile(beta_sample_val, 0.975)
  ),
  by = c("covariate", "scale")
]

beta_summary <- beta_summary[scale == "logit", gamma := mod3$gamma_soln]
beta_summary <- beta_summary[scale == "lin", gamma := exp(mod3$gamma_soln)]

dir.create(paste0(save_dir, "/FILEPATH/"), recursive = TRUE)
data.table::fwrite(
  x = beta_summary,
  file = paste0(save_dir, "/FILEPATH.csv"),
  row.names = F
)

# Make prediction dataframe
df_pred <- data.frame(sdi = seq(0, 1, length.out = 101))
dat_pred <- mr$MRData()

dat_pred$load_df(
  data = df_pred, 
  col_covs=list('sdi') # Covariate
)

# Make predictions using new model
df_pred$pred1 <- mod2$predict(data = dat_pred)
head(df_pred, n = 5)

# Get uncertainty using mod2 draws
draws1 <- mod2$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

df_pred$pred_pt <- mod2$predict(data = dat_pred)
df_pred$pred_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
df_pred$pred_up <- apply(draws1, 1, function(x) quantile(x, 0.975))

# Calculate log se
df_pred$log_se <- (df_pred$pred_up - df_pred$pred_lo) / 2 / qnorm(0.975)

# Convert the mean and standard_error to linear space
df_pred$inpatient_linear_se <- sapply(1:nrow(df_pred), function(i) {
  ratio_i <- df_pred[i, "pred_pt"]
  ratio_se_i <- df_pred[i, "log_se"] 
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
df_pred$inpatient_ratio <- inv.logit(df_pred$pred_pt)
df_pred$inpatient_logit <- df_pred$pred_pt

# Write out preds
fwrite(df_pred, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"))

# get a data frame with estimated weights
df_mod2 <- cbind(mod2$data$to_df(), data.frame(w = mod2$w_soln)) 

# merge data frame to cv_inpatient
setnames(df_mod2, old = c("study_id", "obs", "obs_se"), new = c("match_id", "logit_ratio", "logit_ratio_se"))
df_mod2 <- as.data.table(df_mod2)
cv_inpatient <- as.data.table(cv_inpatient)

#rounding to 7 places. 
cv_inpatient <- cv_inpatient[, logit_ratio := round(logit_ratio, 7)]
cv_inpatient <- cv_inpatient[, logit_ratio_se := round(logit_ratio_se, 7)]

df_mod2 <- df_mod2[, logit_ratio := round(logit_ratio, 7)]
df_mod2 <- df_mod2[, logit_ratio_se := round(logit_ratio_se, 7)]

chk <- copy(cv_inpatient)
cv_inpatient <- merge(cv_inpatient, df_mod2[,.(sdi, match_id, logit_ratio, logit_ratio_se, w)], 
                      by = c("match_id", "sdi", "logit_ratio", "logit_ratio_se"))

### Rejoin inpatient ratio with clinical data  ###
# Data to be saved
rm(output_data)
output_data <- copy(lri_full)
output_data$age_mid <- ceiling((output_data$age_end + output_data$age_start)/2)

output_data$year_id <- round((output_data$year_start + output_data$year_end)/2,0)
output_data <- join(output_data, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
output_data$sdi <- round(output_data$sdi, 2)
df_pred$sdi <- round(df_pred$sdi, 2)
output_data <- join(output_data, df_pred[,c("sdi","inpatient_ratio","inpatient_linear_se","inpatient_logit", "log_se")], by=c("sdi"))

# Keep a record of the values
p <- df_pred[,c("inpatient_logit","pred_pt","pred_lo","pred_up")]
p <- unique(p)
p$count_obs <- c(length(cv_inpatient$ratio))
p$variable <- "cv_inpatient"

# Documentation needs betas, gamma_soln
xwvals <- rbind.fill(xwvals, save_betas) # betas
xwvals$gamma_soln <- mod2$gamma_soln # gamma

# Compute the UIs
lower <- as.numeric(quantile(samples1[[1]], .025, na.rm=TRUE))
upper <- as.numeric(quantile(samples1[[1]], .975, na.rm=TRUE))

# Add to save sheet
xwvals <- as.data.table(xwvals)
xwvals[crosswalk == "cv_inpatient" , "beta_lower"] <- lower
xwvals[crosswalk == "cv_inpatient" , "beta_upper"] <- upper


df_pred$variable <- "cv_inpatient"
setnames(df_pred, old = c("inpatient_linear_se", "inpatient_ratio", "inpatient_logit"), new = c("linear_se", "ratio", "logit"), skip_absent = TRUE)

# Join predvals to save 
predvals <- rbind.fill(predvals, df_pred)

##########################################################################
## Model for marketscan data ##
##########################################################################

## mr-brt model for crosswalking claims data to clinician diagnosed
## Covariate: None
##-------------------------------------------------------------------------------------------------

#New mr-brt
dat2 <- mr$MRData()
dat2$load_df(data = cv_marketscan,
             col_obs = "logit_ratio",
             col_obs_se = "logit_ratio_se",
             col_study_id = "match_id")

mod2 <- mr$MRBRT(data = dat2,
                 inlier_pct = 0.9,
                 cov_models = list(   
                   mr$LinearCovModel("intercept", use_re = TRUE)))


# Fit model
mod2$fit_model()

# Add summary to a save spot
mod_save <- mod2$summary()

# Check variance (Get draws from beta_soln, take standard deviation of draws, square result)
n_samples1 <- as.integer(1000)

samples1 <- mod2$sample_soln(
  sample_size = n_samples1
)

# Take standard deviation of draws
SD <- sd(samples1[[1]])

# square to get variance
SD2 <- SD^2

# Create save df
save_betas <- data.table()
save_betas <-cbind(save_betas, mod_save[[1]])
save_betas$beta_var <- SD2
setnames(save_betas, old = "intercept", new = "beta")
save_betas$crosswalk <- "cv_marketscan"

# Save fitted model
py_save_object(
  object=mod2,
  filename=file.path(paste0(save_dir, "/FILEPATH.pkl")),
  pickle="dill"
)

# Create beta summary
beta_samples_log <- as.data.table(samples1[[1]])
setnames(beta_samples_log, names(beta_samples_log), names(mod2$summary()[[1]]))
beta_samples_log <- melt.data.table(
  beta_samples_log,
  variable.name = "covariate",
  value.name = "beta_sample_val"
)
beta_samples_lin <- copy(beta_samples_log)
beta_samples_lin[, beta_sample_val := exp(beta_sample_val)]
beta_samples_log[, scale := "logit"]
beta_samples_lin[, scale := "lin"]
beta_samples <- rbind(beta_samples_log, beta_samples_lin)
beta_summary <- beta_samples[
  ,
  .(
    mean = mean(beta_sample_val),
    lower = quantile(beta_sample_val, 0.025),
    upper = quantile(beta_sample_val, 0.975)
  ),
  by = c("covariate", "scale")
]

beta_summary <- beta_summary[scale == "logit", gamma := mod3$gamma_soln]
beta_summary <- beta_summary[scale == "lin", gamma := exp(mod3$gamma_soln)]

dir.create(paste0(save_dir, "/FILEPATH/"), recursive = TRUE)
data.table::fwrite(
  x = beta_summary,
  file = paste0(save_dir, "/FILEPATH.csv"),
  row.names = F
)

# Make prediction dataframe
df_pred <- data.frame(expand.grid(intercept = 1, age_mid= seq(0,100,1)))
#df_pred <- data.frame(intercept=1)
dat_pred <- mr$MRData()

dat_pred$load_df(
  data = df_pred, 
  col_covs=list('intercept') # Covariate
)

# Make predictions using new model
df_pred$pred1 <- mod2$predict(data = dat_pred)
head(df_pred, n = 5)

# Get uncertainty using mod2 draws
draws1 <- mod2$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

df_pred$pred_pt <- mod2$predict(data = dat_pred)
df_pred$pred_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
df_pred$pred_up <- apply(draws1, 1, function(x) quantile(x, 0.975))

# Calculate log se
df_pred$log_se <- (df_pred$pred_up - df_pred$pred_lo) / 2 / qnorm(0.975)

# Convert the mean and standard_error to linear space
df_pred$claims_linear_se <- sapply(1:nrow(df_pred), function(i) {
  ratio_i <- df_pred[i, "pred_pt"]
  ratio_se_i <- df_pred[i, "log_se"] 
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
df_pred$marketscan_ratio <- inv.logit(df_pred$pred_pt)
df_pred$marketscan_logit <- df_pred$pred_pt

# Write out preds
fwrite(df_pred, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"))

# get a data frame with estimated weights
df_mod2 <- cbind(mod2$data$to_df(), data.frame(w = mod2$w_soln)) 

# merge data frame to cv_marketscan
setnames(df_mod2, old = c("study_id", "obs", "obs_se"), new = c("match_id", "logit_ratio", "logit_ratio_se"))
df_mod2 <- as.data.table(df_mod2)
cv_marketscan <- as.data.table(cv_marketscan)

#rounding to 7 places. 
cv_marketscan <- cv_marketscan[, logit_ratio := round(logit_ratio, 7)]
cv_marketscan <- cv_marketscan[, logit_ratio_se := round(logit_ratio_se, 7)]

df_mod2 <- df_mod2[, logit_ratio := round(logit_ratio, 7)]
df_mod2 <- df_mod2[, logit_ratio_se := round(logit_ratio_se, 7)]


cv_marketscan <- merge(cv_marketscan, df_mod2[,.(match_id, logit_ratio, logit_ratio_se, w)], 
                       by = c("match_id", "logit_ratio", "logit_ratio_se"))

# Keep a record of the values
p <- df_pred[,c("marketscan_ratio","marketscan_logit", "claims_linear_se", "log_se","pred_pt","pred_lo","pred_up")]
p <- unique(p)
p$count_obs <- c(length(cv_marketscan$ratio))
p$variable <- "cv_marketscan"

# Documentation needs betas, gamma_soln
xwvals <- rbind.fill(xwvals, save_betas) # betas
xwvals$gamma_soln <- mod2$gamma_soln # gamma

# Compute the UIs
lower <- as.numeric(quantile(samples1[[1]], .025, na.rm=TRUE))
upper <- as.numeric(quantile(samples1[[1]], .975, na.rm=TRUE))

# Add to save sheet
xwvals <- as.data.table(xwvals)
xwvals[crosswalk == "cv_marketscan" , "beta_lower"] <- lower
xwvals[crosswalk == "cv_marketscan" , "beta_upper"] <- upper


df_pred$variable <- "cv_marketscan"
setnames(df_pred, old = c("claims_linear_se", "marketscan_ratio"), new = c("linear_se", "ratio"))

# Join predvals to save 
predvals <- rbind.fill(predvals, df_pred)

### Rejoin marketscan ratio with clinical data  ###
output_data$age_mid <- ceiling((output_data$age_end + output_data$age_start)/2)
output_data$marketscan_ratio <- unique(df_pred$ratio)
output_data$marketscan_linear_se <-  unique(df_pred$linear_se)
output_data$marketscan_logit <-  unique(df_pred$marketscan_logit)

################################################################################
## Perform actual crosswalk, save results for upload ##
################################################################################
output_data$raw_mean <- output_data$mean
output_data$raw_standard_error <- output_data$standard_error

#####################################
## Convert the mean by the crosswalk
table(output_data$measure)
summary(output_data$mean)

## Set zero values to a linear floor
output_data <- as.data.table(output_data)
output_data <- output_data[mean == 0, mean:= median(output_data$mean[output_data$mean>0]) * 0.01]
output_data <- output_data[mean < 1]
output_data <- as.data.frame(output_data)
summary(output_data$mean)
range(output_data$mean)

output_data$logit_mean <- logit(output_data$mean)
output_data$log_mean <- log(output_data$mean)

table(is.na(output_data$logit_mean))
which(is.nan(log(output_data$mean))) 
quantile(output_data$mean)
table(output_data$mean > 0)
table(output_data$mean > 0, output_data$clinical_data_type) ## look for zero values; will produce NaN in logits transformation
table(output_data$logit_mean > 0, output_data$clinical_data_type) 

chkpt <- copy(output_data)
chkpt <- as.data.table(chkpt)

## Claims crosswalk
output_data <- as.data.table(output_data)
output_data[clinical_data_type == "claims", mean:= inv.logit(logit_mean + marketscan_logit)]
output_data[clinical_data_type == "claims", standard_error := sqrt(standard_error^2 * marketscan_linear_se^2 + standard_error^2*mean^2 + marketscan_linear_se^2*raw_mean^2)]
output_data$note_modeler <- ifelse(output_data$clinical_data_type!="", paste0("This data point was adjusted for being in a clinical dataset. The code to produce this estimated ratio is from crosswalk_clinical_inpatient_mr-brt.R. This occurred in MR-BRT during decomposition step 2. The original mean was ",
                                                                              round(output_data$raw_mean,3),". ", output_data$note_modeler), output_data$note_modeler)

## Inpatient crosswalk
output_data[clinical_data_type == "inpatient", mean:= inv.logit(logit_mean + inpatient_logit)]
output_data[clinical_data_type == "inpatient", standard_error := sqrt(standard_error^2 * inpatient_linear_se^2 + standard_error^2*mean^2 + inpatient_linear_se^2*raw_mean^2)]
output_data$note_modeler <- ifelse(output_data$clinical_data_type!="", paste0("This data point was adjusted for being in a clinical dataset. The code to produce this estimated ratio is from crosswalk_clinical_inpatient_mr-brt.R. This occurred in MR-BRT during decomposition step 2. The original mean was ",
                                                                              round(output_data$raw_mean,3),". ", output_data$note_modeler), output_data$note_modeler)

colnames(output_data) <- make.unique(names(output_data))

## Save
write.csv(output_data, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)

## Collect output from this crosswalk.
clinical_output <- subset(output_data, clinical_data_type != "")

output_data <- as.data.table(output_data)
output_data[is.na(output_data)] <- ""

write.xlsx(clinical_output, paste0(save_dir,"FILEPATH", bundle_version_id, "_", date, ".xlsx"), rowNames=F)
write.csv(output_data, paste0(save_dir,"/FILEPATH", bundle_version_id, "_", date, ".csv"), row.names = FALSE)


#############################################
## Step 4: duplicate sex data ##
############################################
data_for_upload <- rbind.fill(clinical_output, lit_survey_output)

# Check that mean is not NA, is not greater than the upper UI, and is not less than the lower UI
data_for_upload <- as.data.table(data_for_upload)

# Some cases and sample sizes are missing from lit_survey_output, calculate them
sum(is.na(data_for_upload$cases))
sum(is.na(data_for_upload$sample_size))

sample_size <- with(data_for_upload, mean*(1-mean)/standard_error^2)
cases <- data_for_upload$mean * sample_size
data_for_upload$cases <- ifelse(is.na(data_for_upload$cases), cases, data_for_upload$cases)
data_for_upload$sample_size <- ifelse(is.na(data_for_upload$sample_size), sample_size, data_for_upload$sample_size)

sum(is.na(data_for_upload$cases))
sum(is.na(data_for_upload$sample_size))

table(data_for_upload$is_reference)
table(data_for_upload$is_outlier)

## Both sex data should be duplicated ##

data_for_upload$seq_parent <- ""
data_for_upload$crosswalk_parent_seq <- ""
sex_df <- duplicate_sex_rows(data_for_upload)

sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- as.numeric(sex_df$cases)

summary(sex_df$mean)

# Merge on output_data
output_data_merge <- output_data[,.(nid, field_citation_value, source_type, location_id, sex,
                                    year_start, year_end, age_start, age_end, mean, standard_error, seq, origin_seq)]
setnames(output_data_merge, old = c("mean", "standard_error"), new = c("temp_mean", "temp_standard_error"))
sex_df <- merge(sex_df, output_data_merge, by = c("nid", "field_citation_value", "source_type", "location_id",
                                                  "sex", "year_start", "year_end", "age_start", "age_end", "seq", "origin_seq"), all.x = TRUE)
sex_df$temp_mean <- as.numeric(sex_df$temp_mean)
sex_df$mean <- ifelse(is.na(sex_df$mean), sex_df$temp_mean, sex_df$mean)

summary(sex_df$mean) 

sex_df$group_review[is.na(sex_df$group_review)] <- ""
sex_df <- subset(sex_df, group_review != 0)
sex_df[is.na(sex_df)] <- ""
sex_df$group_review <- ifelse(sex_df$specificity=="","",1)
sex_df$uncertainty_type <- ifelse(sex_df$lower=="","", as.character(sex_df$uncertainty_type))
sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="","",sex_df$uncertainty_type_value)

# After sex splitting, sometimes cases > sample size

sex_df$cases <- as.numeric(sex_df$cases)
sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.99, sex_df$cases)
sex_df$mean <- as.numeric(sex_df$mean)

# Check if data are greater than 1

table(sex_df$mean > 1)


# Make sure cases < sample size

sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- as.numeric(sex_df$cases)
sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.9, sex_df$cases)
sex_df$xwalk_type <- ifelse(sex_df$clinical_data_type!="","Clinical", ifelse(sex_df$cv_hospital==1,"Literature hospital", ifelse(sex_df$cv_diag_selfreport==1,"Survey","None")))

sex_df <- as.data.frame(sex_df)
sex_df <- sex_df[, -which(names(sex_df) %in% c("cv_dhs","cv_whs","cv_nine_plus_test","cv_explicit_test",
                                               "cv_clin_data","unnamed..75","unnamed..75.1","survey","original_mean",
                                               "age_mid","linear_se","std_clinical","cv_inpatient_lit","hospital_linear_se",
                                               "log_hospital_ratio","log_hospital_se","inpatient_lit_ratio","cv_miscoded","inpatient_lit_linear_se","log_inpatient_lit_ratio",
                                               "log_inpatient_lit_se","parent_id","sdi","mean_original",
                                               "standard_error_original","cv_had_fever","indicator","region_name","super_region_name","ihme_loc_id.1",
                                               "selfreport_linear_se","exists_subnational","year_id","note_SR", "cv_diag_x.ray","cv_diag_x.ray.1"))]

# Assign seq_parent and crosswalk_parent_seq
sex_df$seq_parent <- ifelse(sex_df$seq_parent=="", sex_df$seq, sex_df$seq_parent)
sex_df$crosswalk_parent_seq <- ifelse(sex_df$crosswalk_parent_seq=="", sex_df$seq, sex_df$crosswalk_parent_seq)

## Opportunity to quickly look for outliers ##

sex_df <- join(sex_df, locs[,c("location_id","parent_id","level","region_name","super_region_name")], by="location_id")

#### OUTLIER DATA AND FINAL CLEAN ####

sex_df_b <-copy(sex_df)
table(sex_df$is_outlier)

## Add outliers from current best crosswalk version

crosswalk_version_id <- best_cw
df_best <- get_crosswalk_version(crosswalk_version_id)

table(df_best$is_outlier)

# get outliers from current best version
sex_df <- as.data.table(sex_df)
# outlier based off of nids rather than seq
sex_df[nid %in% unique(df_best[is_outlier == 1]$nid), is_outlier := 1]

table(sex_df$is_outlier)

## Central Kalimantan was outliered in GBD21, we are unoutliering for GBD23
sex_df <- sex_df[nid == 206640, is_outlier := 0]
## Same with Indonesia 
sex_df <- sex_df[nid == 439739, is_outlier := 0]

## Outlier Susenas

table(sex_df$nid[sex_df$nid %in% c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)])

sex_df <- subset(sex_df, !(nid %in% c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)))
table(sex_df$is_outlier)

sex_df <- as.data.table(sex_df)

sex_df$unit_value_as_published[is.na(sex_df$unit_value_as_publsihed)] <- 1
sex_df[is.na(unit_value_as_published), unit_value_as_published := 1]
sex_df[unit_value_as_published == "", unit_value_as_published := 1]


sex_df[recall_type == "", recall_type := "Point"]

sex_df[unit_type == "", unit_type := "Person"]

sex_df[urbanicity_type == "", urbanicity_type := "Unknown"]
sex_df[urbanicity_type == "unknown", urbanicity_type := "Unknown"]

sex_df$standard_error <- as.numeric(sex_df$standard_error)
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

setnames(sex_df, c("cv_diag_x.ray"), c("cv_diag_xray"), skip_absent = TRUE)
setnames(sex_df, c("cv_diag_x-ray"), c("cv_diag_xray"), skip_absent = TRUE)
setnames(sex_df, c("cv_diag_xray"), c("cv1_diag_xray"), skip_absent = TRUE)


### UPLOAD ###
table(sex_df$xwalk_type)
sex_df <- sex_df[recall_type_value == 0, recall_type_value := ""]

summary(sex_df$mean)
summary(sex_df[is_outlier == 0, mean])

table(sex_df$is_outlier)

sex_df <- sex_df[!(is.na(mean) & is.na(cases))]

# More outliering: Added in GBD23, three locations are experiencing issues.
# Hunan: All data points should be outliered here as they were in GBD21
sex_df <- sex_df[(location_id == 504), is_outlier := 1]

# Hong Kong Special Administrative Region of China.
sex_df <- sex_df[(location_id == 354 & nid == 438410 & year_start == 2011), is_outlier := 1]

# Biliran. Another implausibly high data point
sex_df <- sex_df[(location_id == 53586 & nid == 545866 & year_start == 2014), is_outlier := 1]

## Recall_type is wrong according to crosswalk_version 43654
sex_df <- sex_df[nid == 438414, recall_type := "Point"]
sex_df <- sex_df[recall_type == "Point", recall_type_value := 1]

sex_df[, lower := NA]
sex_df[, upper := NA]
sex_df[, uncertainty_type_value := NA]

## Make sure that CI data is unoutliered
sex_df[clinical_data_type %in% c("claims", "claims - flagged", "inpatient"), is_outlier := 0]

## Save a version with all data
write.xlsx(sex_df, paste0(upload_dir,"/FILEPATH",bundle_version_id,"_", date,".xlsx"), sheetName="extraction")
write.csv(sex_df, paste0(upload_dir,"/FILEPATH",bundle_version_id,"_", date,".csv") )

