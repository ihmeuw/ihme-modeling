########################################################################
## SURVEY CROSSWALKS
########################################################################


## set up
rm(list = ls())
pacman::p_load(plyr, openxlsx, ggplot2, metafor, msm, lme4, scales, data.table, boot, readxl, magrittr)

# Source all GBD shared functions at once
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Pull in crosswalk packages
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH") 
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

# Set-up variables
date <- gsub("-", "_", Sys.Date())
user <- Sys.info()["user"]
release <- 16

# Save directory
save_dir <- paste0("FILEPATH", date, "/")
dir.create(paste0(save_dir), recursive = TRUE)

# Source utilities
source(paste0("/FILEPATH/", user,"FILEPATH/bundle_crosswalk_collapse.R")) 
source(paste0("/FILEPATH/", user,"FILEPATH/convert_inc_prev_function.R")) 

set.seed(123)

# Function to add a covariate to the model
cov_info <- function(
    covariate, design_matrix, type = "", 
    prior_mean = 0, prior_var = "inf",
    bspline_prior_mean = NA, bspline_prior_var = NA,
    bspline_mono = NA, bspline_cvcv = NA, 
    degree = NA, n_i_knots = NA, 
    r_linear = NA, l_linear = NA){
  out <- data.frame(stringsAsFactors = FALSE,
                    covariate, design_matrix, 
                    prior_mean, prior_var, 
                    bspline_prior_mean, bspline_prior_var, 
                    bspline_mono, bspline_cvcv, 
                    degree, n_i_knots, r_linear, 
                    l_linear, type)
  return(out)
}

# Pull locations
locs <- as.data.table(get_location_metadata(location_set_id = 35, release_id = release))

# Pull age groups
age_groups <- get_ids("age_group")

# "not in" function
`%notin%` <- Negate(`%in%`)

################################################
## STEP 1 -- PULL BUNDLE VERSION AND GET DATA
################################################

## Check for most recent bundle
bundle_meta <- get_version_quota(bundle_id = 19)

## Get bundle version
bundle_version_id <- 47889 ## ----> change to desired bundle version
dt_lri <- as.data.table(get_bundle_version(bundle_version_id = bundle_version_id))

## Incidence or prevalence only
dt_lri <- dt_lri[measure %in% c("incidence", "prevalence")]

seq_all <- unique(dt_lri$seq)

######################################
## STEP 2 -- SURVEY DATA ADJUSTMENTS
######################################

# LRI DEFINITIONS:
## lri1. Cough, difficulty breathing, chest, fever
## lri2. Cough, difficulty breathing, chest
## lri3. Cough, difficulty breathing, fever
## lri4. Cough, difficulty breathing


df <- read.csv("FILEPATH") 
df <- merge(df, locs, by="ihme_loc_id")

## Set zero values to a linear floor
df$linear_floor_chest_symptoms <- median(df$chest_symptoms[df$chest_symptoms>0]) * 0.01
df$linear_floor_chest_fever <- median(df$chest_fever[df$chest_fever>0]) * 0.01
df$linear_floor_diff_fever <- median(df$diff_fever[df$diff_fever>0]) * 0.01
df$linear_floor_diff_breathing <- median(df$diff_breathing[df$diff_breathing>0]) * 0.01

df$chest_symptoms <- ifelse(df$chest_symptoms<=0, df$linear_floor_chest_symptoms, df$chest_symptoms)
df$chest_fever <- ifelse(df$chest_fever<=0, df$linear_floor_chest_fever, df$chest_fever)
df$diff_fever <- ifelse(df$diff_fever<=0, df$linear_floor_diff_fever, df$diff_fever)
df$diff_breathing <- ifelse(df$diff_breathing<=0, df$linear_floor_diff_breathing, df$diff_breathing)

## Define ratios of prevalence in various definitions
df$chest_fever_chest <- df$chest_fever / df$chest_symptoms
df$chest_fever_diff_fever <- df$chest_fever / df$diff_fever
df$chest_fever_diff <- df$chest_fever / df$diff_breathing
df$diff_fever_diff <- df$diff_fever / df$diff_breathing
df$chest_diff <- df$chest_symptoms / df$diff_breathing

## Add values to df_merge for vetting
df <- as.data.table(df)
df_merge <- merge(df_merge, df[,.(child_sex, age_year, ihme_loc_id, start_year, end_year, nid, sex,age_start,
                                  age_end, recall_period, sample_size, location, chest_fever_chest, chest_fever_diff_fever,
                                  chest_fever_diff, diff_fever_diff, chest_diff)],
                  by = c("child_sex", "age_year", "ihme_loc_id", "start_year", 'end_year', 'nid', 'sex', 'age_start',
                         'age_end', 'recall_period', "sample_size", 'location'))

vetting_dir <- paste0("FILEPATH", date)
if (!(dir.exists(vetting_dir))) { dir.create(vetting_dir, recursive = TRUE) }

write.csv(df_merge, paste0(vetting_dir, "FILEPATH"))

df <- as.data.frame(df)
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

df_chest_chest <- df[,c("sex","age_year","nid","end_year","location_id","chest_fever_chest","chest_fever_chest_se")]
df_chest_diff_fever <- df[,c("sex","age_year","nid","end_year","location_id","chest_fever_diff_fever","chest_fever_diff_fever_se")]
df_chest_diff <- df[,c("sex","age_year","nid","end_year","location_id","chest_fever_diff","chest_fever_diff_se")]

# rename
colnames(df_chest_chest)[6:7] <- c("ratio","standard_error")
colnames(df_chest_diff_fever)[6:7] <- c("ratio","standard_error")
colnames(df_chest_diff)[6:7] <- c("ratio","standard_error")

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

# Great! Now we need these ratios in log space
cv_survey$log_ratio <- log(cv_survey$ratio)

cv_survey$log_se <- sapply(1:nrow(cv_survey), function(i) {
  ratio_i <- cv_survey[i, "ratio"]
  ratio_se_i <- cv_survey[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

# Save
write.csv(cv_survey, paste0(save_dir,"FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)

# run in MR-BRT
cov_names <- c("cv_chest","cv_diff_fever","cv_diff")
covs1 <- list()
for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))

############################
# survey_chest MR-BRT model
############################
dat2 <- mr$MRData()
dat2$load_df(data = cv_survey[cv_survey$cv_chest==1,],
             col_obs = "log_ratio",
             col_obs_se = "log_se",
             col_study_id = "nid")

mod2 <- mr$MRBRT(data = dat2,
                 inlier_pct = 0.9,
                 cov_models = list(
                   mr$LinearCovModel("intercept", use_re = TRUE)))

# Fit model
mod2$fit_model(outer_max_iter = 5000L, inner_max_iter = 1000L)

# Check variance (Get draws from beta_soln, take standard deviation of draws, square result)
n_samples1 <- as.integer(1000)

samples1 <- mod2$sample_soln(
  sample_size = n_samples1
)

# Take standard deviation of draws
SD <- sd(samples1[[1]])

# square to get variance
SD2 <- SD^2

# Compute the UIs
lower <- as.numeric(quantile(samples1[[1]], .025, na.rm=TRUE))
upper <- as.numeric(quantile(samples1[[1]], .975, na.rm=TRUE))

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

#################################
# survey_diff_fever MR-BRT model
#################################
dat3 <- mr$MRData()
dat3$load_df(data = cv_survey[cv_survey$cv_diff_fever==1,],
             col_obs = "log_ratio",
             col_obs_se = "log_se",
             col_study_id = "nid")

mod3 <- mr$MRBRT(data = dat3,
                 inlier_pct = 0.9,
                 cov_models = list(
                   mr$LinearCovModel("intercept", use_re = TRUE)))

# Fit model
mod3$fit_model(outer_max_iter = 5000L, inner_max_iter = 1000L)

# Add summary to a save spot
mod_save <- mod3$summary()

# Check variance (Get draws from beta_soln, take standard deviation of draws, square result)
n_samples1 <- as.integer(1000)

samples3 <- mod3$sample_soln(
  sample_size = n_samples1
)

# Take standard deviation of draws
SD <- sd(samples3[[1]])

# square to get variance
SD2 <- SD^2

# Compute the UIs
lower <- as.numeric(quantile(samples3[[1]], .025, na.rm=TRUE))
upper <- as.numeric(quantile(samples3[[1]], .975, na.rm=TRUE))

# Save fitted model
dir.create(paste0(save_dir, "/FILEPATH/"), recursive = TRUE)
py_save_object(
  object=mod3,
  filename=file.path(paste0(save_dir, "/FILEPATH.pkl")),
  pickle="dill"
)

# Create beta summary
beta_samples_log <- as.data.table(samples3[[1]])
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


###########################
# survey_diff MR-BRT model
###########################
dat4 <- mr$MRData()
dat4$load_df(data = cv_survey[cv_survey$cv_diff==1,],
             col_obs = "log_ratio",
             col_obs_se = "log_se",
             col_study_id = "nid")

mod4 <- mr$MRBRT(data = dat4,
                 inlier_pct = 0.9,
                 cov_models = list(
                   mr$LinearCovModel("intercept", use_re = TRUE)))

# Fit model
mod4$fit_model(outer_max_iter = 5000L, inner_max_iter = 1000L)

# Add summary to a save spot
mod_save <- mod4$summary()

# Check variance (Get draws from beta_soln, take standard deviation of draws, square result)
n_samples1 <- as.integer(1000)

samples4 <- mod4$sample_soln(
  sample_size = n_samples1
)

# Take standard deviation of draws
SD <- sd(samples4[[1]])

# square to get variance
SD2 <- SD^2

# Compute the UIs
lower <- as.numeric(quantile(samples4[[1]], .025, na.rm=TRUE))
upper <- as.numeric(quantile(samples4[[1]], .975, na.rm=TRUE))

# Save fitted model
py_save_object(
  object=mod4,
  filename=file.path(paste0(save_dir, "FILEPATH.pkl")),
  pickle="dill"
)

# Create beta summary
beta_samples_log <- as.data.table(samples4[[1]])
setnames(beta_samples_log, names(beta_samples_log), names(mod4$summary()[[1]]))
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

beta_summary <- beta_summary[scale == "log", gamma := mod4$gamma_soln]
beta_summary <- beta_summary[scale == "lin", gamma := exp(mod4$gamma_soln)]

dir.create(paste0(save_dir, "/FILEPATH/"), recursive = TRUE)
data.table::fwrite(
  x = beta_summary,
  file = paste0(save_dir, "/FILEPATH.csv"),
  row.names = F
)


####################################################
# Use all three mr-brt models to create adjustments
####################################################

# Create prediction dataframe
df_pred <- data.frame(intercept=1, age_year = c(0,1,2,3,4))
dat_pred <- mr$MRData()

dat_pred$load_df(
  data = df_pred, 
  col_covs = list("intercept")
)

# predict chest
df_pred$chest <- mod2$predict(data = dat_pred)
head(df_pred, n = 5)

# Get uncertainty using mod2 draws
draws2 <- mod2$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

df_pred$chest_lo <- apply(draws2, 1, function(x) quantile(x, 0.025))
df_pred$chest_up <- apply(draws2, 1, function(x) quantile(x, 0.975))

# predict diff + fever
df_pred$diff_fever <- mod3$predict(data = dat_pred)
head(df_pred, n = 5)

# Get uncertainty using mod2 draws
draws3 <- mod3$create_draws(
  data = dat_pred,
  beta_samples = samples3[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

df_pred$diff_fever_lo <- apply(draws3, 1, function(x) quantile(x, 0.025))
df_pred$diff_fever_up <- apply(draws3, 1, function(x) quantile(x, 0.975))

# Just diff
df_pred$diff <- mod4$predict(data = dat_pred)
head(df_pred, n = 5)

# Get uncertainty using mod2 draws
draws4 <- mod4$create_draws(
  data = dat_pred,
  beta_samples = samples4[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

df_pred$diff_lo <- apply(draws4, 1, function(x) quantile(x, 0.025))
df_pred$diff_up <- apply(draws4, 1, function(x) quantile(x, 0.975))


## Collect outputs
survey_plot_mrbrt <- data.frame(age_start = c(0,1,2,3,4),
                                log_chest_ratio = df_pred$chest,
                                log_diff_fever_ratio = df_pred$diff_fever,
                                log_diff_ratio = df_pred$diff,
                                log_chest_se = (df_pred$chest_up - df_pred$chest_lo)/2/qnorm(0.975), 
                                log_diff_fever_se = (df_pred$diff_fever_up - df_pred$diff_fever_lo)/2/qnorm(0.975),
                                log_diff_se = (df_pred$diff_up - df_pred$diff_lo)/2/qnorm(0.975))

## Write out for next step
write.csv(survey_plot_mrbrt, paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"), row.names=F)

# Move to step 2