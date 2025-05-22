#----------------------------------------------------------------------
# Purpose: modeling age trends of smoking-CVDs RRs and create AF draws
# Data: 06/29/2021
#----------------------------------------------------------------------

rm(list = ls())
options(max.print=1000)
library(dplyr)
library(mrbrt001, lib.loc = FILEPATH)
library(stringr)
library(readr)
library(lme4)
library(car)
library(msm)
library(ggplot2)

source(FILEPATH)
np <- import("numpy")
np$random$seed(as.integer(123))

args <- commandArgs(trailingOnly = TRUE)

if(interactive()){
  # the ro_pair of this script can be age-specific for CVD outcomes
  ro_pair <- "aortic_aneurism"
  out_dir <- FILEPATH
  WORK_DIR <- FILEPATH
  inlier_pct <- 0.9
  setwd(WORK_DIR)
  source(FILEPATH)
} else {
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  inlier_pct <- as.numeric(args[4])
  setwd(WORK_DIR)
  source("./config.R")
}

##################################################
## Estimate the relationship between age and RR ## 
##################################################
# whether to use age stratified data only
age_stratified <- F

# load the fully cleaned data 
age_rr_dt_dir <- FILEPATH
clean_data <- fread(file.path(FILEPATH, 'all_extractions_cleaned.csv'))
clean_data[is.na(log_se)]

# Load ages for adding on gbd age group ids
ages <- get_age_metadata(19)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages[,.(age_start, age_end, age_group_id)]

#----------------------------------------
# 01 fit linear model 

# NOTE: remove rows without age_start or age_end for now. Model does not fit using the predicted age_start and age_end
clean_data <- clean_data[!is.na(age_start_orig) & !is.na(age_end_orig) & !is.na(log_effect_size) & !is.na(log_se)] 

# keep fully adjusted RR only
clean_data <- clean_data[fully_adj_ind==1]

# keep the age-stratified data only (in case we want to use these data)
if(age_stratified==T){
  clean_data <- clean_data[custom_age_stratified==1]
}


mrdata <- MRData()

mrdata$load_df(
  data = clean_data, 
  col_obs = c('effect_size_1'),
  col_obs_se = c('se_rr_1'), 
  col_study_id = c('nid'), 
  col_covs = as.list(c("age_start", "age_end", "age_midpoint"))
)

# Fit Linear Cov model
cov_models <- list(LinearCovModel(
  alt_cov = "age_midpoint",
  use_re = TRUE
))

# No trimming
model <- MRBRT(
  data = mrdata,
  cov_models = cov_models,
  inlier_pct = 0.9
)

model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
                outer_step_size=200L, outer_max_iter=100L) 

# Sample betas to use as priors for covariate selection.
sampling <- import("mrtool.core.other_sampling")
beta_samples <- sampling$sample_simple_lme_beta(1000L, model)
beta_std <- sd(beta_samples)

#----------------------------------------
# 02 select covariates
cv_vars <- names(clean_data)[grepl("cv_", names(clean_data)) & !grepl('cv_exp_nonsmoker', names(clean_data))]
cov_names <- c("age_midpoint", "percent_male", cv_vars)

candidate_covs <- c("percent_male", cv_vars)


# make copy of clean_data
clean_data_covselect <- copy(clean_data)

# populate percent_male (no missingness allowed for the model)
clean_data_covselect[is.na(percent_male), percent_male := 0.5]

# Interaction with age_midpoint
if (T){
  for (cov in candidate_covs) clean_data_covselect[, (cov) := age_midpoint * get(cov)]
}

mrdata_covslect <- MRData()

mrdata_covslect$load_df(
  data = clean_data_covselect, 
  col_obs = c('effect_size_1'),
  col_obs_se = c('se_rr_1'), 
  col_study_id = c('nid'), 
  col_covs = as.list(cov_names)
)

# Beta prior from first loglinear model results.
beta_gprior_std <- beta_std
COV_FINDER_CONFIG$pre_selected_covs[[1]] <- "age_midpoint"

covfinder <- do.call(
  CovFinder,
  c(COV_FINDER_CONFIG, 
    list(
      data = mrdata_covslect, 
      covs = as.list(candidate_covs)),
    beta_gprior_std = 0.1 * beta_gprior_std
  )
)

covfinder$select_covs(verbose = TRUE)

selected_covs <- covfinder$selected_covs
print(selected_covs)

#----------------------------------------
# 03 fit the final mixed effect model

dat1 <- MRData()
dat1$load_df(
  data = clean_data_covselect, # use this dataset with interactions
  col_obs = "effect_size_1",
  col_obs_se = "se_rr_1",
  col_covs = as.list(c("age_start", "age_end", selected_covs)),
  col_study_id = "nid")

bias_covs <- selected_covs[!selected_covs == "age_midpoint"]


# -----------------------
# 03 fit the final mixed effect model
cov_models_final <- list(LinearCovModel("intercept", use_re = T))

for (cov in bias_covs) cov_models_final <- append(cov_models_final, 
                                                  list(
                                                    do.call(
                                                      LinearCovModel, 
                                                      list(
                                                        prior_beta_gaussian=array(c(0, 0.1 * beta_std)),
                                                        use_re = F,
                                                        alt_cov=cov
                                                        
                                                      )
                                                    )
                                                  )
)


dat1 <- MRData()
dat1$load_df(
  data = clean_data,
  col_obs = "effect_size_1",
  col_obs_se = "se_rr_1",
  col_covs = as.list(c("age_start", "age_end", selected_covs)),
  col_study_id = "nid")

N_I_KNOTS <- 2
cov_models_final <- append(
  cov_models_final,
  LinearCovModel(alt_cov = list("age_start", "age_end"), use_re = F,
                 use_spline = T,
                 spline_degree = 2L,
                 spline_knots_type = 'domain',
                 prior_spline_monotonicity = "decreasing",
                 spline_knots = array(seq(0, 1, length.out = N_I_KNOTS + 2)))
)

# Fit mr-brt model
mod1 <- MRBRT(
  data = dat1,
  cov_models = cov_models_final,
  inlier_pct = 0.9
)

mod1$fit_model(inner_print_level = 2L, inner_max_iter = 1000L)

# Extract weights of data point
mod1_data <- mod1$data$to_df() %>% data.table
mod1_data[, w := mod1$w_soln]

# Save model
py_save_object(object = mod1, 
               filename = file.path(out_dir, "age_trend", "smoking_cvds_age.pkl"), 
               pickle = "dill")

py_save_object(object = mod1, 
               filename = file.path(age_rr_dt_dir, "smoking_cvds_age.pkl"), 
               pickle = "dill")

# predict the curve
pred_data <- data.table("intercept"=c(1), "age_start" = seq(20, 99, by = 5), "age_end" = seq(25, 100, by = 5))
pred_data[, age_midpoint := (age_start + age_end)/2]

# add bias covariates
for (cov in bias_covs) {
  if(cov=="cv_exp_current"){
    pred_data[, (cov) := 1]
  } else {
    pred_data[, (cov) := 0]
  }
  
}

if(grepl('stroke', ro_pair)){
  pred_data[, cv_outcome_stroke := 1 * age_midpoint]
}

# add interaction
pred_data[, cv_exp_current := cv_exp_current * age_midpoint]

dat_pred1 <- MRData()

dat_pred1$load_df(
  data = pred_data,
  col_covs = as.list(c("age_start", "age_end", bias_covs))
)

pred_data$Y_mean <- mod1$predict(dat_pred1, sort_by_data_id = T)

# Resample uncertainity (using the mean-gamma approach)
sampling <- import("mrtool.core.other_sampling")
num_samples <- 1000L
beta_samples <- sampling$sample_simple_lme_beta(num_samples, mod1)
gamma_samples <- rep(mod1$gamma_soln, num_samples) * matrix(1, num_samples)


# Draws with gamma
draws2 <- mod1$create_draws(
  data = dat_pred1,
  beta_samples = beta_samples,
  gamma_samples = gamma_samples,
  random_study = F) # turn off the gamma to make the UI smaller

pred_data$Y_mean_re <- apply(draws2, 1, function(x) mean(x))
pred_data$Y_mean_lo_re <- apply(draws2, 1, function(x) quantile(x, 0.025))
pred_data$Y_mean_hi_re <- apply(draws2, 1, function(x) quantile(x, 0.975))

# Plot with uncertainity (including gamma in UI)
plot_age_rr <- ggplot(pred_data, aes(x = age_start, y = Y_mean_re))+geom_line()+
  geom_ribbon(aes(ymin = Y_mean_lo_re, ymax = Y_mean_hi_re), alpha = 0.3) + 
  theme_bw() + labs(x = "Age", y = "RR w/ gamma", title = paste0("smoking-cvds"), subtitle = paste0("Gamma = ", mod1$gamma_soln))
print(plot_age_rr)

# with data point
pdf(file.path(age_rr_dt_dir, paste0(ro_pair,"_rr_plots.pdf")))
p <- ggplot()+
  geom_ribbon(data = pred_data, aes(x = age_start, ymin = Y_mean_lo_re, ymax = Y_mean_hi_re, fill ="95% CI"), alpha = 1/5)+
  geom_line(data = pred_data, aes(x = age_start, y = Y_mean_re, color = "mean estimate")) +
  geom_point(data = mod1_data, aes(x=(age_start+age_end)/2, y=obs, alpha = 1/obs_se, color = as.factor(w)))+
  geom_errorbarh(data = mod1_data, aes(xmin = age_start, xmax = age_end, y = obs, alpha = 1/obs_se, color = as.factor(w)))+
  geom_abline(slope = 0, intercept = 0, color = "blue", alpha = 1/2)+
  theme_classic()+
  theme(legend.position = "bottom") +
  xlab("age")+
  ylab("RR-1") +
  scale_fill_manual(values=c("blue"), name = c("CI"))+
  scale_color_manual(values=c("red", "black", "blue"), name = "") + 
  ggtitle(paste0("Age pattern of RR of ", ro_pair, " outcoomes")) + theme(plot.title = element_text(hjust = 0.5))
print(p)

p2 <- ggplot()+
  geom_ribbon(data = pred_data, aes(x = age_start, ymin = Y_mean_lo_re, ymax = Y_mean_hi_re, fill ="95% CI"), alpha = 1/5)+
  geom_line(data = pred_data, aes(x = age_start, y = Y_mean_re, color = "mean estimate")) +
  geom_point(data = mod1_data, aes(x=(age_start+age_end)/2, y=obs, alpha = 1/obs_se, color = as.factor(w)))+
  geom_errorbarh(data = mod1_data, aes(xmin = age_start, xmax = age_end, y = obs, alpha = 1/obs_se, color = as.factor(w)))+
  geom_abline(slope = 0, intercept = 0, color = "blue", alpha = 1/2)+
  theme_classic()+
  theme(legend.position = "bottom") +
  xlab("age")+
  ylab("RR-1") +
  scale_fill_manual(values=c("blue"), name = c("CI"))+
  scale_color_manual(values=c("red", "black", "blue"), name = "") + 
  coord_cartesian(ylim = c(-1, 6)) +
  ggtitle(paste0("Age pattern of RR of ", ro_pair, " outcoomes")) + theme(plot.title = element_text(hjust = 0.5))
print(p2)

dev.off()

# Add on gbd age groups
pred_data[age_end==100, age_end:=125]
pred_data <- merge(pred_data, ages, by = c("age_start", "age_end"))

save_data <- pred_data[, .(age_start, age_end, age_group_id, Y_mean_re, Y_mean_lo_re, Y_mean_hi_re)]
save_data[, `:=` (Y_mean_re = Y_mean_re,
                  Y_mean_lo_re = Y_mean_lo_re,
                  Y_mean_hi_re = Y_mean_hi_re)]


# save the age-group specific RR.
write.csv(save_data, file.path(FILEPATH, "smoking_cvd_age_rr_summary.csv"), row.names = F)

# Format draws with gamma
draws2 <- as.data.table(draws2)
setnames(draws2, colnames(draws2), paste0("draw_",0:999))

# Resample if NA draws AND drop draws that are greater than 6 MADs
y_draws <- cbind(pred_data[,.(age_start, age_end, age_group_id)], as.data.table(draws2))
y_draws <- outlier_draws(y_draws, exposure_col = c("age_start", "age_end" , "age_group_id"))

# get the reference age group
data <- readRDS(paste0(FILEPATH, "01_template_models/", ro_pair, ".RDS"))
df_data <- data$df_data
age_ref <- df_data$age_ref %>% mean

age_ref_group <- save_data[age_start <= age_ref & age_end >= age_ref, age_group_id]

# Get attenuation factor across age groups
draws_long <- melt(y_draws, id.vars = c("age_start", "age_end", "age_group_id"))

# this would depend on the reference group. 
reference_age_group <- draws_long[age_group_id==age_ref_group, .(variable, value)]
setnames(reference_age_group, "value", "ref_age_group_val")
draws_long <- merge(draws_long, reference_age_group, by = "variable")

# Attenuation factor = value/ref_age_group_val (AF is based on RR)
draws_long[, atten_factor := value/ref_age_group_val]
pct_draws <- as.data.table(dcast(draws_long, formula = age_start +age_end + age_group_id ~ variable, value.var = "atten_factor", drop = T))

pct_draws[, pct_mean := apply(.SD, 1, median), .SDcols = paste0("draw_", 0:999)]
pct_draws[, pct_upper := apply(.SD, 1, quantile, 0.975), .SDcols = paste0("draw_", 0:999)]
pct_draws[, pct_lower := apply(.SD, 1, quantile, 0.025), .SDcols = paste0("draw_", 0:999)]

summary <- pct_draws[, .(age_start, age_end, age_group_id, pct_mean, pct_upper, pct_lower)]
pct_draws[, `:=` (pct_mean = NULL, pct_upper = NULL, pct_lower = NULL)]

plot_summary <- copy(summary)

plot_attenuation <- ggplot(plot_summary, aes(x = age_start, y = pct_mean))+geom_line()+
  geom_ribbon(aes(ymin = pct_lower, ymax = pct_upper), colour = NA, alpha=0.3) + 
  theme_bw() + labs(x = "Age", y = "Attenuation factor", title = paste0("smoking-", ro_pair, " reference age group:", age_ref_group))
print(plot_attenuation)

# save the summary of attenuation factor
write.csv(summary, file.path(FILEPATH, paste0("attenuation_pct_summary_", ro_pair, "_",age_ref_group,".csv")), row.names = F)

# Save draws of the attenuation factor
write.csv(pct_draws, file.path(FILEPATH, paste0("attenuation_pct_draws_", ro_pair, "_",age_ref_group,".csv")), row.names = F)



