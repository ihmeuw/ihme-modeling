####################################################
## Author: Phoebe Rhinehart
## Date: 3/25/2019
## Description: BMD Evidence Score Prep
####################################################

rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j_root <- "/snfs1/"
  h_root <- "~/"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "J:/"
  h_root <- "H:/"
}

library(dplyr)
library(data.table)
library(metafor, lib.loc = "/home/j/temp/reed/prog/R_libraries/")
library(msm, lib.loc = paste0(h_root, "package_lib/"))
library(readxl)
library(ggplot2)
library(readr)

bmd <- as.data.table(read_xlsx("/home/j/temp/prhine22/MrBeRT_risks/CC_template_blank_YOUR_prhine22_2019_04_22_bmd.xlsm", sheet = "extraction_orig")) #as.data.table(read_xlsx("/home/j/temp/prhine22/MrBeRT_risks/bmd_cc_withcovs.xlsx"))
bmd <-bmd[!is.na(nid)]
bmd <- bmd[-1,]
bmd <-bmd[!grepl("quartile", note_modeler)]
bmd$effect_size <- as.numeric(bmd$effect_size)
bmd$lower <- as.numeric(bmd$lower)
bmd$upper <- as.numeric(bmd$upper)
bmd$age_mean <- as.numeric(bmd$age_mean)
bmd$age_mean <- round(bmd$age_mean)

# CONVERT RR TO PER 1 SD DECREASE
bmd[, per_sd_num := parse_number(note_modeler)]
bmd$per_sd_num <- as.numeric(bmd$per_sd_num)
bmd[, effect_size := ifelse(per_sd_num != 1, effect_size^(1/per_sd_num), effect_size)]
bmd[, lower := ifelse(per_sd_num != 1, lower^(1/per_sd_num), lower)]
bmd[, upper := ifelse(per_sd_num != 1, upper^(1/per_sd_num), upper)]

# CONVERT RR TO LOG SPACE
bmd[, `:=` (effect_log = log(as.numeric(effect_size)), upper_log = log(as.numeric(upper)), lower_log = log(as.numeric(lower)))]
bmd[, se_log := (upper_log - lower_log)/3.92]

bmd_test <- copy(bmd)
test <- ggplot(bmd_test, aes(x = age_mean, y = effect_log, color = percent_male)) +
  geom_point() +
  theme_bw() +
  labs(x = "Mean age", title = "Mean Age v Log Effect Size ")

print(test)
ggsave("/snfs1/temp/prhine22/MrBeRT_risks/age_v_logeffect.pdf")

# MR-BERT TIME
repo_dir <- "/home/j/temp/reed/prog/projects/run_mr_brt/"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

keep_vars <- c("nid", "effect_log", "se_log", "cv_hip_fracture", "cv_nonhip")
df <- as.data.frame(bmd) %>%
  select_(.dots = keep_vars) %>%
  mutate(intercept = 1) # %>%
  #filter(cv_exposure_study != 1)

is_singular <- sapply(df, function(x) length(unique(x)) == 1)
df2 <- df[, !is_singular]
cvs <- names(df2)[!grepl("log", names(df2))]
cvs <- cvs[cvs != "nid"]

cov_list <- lapply(cvs, function(x) cov_info(x, "X"))
# # ADD SPLINE ON AGE
# cov_list[[1]]$degree <- 3 #cubic spline
# cov_list[[1]]$n_i_knots <- 2
# cov_list[[1]]$r_linear <- T #linear tails
# cov_list[[1]]$l_linear <- T #linear tails
# cov_list[[1]]$knot_placement_procedure <- "frequency"
# cov_list[[1]]$bspline_mono <- c("increasing", "increasing")

model_name <- "both_sex_ignore_age"

fit1 <- run_mr_brt(
  output_dir = "/home/j/temp/prhine22/MrBeRT_risks/",
  model_label = model_name,
  data = df2,
  mean_var = "effect_log",
  se_var = "se_log",
  covs = cov_list,
  study_id = "nid",
  overwrite_previous = TRUE
)

#####
# load_mr_brt_outputs() function
# Read the raw outputs from the model
results1 <- load_mr_brt_outputs(fit1)

names(results1)
coefs <- results1$model_coefs
metadata <- results1$input_metadata
orig <- results1$train_data
orig <- as.data.table(orig)
orig[, fracture_type := ifelse(cv_nonhip == 0 & cv_hip_fracture == 0, "any fracture", ifelse(cv_nonhip == 1 & cv_hip_fracture == 0, "non-hip fracture", "hip fracture"))]
# orig$percent_male <- as.factor(orig$percent_male)

#####
# predict_mr_brt() function
# Make predictions
# Provide a data frame to 'newdata' that includes all X covariates,
#   with values for which you want predictions
# -- column names in 'newdata' must match the X covariates provided to 'covs' in run_mr_brt()
# The model currently assumes you want predictions with all Z covariates set to 0
df_pred <- as.data.table(expand.grid(cv_hip_fracture = c(0, 1), cv_nonhip = c(0, 1))) # must include values for each X covariate
df_pred <- df_pred[!(cv_hip_fracture == 1 & cv_nonhip == 1)]
df_pred <- df_pred[!(cv_hip_fracture == 0 & cv_nonhip == 0)]
pred1 <- predict_mr_brt(fit1, newdata = df_pred)

#####
# load_mr_brt_preds() function
# Create an R object containing model predictions and draws
pred_object <- load_mr_brt_preds(pred1)
names(pred_object)
preds <- pred_object$model_summaries
preds <- as.data.table(preds)

predss <- copy(preds)
predss[, fracture_type := ifelse(X_cv_nonhip == 0 & X_cv_hip_fracture == 0, "any fracture", ifelse(X_cv_nonhip == 1 & X_cv_hip_fracture == 0, "non-hip fracture", "hip fracture"))]
# predss$X_percent_male <- as.factor(predss$X_percent_male)

test <- ggplot() +
  geom_point(data = predss, aes(x = fracture_type, y = Y_mean), color = "blue") +
  geom_errorbar(data = predss, aes(x = fracture_type, ymin = Y_mean_lo, ymax = Y_mean_hi), color = "blue") +
  geom_point(data = orig, aes(x = fracture_type, y = effect_log), color = "springgreen4", size = 2) +
  theme_bw() +
  labs(x = "Fracture type", y = "Ln(Effect size)", title = "Predicted Ln(Effect size)") +
  theme(axis.text = element_text(size=12), axis.title = element_text(size = 14))
print(test)
ggsave(paste0("/snfs1/temp/prhine22/MrBeRT_risks/", model_name, "/predicted_effectsize.pdf"))

test2 <- ggplot() +
  geom_point(data = predss, aes(x = fracture_type, y = exp(Y_mean)), color = "blue") +
  geom_errorbar(data = predss, aes(x = fracture_type, ymin = exp(Y_mean_lo), ymax = exp(Y_mean_hi)), color = "blue") +
  geom_point(data = orig, aes(x = fracture_type, y = exp(effect_log)), color = "springgreen4", size = 2) +
  theme_bw() +
  labs(x = "Fracture type", y = "Effect size", title = "Predicted effect size") +
  theme(axis.text = element_text(size=12), axis.title = element_text(size = 14))
print(test2)
ggsave(paste0("/snfs1/temp/prhine22/MrBeRT_risks/", model_name, "/predicted_effectsize_exp.pdf"))










# WITH AGE
test <- ggplot(predss, aes(x = X_age_mean, y = Y_mean, linetype = X_percent_male, color = fracture_type)) +
  geom_line() +
  theme_bw() +
  labs(x = "Age", y = "Ln(Effect Size)", title = "Predicted Ln(Effect Size)", linetype = "Percent Male", color = "Fracture Type")
print(test)
ggsave(paste0("/snfs1/temp/prhine22/MrBeRT_risks/", model_name, "/predicted_effectsize.pdf"))

test_4 <- ggplot(predss) +
  geom_line(aes(x = X_age_mean, y = Y_mean, color = fracture_type)) + #linetype = X_percent_male
  geom_errorbar(data = predss, aes(x = X_age_mean, ymin = Y_mean_lo, ymax = Y_mean_hi, color = fracture_type)) +
  theme_bw() +
  theme(text = element_text(size = 14)) +
  labs(x = "Age", y = "Ln(Effect Size)", title = "Predicted Ln(Effect Size)", color = "Fracture Type") + #linetype = "Percent Male", shape = "Percent Male"
  geom_point(data = orig, aes(x = age_mean, y = effect_log, color = fracture_type), size = 3) #shape = percent_male
print(test_4)
ggsave(paste0("/snfs1/temp/prhine22/MrBeRT_risks/", model_name, "predicted_effect_origdata.pdf"))

predss[, `:=` (effect_size = exp(Y_mean), effect_lower = exp(Y_mean_lo), effect_upper = exp(Y_mean_hi))]
orig[, effect_size := exp(effect_log)]

test_5 <- ggplot(predss) +
  geom_line(aes(x = X_age_mean, y = effect_size, color = fracture_type)) + #linetype = X_percent_male
  geom_errorbar(data = predss, aes(x = X_age_mean, ymin = effect_lower, ymax = effect_upper, color = fracture_type)) +
  theme_bw() +
  theme(text = element_text(size = 14)) +
  labs(x = "Age", y = "Effect Size", title = "Predicted Effect Size", color = "Fracture Type") + #linetype = "Percent Male", shape = "Percent Male"
  geom_point(data = orig, aes(x = age_mean, y = effect_size, color = fracture_type), size = 3) #shape = percent_male
print(test_5)
ggsave(paste0("/snfs1/temp/prhine22/MrBeRT_risks/", model_name, "predicted_effect_origdata_exp.pdf"))















