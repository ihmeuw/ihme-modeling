rm(list = ls())

library(openxlsx)
library(dplyr)
library(msm)
library(ggplot2)

source("FILEPATH/utility.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

#########################################
#########################################
############## IMPORT DATA ##############
#########################################
#########################################

version = 8195

# pull bundle data plus clinical data
data <-
  get_bundle_version(bundle_version_id = version)

data <- subset(data, cv_sv_attempt != 1) # Not using data with attempted sexual violence
data$cv_case_def <- ifelse(data$cv_elived == 0 & 
                             data$cv_epart == 0 & 
                             data$cv_emarried == 0 & 
                             data$cv_pene_only == 0 & 
                             data$cv_partner_abuse_only & 
                             data$cv_sv_physical == 0 &
                             data$cv_esex == 0, 
                           1,
                           0)

#########################################
#########################################
########### MR-BRT ADJUSTMENT ###########
#########################################
#########################################

df_orig <- data
df_orig$mean_unadjusted <- df_orig$mean

df_orig$mean_log <- log(df_orig$mean)

# delta transform standard errors
df_orig$se_log <- sapply(1:nrow(df_orig), function(i) {
  mean_i <- df_orig[i, 'mean']
  se_i <- df_orig[i, 'standard_error']
  deltamethod( ~ log(x1), mean_i, se_i ^ 2)
})

# load in MR-BRT model
fit1 <-
  readRDS('FILEPATH')

# this creates a ratio prediction for each observation in the original data
pred1 <- predict_mr_brt(fit1, newdata = df_orig)
preds <- pred1$model_summaries

df_preds <- preds %>%
  mutate(
    pred = Y_mean,
    pred_se = (Y_mean_hi - Y_mean_lo) / 3.92  ) %>%
  select(pred, pred_se)

df_orig2 <- cbind(df_orig, df_preds) %>%
  mutate(
    mean_log_tmp = mean_log - pred, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_log_tmp = se_log^2 + pred_se^2, # adjust the variance
    se_log_tmp = sqrt(var_log_tmp)
  )

# if original data point was a reference data point, leave as-is
df_orig3 <- df_orig2 %>%
  mutate(
    mean_log_adjusted = if_else(case_definition == 1, mean_log, mean_log_tmp),
    se_log_adjusted = if_else(case_definition == 1, se_log, se_log_tmp),
    lo_log_adjusted = mean_log_adjusted - 1.96 * se_log_adjusted,
    hi_log_adjusted = mean_log_adjusted + 1.96 * se_log_adjusted,
    mean_adjusted = exp(mean_log_adjusted),
    lo_adjusted = exp(lo_log_adjusted),
    hi_adjusted = exp(hi_log_adjusted) )

# Back-transform out of delta method
df_orig3$se_adjusted <- sapply(1:nrow(df_orig3), function(i) {
  ratio_i <- df_orig3[i, "mean_log_adjusted"]
  ratio_se_i <- df_orig3[i, "se_log_adjusted"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})

df_adj <- copy(df_orig3)
df_adj$se_normal <- sapply(1:nrow(df_adj), function(i) {
  ratio_i <- df_adj[i, "pred"]
  ratio_se_i <- df_adj[i, "pred_se"]
  deltamethod( ~ exp(x1), ratio_i, ratio_se_i ^ 2)
})

df_adj$se_adjusted <-
  ifelse(
    df_adj$mean_adjusted == 0,
    sqrt(df_adj$standard_error ^ 2 + df_adj$se_normal ^ 2),
    df_adj$se_adjusted
  )
df_adj$lo_adjusted <-
  ifelse(
    df_adj$mean_adjusted == 0,
    0,
    df_adj$lo_adjusted
  )
df_adj$hi_adjusted <-
  ifelse(
    df_adj$mean_adjusted == 0,
    df_adj$mean_adjusted + 1.96 * df_adj$se_adjusted,
    df_adj$hi_adjusted
  )
# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  df_orig, 
  df_adj[, c("case_definition", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

final_data$mean <- final_data$mean_adjusted
final_data$standard_error <- final_data$se_adjusted
final_data$lower <- final_data$lo_adjusted
final_data$upper <- final_data$hi_adjusted

final_data <- final_data[mean_adjusted < 1]
final_data[, crosswalk_parent_seq := NA]
final_data <- final_data[is.na(group_review)] # A weird world where you do want NAs and not zero's
final_data[, group := NA]
final_data[, specificity := NA]
final_data[, cv_case_def := NULL]

write.xlsx(
  final_data, 
  'FILEPATH'
  ,
  sheetName = 'extraction'
)

#########################################
#########################################
######### VISUALIZE ADJUSTMENT ##########
#########################################
#########################################

plotdf <- final_data[, c('mean_unadjusted', 'mean_adjusted')]
ggplot() + geom_point(
  data = plotdf,
  aes(x = mean_unadjusted, y = mean_adjusted),
  alpha = .6,
  size = 2
) + coord_equal() +
  labs(
    x = 'Unadjusted mean',
    y = 'Adjusted mean (xwalked to inpatient incidence)',
    colour = 'Original data definition') +
  xlim(c(0, 1)) +
  ylim(c(0, 1)) +
  geom_abline(intercept = 0,
              slope = 1,
              alpha = 0.5)
