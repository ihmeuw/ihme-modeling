rm(list = ls())

library(openxlsx)
library(dplyr)
library(msm)
library(ggplot2)

source("FILEPATH/utility.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATHsave_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

bundle <- commandArgs()[6]
name <- commandArgs()[7]
version <- commandArgs()[8]

#########################################
#########################################
############## IMPORT DATA ##############
#########################################
#########################################

# pull bundle data plus clinical data
data <-
  get_bundle_version(bundle_version_id = version)

data[clinical_data_type == 'inpatient', c('cv_recv_care')] <- 1
data[clinical_data_type == 'inpatient', c('cv_no_care')] <- 0
data[clinical_data_type == 'inpatient', c('cv_inpatient')] <- 1
data[clinical_data_type == 'inpatient', c('cv_outpatient')] <- 0

data[clinical_data_type == 'outpatient', c('cv_recv_care')] <- 1
data[clinical_data_type == 'outpatient', c('cv_no_care')] <- 0
data[clinical_data_type == 'outpatient', c('cv_inpatient')] <- 0
data[clinical_data_type == 'outpatient', c('cv_outpatient')] <- 1

# adjust oldest age group
data$age_end <- ifelse(data$age_end > 99, 99, data$age_end)

#########################################
#########################################
########### ST-GPR ADJUSTMENT ###########
#########################################
#########################################

# load output from inj_recv_care_prop ST-GPR model
stgpr_id <- 57827
stgpr <- model_load(stgpr_id, 'raked')

ages <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6)
stgpr <- merge(ages, stgpr, by = c('age_group_id'))
setnames(
  stgpr,
  c('age_group_years_start', 'age_group_years_end'),
  c('age_start', 'age_end')
)

stgpr$gpr_se <- (stgpr$gpr_mean - stgpr$gpr_lower) / 1.96
stgpr$midpoint <- (stgpr$age_start + stgpr$age_end) / 2

df <- data
df$midpoint <- (df$age_start + df$age_end) / 2

df$gpr_mean <- NA
df$gpr_se <- NA

for (i in 1:nrow(df)) {
  mid <- df[i, 'midpoint']
  
  stgpr_sub <-
    subset(stgpr,
           year_id == df$year_start[i] &
             location_id == df$location_id[i])
  
  min_dif <- which.min(abs(stgpr_sub$midpoint - as.numeric(mid)))
  
  df$gpr_mean[i] <- stgpr_sub[min_dif, gpr_mean]
  df$gpr_se[i] <- stgpr[min_dif, gpr_se]
}

# save old mean
df$mean_unadjusted <- df$mean

# adjust data tagged with cv_no_care
df$mean <-
  ifelse(df$cv_no_care == 1,
         (df$mean / (1 - df$gpr_mean)) * (df$gpr_mean),
         df$mean)

# adjust data tagged with no covariates (injuries warranting care)
df$mean <-
  ifelse((df$cv_recv_care == 0) &
           (df$cv_no_care == 0) &
           (df$cv_inpatient == 0) &
           (df$cv_outpatient == 0),
         df$mean * df$gpr_mean,
         df$mean
  )

# recalculate standard error using closed form solution
df$standard_error <-
  ifelse((df$cv_no_care == 1) |
           ((df$cv_recv_care == 0) &
              (df$cv_no_care == 0) &
              (df$cv_inpatient == 0) &
              (df$cv_outpatient == 0)
           ),
         sqrt((df$standard_error * df$gpr_se) + (df$standard_error * df$gpr_mean) + (df$mean_unadjusted *
                                                                                       df$gpr_se)
         ),
         df$standard_error)

# clear out lower value that is no longer correct
df$lower <-
  ifelse((df$cv_no_care == 1) |
           ((df$cv_recv_care == 0) &
              (df$cv_no_care == 0) &
              (df$cv_inpatient == 0) &
              (df$cv_outpatient == 0)
           ),
         NA,
         df$lower)

# clear out upper value that is no longer correct
df$upper <-
  ifelse((df$cv_no_care == 1) |
           ((df$cv_recv_care == 0) &
              (df$cv_no_care == 0) &
              (df$cv_inpatient == 0) &
              (df$cv_outpatient == 0)
           ),
         NA,
         df$upper)

# at this point all data will all be at least cv_recv_care

#########################################
#########################################
########### MR-BRT ADJUSTMENT ###########
#########################################
#########################################

df_orig <- df

df_orig$age_midpoint <- (df_orig$age_start + df_orig$age_end) / 2

df_orig$mean_log <- log(df_orig$mean)

# delta transform standard errors
df_orig$se_log <- sapply(1:nrow(df_orig), function(i) {
  mean_i <- df_orig[i, 'mean']
  se_i <- df_orig[i, 'standard_error']
  deltamethod( ~ log(x1), mean_i, se_i ^ 2)
})

# load in MR-BRT model
fit1 <-
  readRDS(
    paste0(
      'FILEPATH',
      as.character(bundle),
      '_',
      name,
      '.rds'
    )
  )

# predict based on age midpoint
pred1 <- predict_mr_brt(fit1, newdata = df_orig)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

df_preds <- preds %>%
  mutate(pred = Y_mean,
         pred_se = (Y_mean_hi - Y_mean_lo) / 3.92) %>%
  select(pred, pred_se)

# apply crosswalk to mean and standard error (for outpatient data points)
df_adj <- cbind(df_orig, df_preds) %>%
  mutate(
    mean_log_tmp = mean_log - pred,
    var_log_tmp = se_log ^ 2 + pred_se ^ 2,
    se_log_tmp = sqrt(var_log_tmp)
  )

df_adj$pred_exp <- exp(df_adj$pred)

# apply crosswalks to mean of non-outpatient data (people who received care but we don't know what type)
df_adj$mean_log_adjusted <-
  ifelse(
    df_adj$cv_recv_care == 1 &
      df_adj$cv_no_care == 0 &
      df_adj$cv_inpatient == 0 &
      df_adj$cv_outpatient == 1,
    df_adj$mean_log_tmp,
    ifelse(
      df_adj$cv_recv_care == 1 &
        df_adj$cv_no_care == 0 &
        df_adj$cv_inpatient == 0 &
        df_adj$cv_outpatient == 0,
      log(df_adj$mean / (df_adj$pred_exp + 1)),
      ifelse(
        df_adj$cv_recv_care == 0 &
          df_adj$cv_no_care == 0 &
          df_adj$cv_inpatient == 0 &
          df_adj$cv_outpatient == 0,
        log(df_adj$mean / (df_adj$pred_exp + 1)),
        ifelse(
          df_adj$cv_recv_care == 0 &
            df_adj$cv_no_care == 1 &
            df_adj$cv_inpatient == 0 &
            df_adj$cv_outpatient == 0,
          log(df_adj$mean / (df_adj$pred_exp + 1)),
          df_adj$mean_log
        )
      )
    )
  )

df_adj$se_log_adjusted <- df_adj$se_log_tmp

# back-transform into normal space
df_adj <- df_adj %>%
  mutate(
    lo_log_adjusted = mean_log_adjusted - 1.96 * se_log_adjusted,
    hi_log_adjusted = mean_log_adjusted + 1.96 * se_log_adjusted,
    mean_adjusted = exp(mean_log_adjusted),
    lo_adjusted = exp(lo_log_adjusted),
    hi_adjusted = exp(hi_log_adjusted)
  )

df_adj$se_adjusted <- sapply(1:nrow(df_adj), function(i) {
  ratio_i <- df_adj[i, "mean_log_adjusted"]
  ratio_se_i <- df_adj[i, "se_log_adjusted"]
  deltamethod( ~ exp(x1), ratio_i, ratio_se_i ^ 2)
})

# need to adjust standard error of mean 0 data points in normal space
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

# compile final crosswalked data set
final_data <- cbind(df,
                    df_adj[, c("mean_adjusted",
                               "se_adjusted",
                               "lo_adjusted",
                               "hi_adjusted")])

# prepare for uploader
final_data <- subset(final_data, sex != 'Both')
if ('group_review' %in% colnames(final_data)) {final_data <- subset(final_data, is.na(group_review) | group_review == 1)}
final_data$mean <- final_data$mean_adjusted
final_data$standard_error <- final_data$se_adjusted
final_data$lower <- final_data$lo_adjusted
final_data$lower <- ifelse(final_data$lower < 0.0000000000000000000000001, 0, final_data$lower)
final_data$lo_adjusted <- ifelse(final_data$lo_adjusted < 0.0000000000000000000000001, 0, final_data$lo_adjusted)
final_data$upper <- final_data$hi_adjusted
final_data$upper <- ifelse(final_data$upper > 1000, 1000, final_data$upper)
final_data$hi_adjusted <- ifelse(final_data$hi_adjusted > 1000, 1000, final_data$hi_adjusted)
final_data$crosswalk_parent_seq <- NA

write.xlsx(
  final_data,
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '_adjusted.xlsx'
  ),
  sheetName = 'extraction'
)

#########################################
#########################################
######### VISUALIZE ADJUSTMENT ##########
#########################################
#########################################

final_data$cov_id <-
  paste0(
    final_data$cv_recv_care,
    final_data$cv_no_care,
    final_data$cv_inpatient,
    final_data$cv_outpatient
  )

final_data$cov_name <-
  ifelse(
    final_data$cov_id == '0000',
    'Injuries warranting care',
    ifelse(
      final_data$cov_id == '0100',
      'No care received',
      ifelse(
        final_data$cov_id == '1000',
        'Care received',
        ifelse(
          final_data$cov_id == '1001',
          'Outpatient care received',
          ifelse(final_data$cov_id == '1010', 'Inpatient care received', 'NA')
        )
      )
    )
  )

lim <-
  max(max(final_data$mean_adjusted),
      max(final_data$mean_unadjusted))


ggplot() + geom_point(
  data = final_data,
  aes(x = mean_unadjusted, y = mean_adjusted, colour = cov_name),
  alpha = .6,
  size = 2
) + coord_equal() +
  labs(
    x = 'Unadjusted mean',
    y = 'Adjusted mean (xwalked to inpatient incidence)',
    colour = 'Original data definition',
    caption = as.character(range(final_data$mean_adjusted))
  ) +
  xlim(c(0, lim)) +
  ylim(c(0, lim)) +
  geom_abline(intercept = 0,
              slope = 1,
              alpha = 0.5)


ggsave(
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '.pdf'
  )
)

ggplot() + geom_point(
  data = final_data,
  aes(x = mean_unadjusted, y = mean_adjusted),
  alpha = .5,
  size = 2
) + coord_equal() +
  labs(x = 'Unadjusted mean', y = 'Adjusted mean (xwalked to inpatient incidence)', colour = 'Original data definition') +
  theme(aspect.ratio = 1) +
  geom_abline(intercept = 0,
              slope = 1,
              alpha = 0.5) +
  facet_wrap(~ cov_name)

ggsave(
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '_facet.pdf'
  )
)
