# team: GBD Injuries
# script: apply adjustments to data (crosswalk!)

library(ggplot2)
library(openxlsx)
library(crosswalk, lib.loc = "FILEPATH")

source('FILEPATH/utility.r')
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

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
  get_bundle_version(bundle_version_id = version, fetch = 'all')

data <- data[data$measure != 'mtexcess',]

data[clinical_data_type == 'inpatient', c('cv_recv_care')] <- 1
data[clinical_data_type == 'inpatient', c('cv_no_care')] <- 0
data[clinical_data_type == 'inpatient', c('cv_inpatient')] <- 1
data[clinical_data_type == 'inpatient', c('cv_outpatient')] <- 0

data[clinical_data_type == 'outpatient', c('cv_recv_care')] <- 1
data[clinical_data_type == 'outpatient', c('cv_no_care')] <- 0
data[clinical_data_type == 'outpatient', c('cv_inpatient')] <- 0
data[clinical_data_type == 'outpatient', c('cv_outpatient')] <- 1

# adjust oldest age group so modeling midpoint isn't around 110 years old
data$age_end <- ifelse(data$age_end > 99, 99, data$age_end)

#########################################
#########################################
########### ST-GPR ADJUSTMENT ###########
#########################################
#########################################

# load output from inj_recv_care_prop ST-GPR model
stgpr_id <- 57827
stgpr <- model_load(stgpr_id, 'raked')

ages <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
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

# at this point all data will all be at least cv_recv_care not going to change
# the covariates tagged though to preserve information about the original data
# point

#########################################
#########################################
########### MR-BRT ADJUSTMENT ###########
#########################################
#########################################

df_orig <- df

df_orig$age_midpoint <- (df_orig$age_start + df_orig$age_end) / 2

# deal with mean of 0
df_orig[df_orig$mean == 0, 'mean'] <- 0.0000000000000000000000001

# load in MR-BRT model
fit1 <-
  py_load_object(
    filename = paste0('FILEPATH/model_',
                      as.character(bundle),
                      '_',
                      name,
                      '.pkl'), pickle = 'dill')

# make a dummy column to indicate rows to adjust (outpatient) / not adjust (inpatient)
df_orig$dorm <- ifelse(df$cv_inpatient==1, 'inpatient', 'outpatient')

# make a dummy column to indicate which rows are recv_care
df_orig$adj_rc <- ifelse(df$cv_outpatient==0 & df$cv_inpatient==0, 1, 0)

df_pred <- df_orig

# predict ratios based on age midpoint
df_pred[, c("mean_adjusted", "se_adjusted", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = fit1,       # result of CWModel()
  df = df_pred,            # original data with obs to be adjusted
  orig_dorms = "dorm", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

# apply crosswalks to mean of non-outpatient data (people who received care but we don't know what type)
df_pred$mean_adjusted <- ifelse(df_pred$adj_rc==1,
                                df_pred$mean / (exp(df_pred$diff) + 1),
                                df_pred$mean_adjusted)

# create confidence interval
df_pred$lo_adjusted <- df_pred$mean_adjusted - 1.96*df_pred$se_adjusted
df_pred$hi_adjusted <- df_pred$mean_adjusted + 1.96*df_pred$se_adjusted

df_pred$lo_adjusted <-
  ifelse(
    df_pred$mean_adjusted == 0,
    0,
    df_pred$lo_adjusted
  )
df_pred$hi_adjusted <-
  ifelse(
    df_pred$mean_adjusted == 0,
    df_pred$mean_adjusted + 1.96 * df_adj$se_adjusted,
    df_pred$hi_adjusted
  )

# compile final crosswalked data set
final_data <- cbind(df,
                    df_pred[, c("mean_adjusted",
                               "se_adjusted",
                               "lo_adjusted",
                               "hi_adjusted")])

# prepare for uploader
final_data <- subset(final_data, sex != 'Both')
if ('group_review' %in% colnames(final_data)) {final_data <- subset(final_data, is.na(group_review) | group_review == 1)}
final_data$mean <- final_data$mean_adjusted
final_data$mean <- ifelse(final_data$mean < 0.0000000000000001, 0, final_data$mean)
final_data$mean_adjusted <- ifelse(final_data$mean_adjusted < 0.0000000000000001, 0, final_data$mean_adjusted)
final_data$standard_error <- final_data$se_adjusted
final_data$lower <- final_data$lo_adjusted
final_data$lower <- ifelse(final_data$lower < 0.0000000000000001, 0, final_data$lower)
final_data$lo_adjusted <- ifelse(final_data$lo_adjusted < 0.0000000000000001, 0, final_data$lo_adjusted)
final_data$upper <- final_data$hi_adjusted
final_data$upper <- ifelse(final_data$upper > 1000, 1000, final_data$upper)
final_data$hi_adjusted <- ifelse(final_data$hi_adjusted > 1000, 1000, final_data$hi_adjusted)
final_data$crosswalk_parent_seq <- NA

write.xlsx(
  final_data,
  paste0(
    'FILEPATH/',
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


g1 <- ggplot() + geom_point(
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
    'FILEPATH/',
    as.character(bundle),
    '_',
    name,
    '.pdf'
  ),
  g1
)

g2 <- ggplot() + geom_point(
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
    'FILEPATH/',
    as.character(bundle),
    '_',
    name,
    '_facet.pdf'
  ),
  g2
)

