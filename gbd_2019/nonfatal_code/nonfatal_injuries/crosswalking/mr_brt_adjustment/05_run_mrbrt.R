rm(list = ls())

bundle <- commandArgs()[6]
name <- commandArgs()[7]
print(bundle)

library(dplyr)
library(data.table)
library(metafor, lib.loc = 'FILEPATH')
library(msm, lib.loc = 'FILEPATH')
library(readxl)
library(ggplot2)

repo_dir <- 'FILEPATH'
source(paste0(repo_dir, 'run_mr_brt_function.R'))
source(paste0(repo_dir, 'cov_info_function.R'))
source(paste0(repo_dir, 'check_for_outputs_function.R'))
source(paste0(repo_dir, 'load_mr_brt_outputs_function.R'))
source(paste0(repo_dir, 'predict_mr_brt_function.R'))
source(paste0(repo_dir, 'check_for_preds_function.R'))
source(paste0(repo_dir, 'load_mr_brt_preds_function.R'))

# import data
df <-
  read.csv(
    paste0(
      'FILEPATH',
      as.character(bundle),
      '.csv'
    )
  )
ratio_var <- 'ratio'
ratio_se_var <- 'ratio_se'

tmp_metareg <- df

# transform - log space
tmp_metareg$ratio_log <- log(tmp_metareg$ratio)
tmp_metareg$ratio_se_log <-
  sapply(1:nrow(tmp_metareg), function(i) {
    ratio_i <- tmp_metareg[i, 'ratio']
    ratio_se_i <- tmp_metareg[i, 'ratio_se']
    deltamethod(~ log(x1), ratio_i, ratio_se_i ^ 2)
  })

tmp_metareg$age_midpoint <-
  (tmp_metareg$age_start + tmp_metareg$age_end) / 2

# prep covariate
if (name %in% c(
  'inj_drowning',
  'inj_foreign_aspiration',
  'inj_trans_road_2wheel',
  'inj_mech_gun',
  'inj_foreign_eye',
  'inj_homicide_gun',
  'inj_homicide_knife',
  'inj_suicide_firearm',
  'inj_trans_road_pedal',
  'inj_trans_road_other'
)) {
  covs1 <- list(
    cov_info(
      covariate = 'age_midpoint',
      design_matrix = 'X',
      degree = 2,
      bspline_gprior_mean = paste(rep(0, 3), collapse = ','),
      bspline_gprior_var = paste(rep('inf', 3), collapse = ','),
      n_i_knots = 2
    )
  )
} else {
  covs1 <- list(
    cov_info(
      covariate = 'age_midpoint',
      design_matrix = 'X',
      degree = 3,
      bspline_gprior_mean = paste(rep(0, 4), collapse = ','),
      bspline_gprior_var = paste(rep('inf', 4), collapse = ','),
      n_i_knots = 3
    )
  )
}

tmp_metareg <- subset(tmp_metareg, !is.na(tmp_metareg$ratio_se))
tmp_metareg <-
  subset(tmp_metareg, !is.nan(tmp_metareg$ratio_se_log))

# running MR-BRT model
fit1 <- run_mr_brt(
  output_dir = paste0(
    'FILEPATH'
  ),
  model_label = paste0('outpatient_xwalk_', as.character(bundle), '_', name),
  data = tmp_metareg,
  mean_var = 'ratio_log',
  se_var = 'ratio_se_log',
  covs = covs1,
  method = "trim_maxL",
  trim_pct = 0.10,
  overwrite_previous = TRUE
)

saveRDS(
  fit1,
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '.rds'
  )
)

check_for_outputs(fit1)

results <- fit1$model_coefs[, c('x_cov', 'beta_soln')]
results$exponentiated <- exp(results$beta_soln)
results

g2 <- ggplot(data = results, aes(x = x_cov, y = exponentiated)) +
  geom_point() +
  labs(y = 'Outpatient coefficient', title = name) +
  ylim(0, max(results$exponentiated) + 1)

ggsave(
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '.pdf'
  ),
  g2
)

# this creates a ratio prediction for each observation in the original data
df_pred <- tmp_metareg
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)

saveRDS(
  pred_object,
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '.rds'
  )
)

data_dt <- as.data.table(fit1$train_data)
data_dt$Trimmed <- ifelse(data_dt$w == 1, 'No', 'Yes')
model_dt <- as.data.table(pred_object$model_summaries)

g3 <-
  ggplot() + geom_point(
    data = data_dt,
    aes(
      x = age_midpoint,
      y = ratio_log,
      size = 1 / ratio_se_log,
      color = Trimmed
    ),
    alpha = 0.25
  ) +
  geom_line(data = model_dt,
            aes(x = X_age_midpoint, y = Y_mean),
            color = "#117777") +
  geom_ribbon(
    data = model_dt,
    aes(x = X_age_midpoint, ymin = Y_mean_lo, ymax = Y_mean_hi),
    alpha = 0.05
  ) +
  labs(x = "Age", y = "Log Ratio", title = name) + scale_colour_manual(values = c('cadetblue4', 'coral2'))


ggsave(
  paste0(
    'FILEPATH',
    as.character(bundle),
    '_',
    name,
    '.pdf'
  ),
  g3
)
