# team: GBD Injuries
# project: Crosswalking for GBD 2020
# script: Run and plot MR-BRT models

library(crosswalk, lib.loc = "FILEPATH")
library(ggplot2)

bundle_df <- read.csv('FILEPATH/bundle_versions.csv')
bundle_df <- bundle_df[!is.na(bundle_df$save),]
bundle_df <- bundle_df[bundle_df$bundle_id != 360,]

bundles <- bundle_df$bundle_id
names <- bundle_df$acause_name

for (i in 1:length(bundles)){
  bundle <- bundles[i]
  name <- names[i]

  print(bundle)
  
  # import data
  df_matched <-
    read.csv(
      paste0(
        'FILEPATH/bundle_',
        as.character(bundle),
        '.csv'))
  
  # calculate age midpoint
  df_matched$age_midpoint <- (df_matched$age_start + df_matched$age_end) / 2
  df_matched <- df_matched[order(df_matched$age_midpoint),]

  # use age midpoint as cv with exceptions
  if (name %in% c('inj_homicide_gun', 'inj_homicide_knife', 'inj_electrocution')) {
    covs_list <- list()
  } else {
    covs_list <- list("age_midpoint")
  }
  
  # format data for meta-regression; pass in data.frame and variable names
  dat1 <- CWData(
    df = df_matched,
    obs = "log_diff",       # matched differences in logit space
    obs_se = "log_diff_se", # SE of matched differences in logit space
    alt_dorms = "dorm_alt", # var for the alternative def/method
    ref_dorms = "dorm_ref", # var for the reference def/method
    covs = covs_list        # list of (potential) covariate columns
  )
  
  # these models have no age covariate
  if (name %in% c('inj_homicide_gun', 'inj_homicide_knife', 'inj_electrocution')) {
    cov_models <- list(CovModel("intercept"))
    
  # these models have less data so fewer knots
  } else if (name %in% c('inj_drowning', 'inj_foreign_aspiration', 'inj_trans_road_2wheel',
                         'inj_mech_gun', 'inj_foreign_eye', 'inj_suicide_firearm',
                         'inj_trans_road_pedal', 'inj_trans_road_other')) {
    cov_models <- list(CovModel(
      cov_name = "age_midpoint", 
      spline = XSpline(
        knots = c(0, 40, 85), # outer knots must encompass the data
        degree = 2L, # polynomial order (1=linear, 2=quadratic, 3=cubic)
        ),
      spline_monotonicity = 'decreasing'
      ), CovModel("intercept"))
    
  } else {
    cov_models <- list(CovModel(
      cov_name = "age_midpoint", 
      spline = XSpline(
        knots = c(0, 30, 60, 85), # outer knots must encompass the data
        degree = 3L, # polynomial order (1=linear, 2=quadratic, 3=cubic)
        ),
      spline_monotonicity = 'decreasing'
      ), CovModel("intercept"))
  }
  
  # create a CWModel object
  fit1 <- CWModel(
    cwdata = dat1,           # result of CWData() function call
    obs_type = "diff_log",   # must be "diff_logit" or "diff_log"
    cov_models = cov_models,
    gold_dorm = "inpatient", # level of 'ref_dorms' that's the gold standard
    inlier_pct = 0.9         # trim 0.10
  )
  
  # save model coefficients
  fit1$save_result_df('FILEPATH/',
                      filename = paste0('result_', 
                                      as.character(bundle), 
                                      '_', 
                                      name, 
                                      '.csv'))
  
  # save model object
  py_save_object(object = fit1,
                 filename = paste0('FILEPATH/',
                                   'model_',
                                   as.character(bundle),
                                   '_',
                                   name,
                                   '.pkl'),
                 pickle = "dill")

  # prep coefficients
  coefs <- fit1$create_result_df()
  coefs <- coefs[coefs$dorms=='outpatient',]
  coefs$exponentiated <- exp(coefs$beta)
  
  # plot coefficients
  g2 <- ggplot(data = coefs, aes(x = cov_names, y = exponentiated)) +
    geom_point() +
    labs(x = 'Covariate', y = 'Outpatient coefficient', title = name) +
    ylim(0, max(coefs$exponentiated) + 1)
  
  ggsave(
    paste0(
      'FILEPATH/outpatient_coeffs_',
      as.character(bundle),
      '_',
      name,
      '.pdf'
    ),
    g2
  )
  
  # this creates a ratio prediction for each observation in the original data
  df_pred <- df_matched
  df_pred[, c("inc2", "inc2_se", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
    fit_object = fit1,       # result of CWModel()
    df = df_pred,            # original data with obs to be adjusted
    orig_dorms = "dorm_alt", # name of column with (all) def/method levels
    orig_vals_mean = "inc_alt",  # original mean
    orig_vals_se = "inc_se_alt"  # standard error of original mean
  )
  
  # use weights to determine what got trimmed
  df_pred$trim <- ifelse(fit1$w==1, 'no', 'yes')
  
  # prep predictions
  df_dedupe <- df_pred[, c('age_midpoint', 'diff')]
  df_dedupe <- unique(df_dedupe)
  
  # plot predictions
  g3 <-
    ggplot() + geom_point(
      data = df_pred,
      aes(
        x = age_midpoint,
        y = log_diff,
        color = trim,
        size = 1/log_diff_se),
      alpha = 0.25) +
    geom_line(data = df_dedupe,
              aes(x = age_midpoint, y = diff),
              color = "#117777") +
    labs(x = "Age", y = "Log Ratio", color = 'Trimmed', size = '1/Log SE', title = name) + 
    scale_colour_manual(values = c('cadetblue4', 'coral2'))

  ggsave(
    paste0(
      'FILEPATH/model_fit_',
      as.character(bundle),
      '_',
      name,
      '.pdf'
    ),
    g3
  )
}
