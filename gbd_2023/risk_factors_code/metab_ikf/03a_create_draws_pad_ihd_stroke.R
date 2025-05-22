#--------------------------------------------------------------------
# Project: KD RR Evidence Score + Pub Bias
# Purpose: Run MRBRT models for all ihd, pvd, stroke risk-outcome pairs, MIXED b/c of splines
#---------------------------------------------------------------------

# Set up ---------------------------------------------------------------------------------------------------
rm(list = ls())

# map drives
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

# libs
require(reticulate)
library(data.table)
library(mrbrt001, lib.loc = "FILEPATH")
library(ggplot2)
library(openxlsx)
library(dplyr)
source("FILEPATH/mixed_functions.R")
source("FILEPATH/egger_functions.R")
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Settings ---------------------------------------------------------------------------------------------------
folder <- "FILEPATH"
cvs <- list("age_mean")
ver <- "ikf_cvd_2024_07_10"
# run a test on one combo before running all ros
exp <- "stage3"
out <- "pad"
dataset <- get_crosswalk_version(44842)


# MR-BRT loop ---------------------------------------------------------------------------------------------------
for (exp in c("albuminuria", "stage3", "stage4", "stage5")) {
  for (out in c("ihd", "pad", "stroke")) {
    message(paste0("working on ", exp, " : ", out))
    df <- dataset[risk == exp & cvd_mrbrt_outcome == out]
    
    # set up folder
    model_dir_bp <- paste0("metab_ikf_", exp, "-cvd_", out, "/")
    dir.create(paste0(folder,ver,"/mrbrt_model_outputs/",model_dir_bp), recursive = TRUE)
    dir.create(paste0(folder,ver, "/plots/"), recursive = TRUE)
    
    # set up data
    mr_df_bp <- MRData()
    
    mr_df_bp$load_df(
      data = df, col_obs = "ln_rr", col_obs_se = "ln_rr_se",
      col_covs = cvs, col_study_id = "study_id"
    )
    
    # set up model
    model_bp <- MRBRT(
      data = mr_df_bp,
      cov_models = list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel(
          alt_cov = "age_mean",
          use_spline = TRUE,
          spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
          spline_degree = 3L,
          spline_knots_type = "frequency",
          spline_r_linear = TRUE,
          spline_l_linear = TRUE
        )
      ),
      inlier_pct = .9
    )
    
    # fit model
    model_bp$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    # save model object
    py_save_object(object = model_bp, filename = paste0(folder,ver,"/mrbrt_model_outputs/", model_dir_bp, "mod1.pkl"), pickle = "dill")
  }
}

# get draws with fisher mat ----------------------------------------------------------------------------------------------------------
repl_python()
quit

path <-  paste0(folder,ver,"/mrbrt_model_outputs/")

for (exp in c("albuminuria", "stage3", "stage4", "stage5")) {
  for (out in c("ihd", "pad", "stroke")) {
    model_path <- paste0(path, "metab_ikf_", exp, "-cvd_", out, "/mod1.pkl")
    model_path
    ro_pair <- paste0("metab_ikf_", exp, "-cvd_", out)
    ro_pair
    
    model <- py_load_object(filename = model_path, pickle = "dill")
    
    ### Extract data
    data_info <- extract_data_info(model, cont_cov = "age_mean")
    df <- data_info$df
    data_info$ro_pair <- ro_pair
    
    
    # Get evidence score with Pub Bias ------------------------------------------------------------------------------------
    ## Detect publication bias
    egger_model_all <- egger_regression(df$residual, df$residual_se)
    egger_model <- egger_regression(df[!df$outlier, ]$residual, df[!df$outlier, ]$residual_se)
    has_pub_bias <- egger_model$pval < 0.05
    
    
    ### Adjust for publication bias
    if (has_pub_bias) {
      df_fill <- get_df_fill(df)
      num_fill <- nrow(df_fill)
    } else {
      num_fill <- 0
    }
    
    # fill the data if needed and refit the model
    if (num_fill > 0) {
      df <- rbind(df, df_fill)
      data_info$df <- df
      
      # refit the model
      data <- MRData()
      data$load_df(
        data = df[!df$outlier, ],
        col_obs = "obs",
        col_obs_se = "obs_se",
        col_covs = as.list(model$cov_names),
        col_study_id = "study_id"
      )
      model_fill <- MRBRT(data, cov_models = model$cov_models)
      model_fill$fit_model()
    } else {
      model_fill <- NULL
    }
    
    
    ### Extract scores
    uncertainty_info <- get_uncertainty_info(data_info, model)
    if (is.null(model_fill)) {
      uncertainty_info_fill <- NULL
    } else {
      uncertainty_info_fill <- get_uncertainty_info(data_info, model_fill)
    }
    
    
    ### Output diagnostics
    dir.create(paste0(folder,ver,"/plots/"), recursive = TRUE)
    pdf(file = paste0(folder,ver,"/plots/", ro_pair, ".pdf"))
    title <- (paste(
      ro_pair, "\n", "sim", ": egger_mean=", round(egger_model$mean, 3),
      ", egger_sd=", round(egger_model$sd, 3), ", egger_pval=",
      round(egger_model$pval, 3)
    ))
    plot_residual(df, title)
    
    plot_model(
      data_info,
      uncertainty_info,
      model,
      uncertainty_info_fill,
      model_fill
    )
    dev.off()
    
    summary <- summarize_model(data_info,
                               uncertainty_info,
                               model,
                               egger_model,
                               egger_model_all,
                               uncertainty_info_fill = uncertainty_info_fill,
                               model_fill = model_fill
    )
    # summary
    dir.create(paste0(folder,ver,"/summaries/"), recursive = TRUE)
    write.csv(summary, file = paste0(folder,ver,"/summaries/", ro_pair, ".csv"), row.names = FALSE)
    
    data_info$pred_cov <- c(27.5,32.5,37.5,42.5,47.5,52.5,57.5,62.5,67.5, 72.5, 77.5, 82.5, 87.5, 92.5, 97.5)
    num_draws <- 1000L
    
    beta_samples <- mrbrt001::core$other_sampling$sample_simple_lme_beta(num_draws, model)
    gamma_sd <- get_gamma_sd(model)
    gamma_outer_samples <- matrix(rep(model$gamma_soln[1] + 2*gamma_sd, num_draws), nrow = num_draws)
    
    df_pred <- data.frame(data_info$pred_cov)
    names(df_pred) <- data_info$cont_cov
    for (name in model$cov_names) {
      if (name == "intercept") {
        df_pred[name] <- 1
      } else if (name != data_info$cont_cov) {
        df_pred[name] <- 0
      }
    }
    data <- MRData()
    data$load_df(df_pred, col_covs = as.list(model$cov_names))
    outer_draws <- model$create_draws(data,
                                      beta_samples = beta_samples,
                                      gamma_samples = gamma_outer_samples)
    
    draws <- as.data.frame(
      cbind(data_info$pred_cov, outer_draws)
    )
    names(draws) <- c(data_info$cont_cov, sapply(1:num_draws, function(i) paste0("draw_", i)))
    
    # write draws
    dir.create(paste0(folder,ver,"/rrs/"), recursive = TRUE)
    write.csv(draws,
              file = paste0(folder,ver,"/rrs/rr_draws_", ro_pair, ".csv"),
              row.names = FALSE)
    
  }
}
