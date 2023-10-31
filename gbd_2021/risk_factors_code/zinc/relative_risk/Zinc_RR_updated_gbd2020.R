################################################################################
## DESCRIPTION ##  Dichotomous Zinc, final GBD 2020 risk curves for zinc
## OUTPUTS ##
## AUTHOR ##   
## DATE ##    
################################################################################

# load packages and libraries
rm(list = ls())
source("FILEPATH/plotting_functions.R")
library(mrbrt001, lib.loc = "FILEPATH") # for R version 3.6.3
library(reticulate)
library(data.table)
library(ggplot2)
# set seed
np <- import("numpy")
np$random$seed(as.integer(2738))
source("FILEPATH/dichotomous_functions.R")

# Make date version dir
invisible(loadNamespace("ihme.covid", lib.loc = "FILEPATH"))
my_data_dir <- "FILEPATH"
my_output_dir <- "FILEPATH"
version_dir <- ihme.covid::get_output_dir(root = my_output_dir, date = "today")
print(paste0("Saving to ", version_dir))


# --------------------------------------------------------------------------------
outcome <- c("Diarrhea", "LRTI")
test_covs <- c(0) # if 1, incidence will be included as a covariate. Keep =0
# --------------------------------------------------------------------------------


pdf(paste0(version_dir, "/vetting_plots.pdf"), width = 11, height = 8)
all_results <- data.table()
pub_bias_results <- data.table()

for(out in outcome){
  for(bias_cov in test_covs){
    
    # load data from whereever it belongs
    outdata <- fread(paste0(my_data_dir, out, "_data_file.csv"))
    
    # trimming for both outcomes
    pct_trim <- 1
    
    if(bias_cov ==1){
      # list of candidate covs. Should be dichotomized where we will predict for 0
      candidate_covs <- c("cv_incidence")
      
      mrdata <- MRData()
      mrdata$load_df(
        data = outdata,
        col_obs = "log_effect_size",
        col_obs_se = "log_se",
        col_study_id = "nid",
        col_covs = as.list(candidate_covs)
      )
      
      
      # run covfinder to see if any covariates are significant
      covfinder <- CovFinder(
        data = mrdata,
        covs = as.list(candidate_covs),
        pre_selected_covs = list(),
        num_samples = 1000L,
        power_range = list(-4, 4),
        power_step_size = 0.05,
        inlier_pct = pct_trim,
        laplace_threshold = 1e-5
      )
      
      covfinder$select_covs(verbose = FALSE)
      new_covs <- covfinder$selected_covs
      print(new_covs)
      
      if(length(new_covs) ==0){
        next
      }
      
      cov_lab <- "wcov"
    }else{ 
      new_covs <- NULL
      cov_lab <- "wo cov"
    }
    
    #prep data and covariates for mrbrt
    dat1 <- MRData()
    dat1$load_df(
      data = outdata,
      col_obs = "log_effect_size",
      col_obs_se = "log_se",
      col_covs = as.list(new_covs),
      col_study_id = "nid")
    
    cov_models1 <- list(
      LinearCovModel("intercept", use_re = T)
    )
    
    # I treat as z-covs (use_re = T)
    # leave without random effects on bias
    for (cov in new_covs) cov_models1 <- append(cov_models1,
                                                list(do.call(
                                                  LinearCovModel,
                                                  c(list(alt_cov=cov, use_re = F)
                                                  ))))
    #fit mr-brt model
    mod1 <- MRBRT(
      data = dat1,
      cov_models = cov_models1,
      inlier_pct = pct_trim
    )
    
    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    py_save_object(object = mod1, 
                   filename = paste0(version_dir, "/", out, "_model.pkl"), 
                   pickle = "dill")
    
    # make a predict data frame
    if(length(new_covs) > 0 ){
      pred_data <- as.data.table(expand.grid("intercept"=c(0), "cv_incidence" = c(0)))
      dat_pred1 <- MRData()
      
      dat_pred1$load_df(
        data = pred_data,
        col_covs = list(new_covs)
      )
      
    }else{
      pred_data <- as.data.table(expand.grid("intercept"=c(0), "cv_incidence" = c(0)))
      dat_pred1 <- MRData()
      
      dat_pred1$load_df(
        data = pred_data,
        col_covs = list("intercept")
      )
    }
    
    # Create draws and prediction
    sampling <- import("mrtool.core.other_sampling")
    num_samples <- 1000L
    beta_samples <- sampling$sample_simple_lme_beta(num_samples, mod1)
    gamma_samples <- rep(mod1$gamma_soln, num_samples) * matrix(1, num_samples)
    
    
    # make draws with and without gamma
    draws3 <- mod1$create_draws(
      data = dat_pred1,
      beta_samples = beta_samples,
      gamma_samples = gamma_samples,
      random_study = FALSE)
    
    pred_data$Y_mean <- mod1$predict(dat_pred1, sort_by_data_id = T)
    pred_data$Y_mean_fe <- apply(draws3, 1, function(x) mean(x))
    pred_data$Y_mean_lo_fe <- apply(draws3, 1, function(x) quantile(x, 0.025))
    pred_data$Y_mean_hi_fe <- apply(draws3, 1, function(x) quantile(x, 0.975))
    
    draws2 <- mod1$create_draws(
      data = dat_pred1,
      beta_samples = beta_samples,
      gamma_samples = gamma_samples,
      random_study = TRUE)
    
    pred_data$Y_mean_meanre <- apply(draws2, 1, function(x) mean(x))
    pred_data$Y_mean_lo_meanre <- apply(draws2, 1, function(x) quantile(x, 0.025))
    pred_data$Y_mean_hi_meanre <- apply(draws2, 1, function(x) quantile(x, 0.975))
    
    
    # publication bias and fisher information boost draws
    #-----------------------------------------------------
    
    
    ### Extract data
    df <- extract_data_info(mod1)
    
    ### Detect publication bias
    egger_model_all <- egger_regression(df$residual, df$residual_se)
    egger_model <- egger_regression(df[!df$outlier,]$residual, df[!df$outlier,]$residual_se)
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
      
      # refit the model
      data = MRData()
      data$load_df(
        data=df[!df$outlier,],
        col_obs = "obs",
        col_obs_se = "obs_se",
        col_covs = as.list(mod1$cov_names),
        col_study_id = "study_id"
      )
      model_fill <- MRBRT(data, cov_models=mod1$cov_models)
      model_fill$fit_model()
    } else {
      model_fill <- NULL
    }
    
    ### Extract scores
    uncertainty_info <- get_uncertainty_info(mod1)
    if (is.null(model_fill)) {
      uncertainty_info_fill <- NULL
    } else {
      uncertainty_info_fill <- get_uncertainty_info(model_fill)
    }
    
    
    ### Output diagnostics
    plot_model(df, uncertainty_info, mod1, uncertainty_info_fill, model_fill, out)
    summary <- summarize_model(out, mod1, model_fill, egger_model, egger_model_all, uncertainty_info)
    pub_bias_results <- rbind(pub_bias_results, summary, fill = T)
    draws <- get_draws(mod1)
    
    
    pred_data$Y_mean_re <- mean(draws$draw)
    pred_data$Y_mean_lo_re <-  quantile(draws$draw, 0.025)
    pred_data$Y_mean_hi_re <-  quantile(draws$draw, 0.975)
    
    
    pred_data_plot <- pred_data[intercept == 0 & cv_incidence ==0]
    
    trim_lab <- ifelse(pct_trim ==1, "no trim", "10% trimming")
    
    # you can use these functions! just make sure pred_data only has one row if you do (will append to the plots as results)
    a <- plot_dich_simple_forest(dat1, mod1, pred_data_plot, rr_lab = "log(RR)", plot_title = paste0("Zinc - ",out,", ", trim_lab, ", ", cov_lab))
    b <- plot_dich_simple_funnel(dat1, mod1, pred_data_plot, rr_lab = "log(RR)", plot_title = paste0("Zinc - ",out,", ", trim_lab, ", ", cov_lab))
    
    fe_val <- paste0(exp(pred_data$Y_mean_fe)," (", exp(pred_data$Y_mean_hi_fe)," , ", exp(pred_data$Y_mean_lo_fe), ")")
    re_val <- paste0(exp(pred_data$Y_mean_re)," (", exp(pred_data$Y_mean_hi_re)," , ", exp(pred_data$Y_mean_lo_re), ")")
    
    pred_data[, "with gamma":= re_val]
    pred_data[, "wo gamma":= fe_val]
    pred_data[, outcome := out]
    pred_data[, pct_trim := 1-pct_trim]
    pred_data[, cov := cov_lab]
    pred_data <- pred_data[intercept ==0]
    
    
    all_results <- rbind(all_results, pred_data, fill = T)
    
    
    draws_wide <- as.data.table(dcast(draws, ... ~ draw))
    draws_wide <- draws_wide[,2:1001]
    
    draw_cols <- paste0("draw_", 0:999)
    setnames(draws_wide, colnames(draws_wide), draw_cols)
    
    # Already in harmful space, exponentiate space
    draws_wide[, (draw_cols) := lapply(.SD, function(x)
      exp(x)), .SDcols = draw_cols]
    
    write.csv(draws_wide, paste0(version_dir, "/", out, "_draws_gamma_fib.csv"), row.names = F)
    
    
  }
}
dev.off()

write.csv(all_results, paste0(version_dir, "/model_estimates.csv"), row.names = F)
write.csv(pub_bias_results, paste0(version_dir, "/pub_bias_results.csv"), row.names = F)

