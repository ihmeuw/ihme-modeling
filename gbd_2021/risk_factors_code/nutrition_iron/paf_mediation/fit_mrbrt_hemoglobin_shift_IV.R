rm(list = ls())
library(ggplot2)
library(data.table)
library(openxlsx)
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH") # for R version 3.6.3
set.seed(27)
source("FILEPATH/plotting_functions.R")
out_dir <- "FILEPATH/MR_BRT"
#--------------------------

outcomee <- "Hemoglobin level"
age <- "pregnant women"
trimming <- TRUE
intercept_only <- FALSE
label <- "_IVoral"

version <- "2021_03_04"

save_dir <- paste0(out_dir,"/" ,"hemoglobin_shift",version)

# Step 1 - load in data that is Oral-placebo
#---------------------------

if(!file.exists(paste0(save_dir, "/hemoglobin_data.csv"))){
  
  stop("Need to run oral hemoglobin shift first")
  
}else{
  
  subdata <- fread(paste0(save_dir, "/hemoglobin_data.csv"))
}

# Step 2 - bring in the data that is IV-Oral

oral_iv <- fread(paste0(out_dir, "/oral_iv_studies.csv"))

oral_iv[, mean_diff_ref := ref_mean_base - ref_mean_fup]
oral_iv[, mean_diff_exp := exp_mean_base - exp_mean_fup]
oral_iv[, mean_diff_sd_ref := sqrt( ref_sd_base^2 + ref_sd_fup^2 )]
oral_iv[, mean_diff_sd_exp := sqrt( exp_sd_base^2 + exp_sd_fup^2 )]

oral_iv[, mean_difference := mean_diff_ref - mean_diff_exp]
oral_iv[, mean_difference_se := sqrt( mean_diff_sd_exp^2 + mean_diff_sd_ref^2)]

oral_iv <- oral_iv[!is.na(mean_difference)]

oral_iv[, iv_oral := 1]
oral_iv[, oral_placebo := 0]
oral_iv[, nid:= as.numeric(as.factor(study_name))]
oral_iv[, convert_units := as.character(convert_units)]
oral_iv[, convert_units := "g/L"]

subdata[, iv_oral := 0]
subdata[, oral_placebo := 1]


subdata <- rbind(subdata, oral_iv)
  

  # Step 2 - loop over outcome and run MR-BRT
  #---------------------------
  
  if(trimming){pct_trim <- 0.9}else{
    pct_trim <- 1
  }
  
  outcomedir <- paste0(save_dir, "/", gsub(" ", "_", outcomee),"/", gsub(" ","_", age), ifelse(trimming, "_trim", "_notrim"), ifelse(intercept_only, "_intercept", "_wcovs"), label)
  dir.create(outcomedir, recursive = T)
  
  plot_name <- paste0(outcomedir, "/vetting_plots",label,".pdf")
  pdf(plot_name, height = 8, width = 8)
  
  outdata <- subdata[note_modeler == age]
  outdata[, outcome_2 := as.numeric(outcome_2)]
  
  write.csv(outdata, paste0(outcomedir, "/input_data.csv"), row.names = F)
  
  
  mrdata <- MRData()
  cov_names <- c()
  
  mrdata$load_df(
    data = outdata,
    col_obs = "mean_difference",
    col_obs_se = "mean_difference_se",
    col_study_id = "nid",
    col_covs = as.list(cov_names)
  )
  
  candidate_covs <- cov_names[cov_names != "baseline_hemoglobin"]
  
  covfinder <- CovFinder(
    data = mrdata,
    covs = as.list(candidate_covs),
    pre_selected_covs = list("iv_oral", "oral_placebo"),
    num_samples = 1000L,
    power_range = list(-4, 4),
    power_step_size = 0.05,
    inlier_pct = pct_trim,
    laplace_threshold = 1e-5 #changed from 1e-5
  )
  
  covfinder$select_covs(verbose = FALSE)
  
  new_covs <- covfinder$selected_covs
  print(new_covs)
  
  gplot <- ggplot(outdata, aes(x = exp_mean_fup, y = ref_mean_fup, color = convert_units))+geom_point()+geom_abline(intercept = 0, slope = 1)+
    labs(x = "Exposed group follow up", y = "Reference group follow up", title = paste0(outcomee, "(",age, ")"), shape = "Calculated follow up", color = "original unit")+theme_bw()
  
  print(gplot)
  # Fit MR-BRT - intercept only
  if(intercept_only){
    
    #prep data
    dat1 <- MRData()
    dat1$load_df(
      data = outdata,
      col_obs = "mean_difference",
      col_obs_se = "mean_difference_se",
      col_covs = list(),
      col_study_id = "nid")
    
    #fit mr-brt model
    mod1 <- MRBRT(
      data = dat1,
      cov_models = list(
        LinearCovModel("intercept", use_re = T)
      ),
      inlier_pct = pct_trim
    )
    
    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    ############ SAVE
    py_save_object(object = mod1, 
                   filename = paste0(outcomedir, "/mod.pkl"), 
                   pickle = "dill")
    
    ##############
    
    
    # mean prediction only
    pred_data <- data.table("intercept"=c(1))
    dat_pred1 <- MRData()
    
    dat_pred1$load_df(
      data = pred_data,
      col_covs = list("intercept")
    )
  }else if(length(new_covs) > 0){
    
    #prep data
    dat1 <- MRData()
    dat1$load_df(
      data = outdata,
      col_obs = "mean_difference",
      col_obs_se = "mean_difference_se",
      col_covs = as.list(new_covs),
      col_study_id = "nid")
    
    
    cov_models1 <- list(
      #LinearCovModel("intercept", use_re = T)
    )
    
    #---------------------------------------------
    #leave without random effects on bias
    for (cov in new_covs){ 
      
      re <- T
      cov_models1 <- append(cov_models1,
                            list(do.call(
                              LinearCovModel,
                              c(list(alt_cov=cov, use_re = re)
                              ))))
      
    }
    #---------------------------------------------
    #fit mr-brt model
    mod1 <- MRBRT(
      data = dat1,
      cov_models = cov_models1,
      inlier_pct = pct_trim
    )
    
    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    
    ############ SAVE
    py_save_object(object = mod1, 
                   filename = paste0(outcomedir, "/mod.pkl"), 
                   pickle = "dill")
    
    ##############
    
    
    # predict out at 0s
    pred_data <- expand.grid("intercept"=c(0), "iv_oral" = c(0,1), "oral_placebo" = c(0,1))
    
    dat_pred1 <- MRData()
    
    dat_pred1$load_df(
      data = pred_data,
      col_covs = as.list(new_covs)
    )
    
    
  }else{
    print("No covariates selected for this one, on to next!")
    next
  } #end loop
  
  
  # save model summary
  beta_result <- as.data.table(t(py_to_r(mod1$summary()[[1]])), keep.rownames = "parameter")
  setnames(beta_result, "0", "beta")
  gamma_result <- as.data.table(t(py_to_r(mod1$summary()[[2]])),keep.rownames = "parameter")
  setnames(gamma_result, "0", "gamma")
  parameter_results <- merge(beta_result, gamma_result, by = "parameter", all = T)
  
  write.csv(parameter_results, paste0(outcomedir, "/parameter_estimates.csv"), row.names = F)
  
  # get uncertainty
  
  n_samples <- 1000L
  samples3 <- mod1$sample_soln(sample_size = n_samples)
  
  draws3 <- mod1$create_draws(
    data = dat_pred1,
    beta_samples = samples3[[1]],
    gamma_samples = samples3[[2]],
    random_study = FALSE)
  draws2 <- mod1$create_draws(
    data = dat_pred1,
    beta_samples = samples3[[1]],
    gamma_samples = samples3[[2]],
    random_study = TRUE)
  
  # draws with gamma
  #add exp to draws
  colnames(draws2) <- paste0("draw_", 0:(n_samples-1))
  draws_w_gamma <- cbind(pred_data, as.data.table(draws2))
  
  pred_data$Y_mean <- mod1$predict(dat_pred1, sort_by_data_id = T)
  pred_data$Y_mean_fe <- apply(draws3, 1, function(x) mean(x))
  pred_data$Y_mean_lo_fe <- apply(draws3, 1, function(x) quantile(x, 0.025))
  pred_data$Y_mean_hi_fe <- apply(draws3, 1, function(x) quantile(x, 0.975))
  
  pred_data$Y_mean_re <- apply(draws2, 1, function(x) mean(x))
  pred_data$Y_mean_lo_re <- apply(draws2, 1, function(x) quantile(x, 0.025))
  pred_data$Y_mean_hi_re <- apply(draws2, 1, function(x) quantile(x, 0.975))
  
  write.csv(pred_data, paste0(outcomedir, "/prediction_data_all.csv"), row.names = F)
  
  
  # make a plot
  pred_data <- as.data.table(pred_data)
  
  pred_data2 <- pred_data[iv_oral==1 &  oral_placebo==1]
  
  plot_dich_simple_forest(dat1, mod1, pred_data2, rr_lab = "Mean Difference", plot_title = paste0(outcomee," ", label, "oral-placebo"))
  plot_dich_simple_funnel(dat1, mod1, pred_data2, rr_lab = "Mean Difference", plot_title = paste0(outcomee," ", label))
  
  dev.off()
  
