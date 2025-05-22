  #--------------------------------------------------------------
  # Date: 27 Feb 2021
  # Project: GBD nonfatal COVID
  # Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
  #--------------------------------------------------------------
  
  # setup -------------------------------------------------------
  
  # clear workspace
  rm(list=ls())
  setwd("FILEPATH")
  
  # map drives
  if (Sys.info()['sysname'] == 'Linux') {
    "DRIVE" <- "FILEPATH"
    "DRIVE" <- "FILEPATH"
  } else {
    "DRIVE" <- "FILEPATH"
    "DRIVE" <- "FILEPATH"
  }
  
  
  # load packages
  pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
  library(reticulate) 
  #library(mrbrt001, lib.loc = 'FILEPATH')
  #source("FILEPATH/rstudio_singularity_4034_patch.R")
  #remove_python_objects()
  library(mrbrt002, lib.loc = "FILEPATH")
  library(plyr)
  library(msm)
#  library(msm, lib.loc = "FILEPATH")
  

  folder <- "FILEPATH"
#  data <- "FILEPATH/long_covid_extraction_03.30.2021.xlsx"
  outputfolder <- "FILEPATH"
  
  
  cvs <- list("hospital", "icu", "female", "male", "follow_up_days", "other_list")
  version <- 38
  datadate <- '051321'
  n_samples <- 1000L
  max_draw <- n_samples - 1
  year_start <- 2020
  year_end <- 2023
  inlier <- 0.9
  
  
  
  # graphing function
  add_ui <- function(dat, x_var, lo_var, hi_var, color = "darkblue", opacity = 0.2) {
    polygon(
      x = c(dat[, x_var], rev(dat[, x_var])),
      y = c(dat[, lo_var], rev(dat[, hi_var])),
      col = adjustcolor(col = color, alpha.f = opacity), border = FALSE
    )
  }
  
  
  
  ################################################################
  # READ DATA
  ################################################################
  
  dataset <- read.csv(paste0(outputfolder, "prepped_data_", datadate, ".csv"))
  
  dim(dataset)
  
  dataset <- data.table(dataset)
  dataset <- dataset[!(study_id=="PRA" & outcome=="any")]
  dataset <- dataset[is_outlier!=1]
  dataset <- dataset[!is.na(standard_error)]
  table(dataset$study_id, dataset$outcome)
  
  ######################################################################################
  #   run model with all data with multiple follow up times, to get duration
  #      and beta on follow_up_days to use as prior for cluster-specific models
  ######################################################################################
  
  dataset$log_mean <- log(dataset$mean)
  logitvals <- linear_to_logit(mean = array(dataset$mean), sd = array(dataset$standard_error))
  logitmean <- logitvals[1]
  logitse <- logitvals[2]
  dataset$logitmean <- logitmean
  dataset$logitse <- logitse
  
  ### basically a loop that goes through each row and calcs the se in log space
#  dataset$delta_log_se <- sapply(1:nrow(dataset), function(i) {
#    ratio_i <- dataset[i, "mean"] # relative_risk column
#    ratio_se_i <- dataset[i, "standard_error"]
#    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
#  })
#  dataset$log_se <- dataset$delta_log_se
  
  
  
  ##########################################################
  ##  HOSPITAL CASES: DURATION
  ##########################################################
  out <- "duration"
  message(paste0("working on duration"))
  df <- dataset
  df <- df[age_specific==0]
  df <- df[study_id=="Cirulli et al" | study_id=="CO-FLOW" | study_id=="CSS" | study_id=="Faroe" | (study_id=="UK CIS" & outcome=='any') | study_id=="Zurich CC"]
  # if dropping zeroes, drop rsp and cog from zurich study because then left with only one time point for each
  if (version>=20) {
    df <- df[!(study_id=="Zurich CC" & (outcome=="rsp" | outcome=="cog"))]
  }
  table(df$study_id, df$outcome)
  
  df <- df[female==0 & male==0 & !is.na(standard_error) & !is.na(logitse)]
  
  df$study_id <- paste(df$study_id, df$outcome)
  df <- df[outcome=="any" | outcome=="fat" | outcome=="cog" | outcome=="rsp"]
  table(df$study_id, df$outcome)
  #df <- df[outcome=="any"]
  
  # set up folder
  model_dir <- paste0(out, "_v", version, "/")
  dir.create(paste0(outputfolder, model_dir))
  dir.create(paste0(outputfolder, "plots"))
  df <- df[hospital_or_icu==1]
  # set up data
  mr_df <- MRData()
  
  mr_df$load_df(
    data = df, col_obs = "logitmean", col_obs_se = "logitse",
    col_covs = list("follow_up_days"), col_study_id = "study_id")
  
  model <- MRBRT(
    data = mr_df,
    cov_models =list(
      LinearCovModel("intercept", use_re = TRUE),
      LinearCovModel("follow_up_days", use_re = FALSE)
    ),
    inlier_pct = 1)
  #  LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_uniform = array(c(min, max)))
  
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  (coeffs <- rbind(model$cov_names, model$beta_soln))
  write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs.csv"))
  
  # save model object
  py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1.pkl"), pickle = "dill")
  #py_save_object(object = model_no, filename = paste0(folder, "mrbrt_model_outputs/", model_dir_no, "mod1.pkl"), pickle = "dill")
  
  # make predictions for full day-series
  # subtract one from the day count because we start follow up at day 0
  days <- (year_end - year_start + 1) * 365
  predict_matrix_midmod <- data.table(intercept = model$beta_soln[1], follow_up_days = c(0:days))
  predict_matrix_hospicu <- data.table(intercept = model$beta_soln[1], follow_up_days = c(0:days))
  predict_matrix <- predict_matrix_hospicu
  
  predict_data <- MRData()
  predict_data$load_df(
    data = predict_matrix,
    col_covs=list("follow_up_days"))
  
  #n_samples <- 1000L
  samples <- model$sample_soln(sample_size = n_samples)
  beta_hosp <- mean(samples[[1]][,2])
  beta_hosp_sd <- sd(samples[[1]][,2])
  
  
  draws_raw <- model$create_draws(
    data = predict_data,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = TRUE,
    sort_by_data_id = TRUE)

  # write draws for pipeline
  draws_raw <- data.table(draws_raw)
  
  draws_raw <- cbind(draws_raw, predict_matrix)
  setnames(draws_raw, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
  draws_raw <- melt(data = draws_raw, id.vars = c("intercept", "follow_up_days"))
  setnames(draws_raw, "variable", "draw")
  setnames(draws_raw, "value", "proportion")
  draws_raw$proportion <- exp(draws_raw$proportion) / (1 + exp(draws_raw$proportion))
  
  draws_raw$intercept <- NULL
  draws_raw <- reshape(draws_raw, idvar = c("follow_up_days"), timevar = "draw", direction = "wide")
  setnames(draws_raw, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  draws_save <- draws_raw
  
  draws_save$hospital_icu <- 1
  write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws.csv"))
  
  
  predict_matrix$pred_raw <- model$predict(predict_data, sort_by_data_id = TRUE)
  predict_matrix$pred <- exp(predict_matrix$pred_raw) / (1 + exp(predict_matrix$pred_raw))
  predict_matrix$pred_lo <- apply(draws_raw[,2:ncol(draws_raw)], 1, function(x) quantile(x, 0.025))
  predict_matrix$pred_hi <- apply(draws_raw[,2:ncol(draws_raw)], 1, function(x) quantile(x, 0.975))
  used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
  used_data <- as.data.table(used_data)
  rr_summaries <- copy(predict_matrix)
  
  rr_summaries
  rr_summaries$pred_hi[rr_summaries$pred_hi>1] <- 1
  rr_summaries$gamma <- mean(samples[[2]])
  write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary.csv"))
  
  
  dur <- data.table(draws_raw)
  dur <- melt(data = dur, id.vars = c("follow_up_days"))
  head(dur)
  dur0 <- dur[follow_up_days==0]
  dur0$follow_up_days <- NULL
  dur0 <- cbind(dur0, samples[[1]])
  dur$value <- NULL
  dur <- merge(dur, dur0, by = c("variable"))
  setnames(dur, "value", "prop_start")
  setnames(dur, "variable", "draw")
  setnames(dur, "V1", "beta0")
  setnames(dur, "V2", "beta1")
  setnames(dur, "follow_up_days", "day")
  dur
  write.csv(dur0, file = paste0(outputfolder, "duration_parameters_hospicu.csv"))
  
  #####################
  # Create annual durations for an incident case in the given or previous years
  #####################
  
  # day end for threshold of proportion going below 0.001, for calculation of overall average duration 
  # (not used directly in analysis, but rather for presentations, etc)
  dur$day_end_whole <- (log(0.001/(1-0.001))-dur$beta0)/dur$beta1
  
  dur <- dur[, year := ifelse(day<=364, 1, ifelse(day<=729, 2, ifelse(day<=1094, 3, 4)))]
  dur <- dur[, day_end := ifelse(day<=364, 364, ifelse(day<=729, 729, ifelse(day<=1094, 1094, 1459)))]
  
  dur <- dur[, C := -(1 / beta1) * log(abs(1 + exp(beta0)))]
  dur <- dur[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 0))) + C]
  dur <- dur[, integral_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end-day)))) + C]
  dur <- dur[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * day_end_whole))) + C]
  dur <- dur[, integral := integral_end - integral_start]
  dur <- dur[, integral_whole := integral_very_end - integral_start]
  dur <- dur[, prop_start := exp(beta0) / (1+exp(beta0))]

  # calculate duration by year (for the pipeline) and overall (for reporting)
  dur$duration <- dur$integral / dur$prop_start
  dur$duration_whole <- dur$integral_whole / dur$prop_start
  dur
  dur$hospital_icu <- 1
  (hospdur <- median(dur$duration_whole))
  dur$outcome <- "fat_or_resp_or_cog"
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_hospicu_full.csv"))
  dur <- dur[,c("draw", "day", "hospital_icu", "outcome", "duration")]
  dur <- reshape(dur, idvar = c("hospital_icu", "outcome", "day"), timevar = "draw", direction = "wide", sep="_")
  setnames(dur, paste0("duration_draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  dur[1:5,1:15]
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_hospicu_clean.csv"))
  write.csv(dur, file =paste0(outputfolder, "duration_draws_hospicu_clean_by_day.csv"))
  
  
  
  used_data$obs_exp <- exp(used_data$obs) / (1 + exp(used_data$obs))
  used_data <- used_data[order(obs)]
  df <- df[order(logitmean)]
  used_data$se <- df$standard_error
  used_data$follow_up_weeks <- used_data$follow_up_days/7
  rr_summaries$follow_up_weeks <- rr_summaries$follow_up_days/7
  used_data$weight <- 1/(10*used_data$se)
  #used_data$weight <- 1/(used_data$obs_se)
  rr_summaries$coflow <- 1
  used_data$weight[used_data$weight>10] <- 10
  rr_summaries <- rr_summaries[follow_up_days<=365]
  
  # plot results of model using data that does control for blood pressure
  plot <- ggplot(data=rr_summaries, aes(x=follow_up_days, y=pred), fill = "blue")+
    geom_ribbon(data= rr_summaries[coflow==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries[coflow==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries[coflow==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries[coflow==1], aes(x=follow_up_days, y=pred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste(out, ", hospital/ICU COVID cases: ", round(hospdur, 1), " days")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=1) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs_exp) , color="dark gray", shape=1, alpha=1) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    guides(fill=FALSE) +
    guides(color = guide_legend(override.aes = list(size = 3))) +
    theme(legend.title = element_text(size = 8), 
          legend.text = element_text(size = 8)) +
    theme(legend.spacing.x = unit(.2, 'cm'))
  plot
  #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_hosp_duration.pdf"), width = 8, height = 5)
  
  
  
  coeffs
  ##########################################################
  ##  COMMUNITY CASES: DURATION
  ##########################################################
  
  out <- "duration"
  message(paste0("working on duration"))
  df <- dataset[!is.na(outcome) | study_id=="CSS"]
  if (version<18) {
    df <- df[!(study_id=="Cirulli et al" & outcome=="any")]
    df <- df[!(study_id=="UK CIS" & outcome=="any")]
  }
  df <- df[!is.na(mean)]
  
#  nonzero_min <- min(df$mean[df$mean!=0 & !is.na(df$mean)])
#  df$log_mean <- log(df$mean+0.01*nonzero_min)
#  df$mean2 <- df$mean+0.1*nonzero_min
#  df[standard_error==0 | is.na(standard_error), standard_error := sqrt(((mean+0.01*nonzero_min) * (1-(mean+0.01*nonzero_min))) /  sample_size_envelope)]

  
  table(df$study_id, df$outcome)
  table(df$study_id, df$sex)
  table(df$study_id, df$female)
  df <- df[age_specific==0]
  #df <- df[(study_id=="CSS" | study_id=="Zurich CC") & female==0.5]
  df <- df[female==0 & male==0 & !is.na(standard_error) & !is.na(logitse)]
  df <- df[study_id=="Cirulli et al" | study_id=="CO-FLOW" | study_id=="CSS" | study_id=="Faroe" | (study_id=="UK CIS" & outcome=='any') | study_id=="Zurich CC"]
  table(df$study_id, df$female)
  df$study_id <- paste(df$study_id, df$outcome)
  df <- df[outcome=="any" | outcome=="fat" | outcome=="cog" | outcome=="rsp"]
  #df <- df[outcome=="any"]
  table(df$study_id, df$outcome)
  
  # set up folder
  model_dir <- paste0(out, "_v", version, "/")
  dir.create(paste0(outputfolder, model_dir))
  dir.create(paste0(outputfolder, "plots"))
  df <- df[hospital_or_icu==0]
  # set up data
  mr_df <- MRData()
  
    mr_df$load_df(
      data = df, col_obs = "logitmean", col_obs_se = "logitse",
      col_covs = list("follow_up_days", "other_list"), col_study_id = "study_id")
    
    model <- MRBRT(
      data = mr_df,
      cov_models =list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("follow_up_days", use_re = FALSE),
        LinearCovModel("other_list", use_re = FALSE)
      ),
      inlier_pct = 1)
  #  LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_uniform = array(c(min, max)))
  
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  (coeffs <- rbind(model$cov_names, model$beta_soln))
  write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs.csv"))
  
  # save model object
  py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1.pkl"), pickle = "dill")

  # make predictions for full year
  predict_matrix_midmod <- data.table(intercept = model$beta_soln[1], follow_up_days = c(0:days), other_list = 0)
  predict_matrix_hospicu <- data.table(intercept = model$beta_soln[1], follow_up_days = c(0:days), other_list = 0)
  predict_matrix <- predict_matrix_hospicu
  
  if (version<18) {
    predict_data <- MRData()
    predict_data$load_df(
      data = predict_matrix,
      col_covs=list("follow_up_days"))
  } else {
    predict_data <- MRData()
    predict_data$load_df(
      data = predict_matrix,
      col_covs=list("follow_up_days", "other_list"))
  }
  
  samples <- model$sample_soln(sample_size = n_samples)
  (beta_comm <- mean(samples[[1]][,2]))
  (beta_comm_sd <- sd(samples[[1]][,2]))
  if (version>=18) {
    (beta_other_list <- mean(samples[[1]][,3]))
    (beta_other_list_sd <- sd(samples[[1]][,3]))
  }
  
  draws_raw <- model$create_draws(
    data = predict_data,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = TRUE,
    sort_by_data_id = TRUE)
  # write draws for pipeline
  draws_raw <- data.table(draws_raw)
  draws_raw <- cbind(draws_raw, predict_matrix)
  setnames(draws_raw, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
  draws_raw$other_list <- NULL
  draws_raw <- melt(data = draws_raw, id.vars = c("intercept", "follow_up_days"))
  setnames(draws_raw, "variable", "draw")
  setnames(draws_raw, "value", "proportion")
  draws_raw$proportion <- exp(draws_raw$proportion) / (1 + exp(draws_raw$proportion))
  
  draws_raw$intercept <- NULL
  draws_raw <- reshape(draws_raw, idvar = c("follow_up_days"), timevar = "draw", direction = "wide")
  setnames(draws_raw, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  draws_save <- draws_raw
  
  draws_save$hospital_icu <- 0
  write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws.csv"))
  
  
  
  
  
  dur <- data.table(draws_raw)
  dur <- melt(data = dur, id.vars = c("follow_up_days"))
  head(dur)
  dur0 <- dur[follow_up_days==0]
  dur0$follow_up_days <- NULL
  dur0 <- cbind(dur0, samples[[1]])
  dur$value <- NULL
  dur <- merge(dur, dur0, by = c("variable"))
  setnames(dur, "value", "prop_start")
  setnames(dur, "variable", "draw")
  setnames(dur, "V1", "beta0")
  setnames(dur, "V2", "beta1")
  dur$V3 <- NULL
  setnames(dur, "follow_up_days", "day")
  dur
  coeffs
  write.csv(dur0, file = paste0(outputfolder, "duration_parameters_midmod.csv"))
  
  
  # day end for threshold of proportion going below 0.001
  dur$day_end_whole <- (log(0.001/(1-0.001))-dur$beta0)/dur$beta1

  dur <- dur[, year := ifelse(day<=364, 1, ifelse(day<=729, 2, ifelse(day<=1094, 3, 4)))]
  dur <- dur[, day_end := ifelse(day<=364, 364, ifelse(day<=729, 729, ifelse(day<=1094, 1094, 1459)))]
  
  dur <- dur[, C := -(1 / beta1) * log(abs(1 + exp(beta0)))]
  dur <- dur[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 0))) + C]
  dur <- dur[, integral_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end - day)))) + C]
  dur <- dur[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * day_end_whole))) + C]
  dur <- dur[, integral := integral_end - integral_start]
  dur <- dur[, integral_whole := integral_very_end - integral_start]
  dur <- dur[, prop_start := exp(beta0) / (1+exp(beta0))]
  
  
  #dur$integral <- exp(dur$beta0)*exp(dur$beta1*dur$day_end)/dur$beta1 - exp(dur$beta0)*exp(dur$beta1*0)/dur$beta1
  dur$duration <- dur$integral / dur$prop_start
  dur$duration_whole <- dur$integral_whole / dur$prop_start

  dur
  dur$hospital_icu <- 0
  (commdur <- median(dur$duration_whole))
  dur$outcome <- "fat_or_resp_or_cog"
  
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_midmod_full.csv"))
  dur <- dur[,c("draw", "day", "hospital_icu", "outcome", "duration")]
  dur <- reshape(dur, idvar = c("hospital_icu", "outcome", "day"), timevar = "draw", direction = "wide", sep="_")
  setnames(dur, paste0("duration_draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  dur[1:5,1:15]
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_midmod_clean.csv"))
  write.csv(dur, file =paste0(outputfolder, "duration_draws_midmod_clean_by_day.csv"))


  
  
  predict_matrix$pred_raw <- model$predict(predict_data, sort_by_data_id = TRUE)
  predict_matrix$pred <- exp(predict_matrix$pred_raw) / (1 + exp(predict_matrix$pred_raw))
  predict_matrix$pred_lo <- apply(draws_raw[,2:ncol(draws_raw)], 1, function(x) quantile(x, 0.025))
  predict_matrix$pred_hi <- apply(draws_raw[,2:ncol(draws_raw)], 1, function(x) quantile(x, 0.975))
  used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
  used_data <- as.data.table(used_data)
  rr_summaries <- copy(predict_matrix)
  
  rr_summaries
  rr_summaries$pred_hi[rr_summaries$pred_hi>1] <- 1
  rr_summaries$gamma <- mean(samples[[2]])
  write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary.csv"))
  
  
  
  used_data$obs_exp <- exp(used_data$obs) / (1 + exp(used_data$obs))
  used_data <- used_data[order(obs)]
  df <- df[order(logitmean)]
  used_data$se <- df$standard_error
  used_data$follow_up_weeks <- used_data$follow_up_days/7
  rr_summaries$follow_up_weeks <- rr_summaries$follow_up_days/7
  used_data$weight <- 1/(20*used_data$se)
  rr_summaries$coflow <- 0
  used_data$weight[used_data$weight>10] <- 10
  used_data$weight[used_data$weight<2] <- 2
  rr_summaries <- rr_summaries[follow_up_days<=365]
  
  ############################
  # GRAPHS WITH RAW DATA
  # plot results of model using data that does control for blood pressure
  plot <- ggplot(data=rr_summaries, aes(x=follow_up_days, y=pred), fill = "blue")+
    geom_ribbon(data= rr_summaries[coflow==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries[coflow==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries[coflow==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries[coflow==1], aes(x=follow_up_days, y=pred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste(out, ", mild/moderate COVID cases: ", round(commdur, 1), " days")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.5,0.1), limits=c(0,0.5)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs_exp) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    guides(fill=FALSE) +
    guides(color = guide_legend(override.aes = list(size = 3))) +
    theme(legend.title = element_text(size = 8), 
          legend.text = element_text(size = 8)) +
    theme(legend.spacing.x = unit(.1, 'cm'))
  plot
  ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_community_duration_zoom.pdf"), width = 8, height = 5)
  
  
  
  
  rr_summaries$logitpred <- rr_summaries$pred_raw
  rr_summaries$logitpred_hi <- log(rr_summaries$pred_hi / (1-rr_summaries$pred_hi))
  rr_summaries$logitpred_lo <- log(rr_summaries$pred_lo / (1-rr_summaries$pred_lo))
  #rr_summaries2 <- rr_summaries[follow_up_weeks<=20]
  rr_summaries2 <- rr_summaries
  used_data$weight[used_data$weight>10] <- 10
  
  plot <- ggplot(data=rr_summaries2, aes(x=follow_up_days, y=logitpred), fill = "blue")+
    geom_ribbon(data= rr_summaries2[coflow==0], aes(x=follow_up_days, ymin=logitpred_lo, ymax=logitpred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries2[coflow==0], aes(x=follow_up_days, y=logitpred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries2[coflow==1], aes(x=follow_up_days, ymin=logitpred_lo, ymax=logitpred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries2[coflow==1], aes(x=follow_up_days, y=logitpred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste(out, ", mild/moderate COVID cases: ", round(commdur, 1), " days")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(-12,0,2), limits=c(-12,0)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    guides(fill=FALSE) +
    guides(color = guide_legend(override.aes = list(size = 3))) +
    theme(legend.title = element_text(size = 8), 
          legend.text = element_text(size = 8)) +
    theme(legend.spacing.x = unit(.1, 'cm'))
  
  #  guides(shape = guide_legend(override.aes = list(size = 1))) +
  plot
  #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_community_duration_logit.pdf"), width = 8, height = 5)
  
  ############################################
  # GRAPHS WITH ADJUSTED DATA
  
  used_data$obs_adj <- used_data$obs - used_data$other_list * beta_other_list
  used_data$obs_exp <- exp(used_data$obs_adj) / (1 + exp(used_data$obs_adj))

  
  # plot results of model using data that does control for blood pressure
  plot <- ggplot(data=rr_summaries, aes(x=follow_up_days, y=pred), fill = "blue")+
    geom_ribbon(data= rr_summaries[coflow==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries[coflow==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries[coflow==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries[coflow==1], aes(x=follow_up_days, y=pred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste(out, ", mild/moderate COVID cases: ", round(commdur, 1), " days")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.5,0.1), limits=c(0,0.5)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs_exp) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    guides(fill=FALSE) +
    guides(color = guide_legend(override.aes = list(size = 3))) +
    theme(legend.title = element_text(size = 8), 
          legend.text = element_text(size = 8)) +
    theme(legend.spacing.x = unit(.1, 'cm'))
  plot
  ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_community_duration_adjusted.pdf"), width = 8, height = 5)
  
  
  
  plot <- ggplot(data=rr_summaries2, aes(x=follow_up_days, y=logitpred), fill = "blue")+
    geom_ribbon(data= rr_summaries2[coflow==0], aes(x=follow_up_days, ymin=logitpred_lo, ymax=logitpred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries2[coflow==0], aes(x=follow_up_days, y=logitpred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries2[coflow==1], aes(x=follow_up_days, ymin=logitpred_lo, ymax=logitpred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries2[coflow==1], aes(x=follow_up_days, y=logitpred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste(out, ", mild/moderate COVID cases: ", round(commdur, 1), " days")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(-12,0,2), limits=c(-12,0)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    guides(fill=FALSE) +
    guides(color = guide_legend(override.aes = list(size = 3))) +
    theme(legend.title = element_text(size = 8), 
          legend.text = element_text(size = 8)) +
    theme(legend.spacing.x = unit(.1, 'cm'))
  
  #  guides(shape = guide_legend(override.aes = list(size = 1))) +
  plot
  #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_community_duration_log_adjusted.pdf"), width = 8, height = 5)
  
  
  
  coeffs
  
  
  
  
  
  unique(dataset$outcome)
  #beta_hosp <- -0.00327
  #minh <- 1.9*beta_hosp
  #maxh <- 0.1*beta_hosp
  #beta_comm <- -0.0182
  #minc <- 1.9*beta_comm
  #maxc <- 0.1*beta_comm
  
  hospdur
  commdur
  beta_hosp
  beta_comm
  
  ########################################################################################################
  # GET PRIOR FOR ICU COVARIATE
  # using VA and PRA data
  
  df_va <- dataset[study_id=="Veterans Affairs" | (study_id=="PRA" & age_start==0 & age_end==99 & sex=="Both" & hospital_or_icu==1)]
  df_va <- df_va[outcome=='fat' | outcome=='rsp' | outcome=='cog']
  df_va <- df_va[!is.na(standard_error) & !is.na(logitse)]
  df_va$re <- paste0(df_va$study_id, "_", df_va$outcome)
  
  mr_df <- MRData()
  mr_df$load_df(
    data = df_va[hospital==1 | icu==1], col_obs = "logitmean", col_obs_se = "logitse",
    col_covs = list("icu"), col_study_id = "re")
  
  model_va <- MRBRT(
    data = mr_df,
    cov_models =list(
      LinearCovModel("intercept", use_re = TRUE),
      LinearCovModel("icu", use_re = FALSE)
    ),
    inlier_pct = 1)
  
  model_va$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  (coeffs <- rbind(model_va$cov_names, model_va$beta_soln))

  samples <- model_va$sample_soln(sample_size = n_samples)
  beta_icu <- mean(samples[[1]][,2])
  beta_icu_sd <- sd(samples[[1]][,2])
  write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_icu_beta.csv"))
  
  
  
  
  
  
  ########################################################
  # make sure pa-COVID and PRA data with different follow-up times are treated the same (not as a pattern of recovery)
  # random effect = study (and follow-up time for these two sources)
  
  dataset$re <- dataset$study_id
  dataset <- dataset[study_id=="pa-COVID" | study_id=="PRA", re := paste(re, follow_up_value)]
#  dataset <- dataset[study_id=="pa-COVID" | study_id=="PRA", study_id := paste(re, follow_up_value)]
  
  
  ############################################################################################################################################
  dataset <- dataset[!is.na(outcome)]
  table(dataset$symptom_cluster[is.na(dataset$outcome)])
  table(dataset$symptom_cluster, dataset$study_id)
  dataset$weight <- 1/(30*dataset$standard_error)
  dataset$weight[dataset$weight>10] <- 10
  plot(dataset$weight, dataset$standard_error)
  table(dataset$study_id, dataset$outcome)
  
  for(out in c('rsp', 'any', 'fat', 'cog')) {
#  for(out in c('rsp', 'any')) {
      model_dir <- paste0(out, "_v", version, "/")
    dir.create(paste0(outputfolder, model_dir))
    message(paste0("working on ", out))
    df <- dataset[outcome==out]
    df <- df[!is.na(standard_error) & !is.na(logitse)]
    
    
    df <- df[age_specific==0]
    table(df$study_id, df$female)
    df$sex_specific <- 0
    df$sex_specific[df$female==1 | df$male==0] <- 1
    df$sex_keep <- 0
    df$sex_keep[(df$study_id=="Iran")] <- 1
    df$sex_keep[(df$study_id=="Italy ISARIC")] <- 1
    df$sex_keep[(df$study_id=="Sechenov StopCOVID")] <- 1
    df$sex_keep[(df$study_id=="UK CIS" & df$follow_up_value==5)] <- 1
    if(out=="any") {
      df$sex_keep[df$study_id=="Zurich CC" & df$age_start==18 & df$age_end==99] <- 1
    }
    table(df$study_id, df$sex_keep)
    
    table(df$study_id, df$other_list)
    df$follow_up_days_hosp <- df$follow_up_days * df$hospital_icu
    df$follow_up_days_comm <- df$follow_up_days * (1-df$hospital_icu)
    
    
    
  
    
    #############################
    # GET PRIOR ON FEMALE BETA
    
    mr_df <- MRData()
    mr_df$load_df(
      data = df[sex_keep==1], col_obs = "logitmean", col_obs_se = "logitse",
      col_covs = list("female", "male"), col_study_id = "re")
    
    model <- MRBRT(
      data = mr_df,
      cov_models =list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("female", use_re = FALSE),
        LinearCovModel("male", use_re = FALSE)
      ),
      inlier_pct = 1)
    
    # fit model
    model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    samples <- model$sample_soln(sample_size = n_samples)
    (beta_female <- mean(samples[[1]][,2]))
    (beta_female_sd <- sd(samples[[1]][,2]))
    (beta_male <- mean(samples[[1]][,3]))
    (beta_male_sd <- sd(samples[[1]][,3]))
    (coeffs <- rbind(model$cov_names, model$beta_soln))
    write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_female_beta", out, ".csv"))
    
    
    #####################################
    # set up data
    df <- df[(sex_keep==1 & sex!="Both") | (sex_keep==0 & sex=="Both")]
    table(df$study_id, df$sex)
    mr_df <- MRData()
    
    if(out=="any") {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "other_list")
      cvs_c <- list("female", "male", "follow_up_days", "other_list")
      title <- "At least 1 symptom cluster"
    } else if (out=="cog") {
  #    df <- df[(sex_specific==1 & (study_id=="Iran" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
  #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
      cvs_h <- list("icu", "female", "male", "follow_up_days", "memory_problems")
      cvs_c <- list("female", "male", "follow_up_days", "memory_problems")
      title <- "Cognitive symptoms"
    } else if (out=="fat") {
  #    df <- df[(sex_specific==1 & (study_id=="Iran" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
  #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
      cvs_h <- list("icu", "female", "male", "follow_up_days", "fatigue", "administrative")
      cvs_c <- list("female", "male", "follow_up_days", "fatigue", "administrative")
      title <- "Post-acute fatigue syndrome"
    } else if (out=="rsp") {
  #    df <- df[(sex_specific==1 & (study_id=="Iran" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
  #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
      cvs_h <- list("icu", "female", "male", "follow_up_days", "cough", "shortness_of_breath", "administrative")
      cvs_c <- list("female", "male", "follow_up_days", "cough", "shortness_of_breath", "administrative")
      title <- "Respiratory symptoms"
    } else {
  #    df <- df[(sex_specific==1 & (study_id=="Iran" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
  #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
      cvs_h <- list("icu", "female", "male", "follow_up_days")
      cvs_c <- list("female", "male", "follow_up_days")
      title <- out
    }
    
    
  
    
    
    
    mr_df_h <- MRData()
    mr_df_c <- MRData()
    mr_df_h$load_df(
      data = df[hospital==1 | icu==1], col_obs = "logitmean", col_obs_se = "logitse",
      col_covs = cvs_h, col_study_id = "re")
    mr_df_c$load_df(
      data = df[hospital==0 & icu==0], col_obs = "logitmean", col_obs_se = "logitse",
      col_covs = cvs_c, col_study_id = "re")
    
    if(out=="any") {
      model_h <- MRBRT(
        data = mr_df_h,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd)))
        ),
        inlier_pct = inlier)
      
      model_c <- MRBRT(
        data = mr_df_c,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd))),
          LinearCovModel("other_list", use_re = FALSE, prior_beta_gaussian = array(c(beta_other_list, beta_other_list_sd)))
        ),
        inlier_pct = inlier)
      
    } else if (out=="cog") {
      model_h <- MRBRT(
        data = mr_df_h,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*3))),
          LinearCovModel("memory_problems", use_re = FALSE)
        ),
        inlier_pct = inlier)
      
      model_c <- MRBRT(
        data = mr_df_c,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*3))),
          LinearCovModel("memory_problems", use_re = FALSE)
        ),
        inlier_pct = inlier)
    } else if (out=="fat") {
      model_h <- MRBRT(
        data = mr_df_h,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*3))),
          LinearCovModel("fatigue", use_re = FALSE),
          LinearCovModel("administrative", use_re = FALSE)
        ),
        inlier_pct = inlier)
      
      model_c <- MRBRT(
        data = mr_df_c,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*3))),
          LinearCovModel("fatigue", use_re = FALSE),
          LinearCovModel("administrative", use_re = FALSE)
        ),
        inlier_pct = inlier)
    } else if (out=="rsp") {
      model_h <- MRBRT(
        data = mr_df_h,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*3))),
          LinearCovModel("cough", use_re = FALSE),
          LinearCovModel("shortness_of_breath", use_re = FALSE),
          LinearCovModel("administrative", use_re = FALSE, prior_beta_uniform = array(0,2))
        ),
        inlier_pct = inlier)
      
      model_c <- MRBRT(
        data = mr_df_c,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*3))),
          LinearCovModel("cough", use_re = FALSE),
          LinearCovModel("shortness_of_breath", use_re = FALSE),
          LinearCovModel("administrative", use_re = FALSE)
        ),
        inlier_pct = inlier)
    } else if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
      model_h <- MRBRT(
        data = mr_df_h,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*3)))
        ),
        inlier_pct = inlier)
      
      model_c <- MRBRT(
        data = mr_df_c,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*3)))
        ),
        inlier_pct = inlier)
    }
    #  LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_uniform = array(c(min, max)))
    
    
    # fit model
    model_h$fit_model(inner_print_level = 5L, inner_max_iter = 1500L)
    model_c$fit_model(inner_print_level = 5L, inner_max_iter = 1500L)
    
    (coeffs_h <- rbind(model_h$cov_names, model_h$beta_soln))
    coeffs_h <- data.table(coeffs_h)
    coeffs_h$hospital_icu <- 1
    coeffs_h$prior_icu <- beta_icu
    coeffs_h$prior_icu_sd <- beta_icu_sd
    coeffs_h$prior_female <- beta_female
    coeffs_h$prior_female_sd <- beta_female_sd
    coeffs_h$prior_male <- beta_male
    coeffs_h$prior_male_sd <- beta_male_sd
    coeffs_h$prior_follow_up <- beta_hosp
    if (out=='any') {
      coeffs_h$prior_follow_up_sd <- beta_hosp_sd
    } else {
      coeffs_h$prior_follow_up_sd <- beta_hosp_sd*3
    }
    (coeffs_c <- rbind(model_c$cov_names, model_c$beta_soln))
    coeffs_c <- data.table(coeffs_c)
    coeffs_c$hospital_icu <- 0
    coeffs_c$hospital_icu <- 1
    coeffs_c$prior_female <- beta_female
    coeffs_c$prior_female_sd <- beta_female_sd
    coeffs_c$prior_male <- beta_male
    coeffs_c$prior_male_sd <- beta_male_sd
    coeffs_c$prior_follow_up <- beta_comm
    if (out=='any') {
      coeffs_c$prior_follow_up_sd <- beta_comm_sd
    } else {
      coeffs_c$prior_follow_up_sd <- beta_comm_sd*3
    }
    coeffs_c$prior_other_list <- beta_other_list
    coeffs_c$prior_other_list_sd <- beta_other_list_sd
    write.csv(coeffs_h, paste0(outputfolder, model_dir, "coeffs_hospital_icu.csv"))
    write.csv(coeffs_c, paste0(outputfolder, model_dir, "coeffs_community.csv"))
    
    # save model object
    py_save_object(object = model_h, filename = paste0(outputfolder, model_dir, "mod1_h.pkl"), pickle = "dill")
    py_save_object(object = model_c, filename = paste0(outputfolder, model_dir, "mod1_c.pkl"), pickle = "dill")
    
    # make predictions for full year
    predict_matrix_midmod_M <- data.table(intercept = model_c$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=0, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_midmod_F <- data.table(intercept = model_c$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=0, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_hosp_M <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=1, icu=0, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_hosp_F <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=1, icu=0, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_icu_M <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=1, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_icu_F <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=1, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_c <- rbind(predict_matrix_midmod_M,predict_matrix_midmod_F)
    predict_matrix_h <- rbind(predict_matrix_hosp_M,predict_matrix_hosp_F,predict_matrix_icu_M,predict_matrix_icu_F)
    
    predict_data_h <- MRData()
    predict_data_h$load_df(
      data = predict_matrix_h,
      col_covs=cvs_h)
    predict_data_c <- MRData()
    predict_data_c$load_df(
      data = predict_matrix_c,
      col_covs=cvs_c)
    
    
    ## CREATE DRAWS  
    
    samples_h <- model_h$sample_soln(sample_size = n_samples)
    samples_c <- model_c$sample_soln(sample_size = n_samples)
    
    draws_h <- model_h$create_draws(
      data = predict_data_h,
      beta_samples = samples_h[[1]],
      gamma_samples = samples_h[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
    
    # write draws for pipeline
    draws_h <- data.table(draws_h)
    draws_h <- cbind(draws_h, predict_matrix_h)
    draws_h$intercept <- NULL
    setnames(draws_h, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
    draws_h$other_list <- NULL
    draws_h$shortness_of_breath <- NULL
    draws_h$memory_problems <- NULL
    draws_h$fatigue <- NULL
    draws_h$administrative <- NULL
    draws_h$cough <- NULL
    draws_h <- melt(data = draws_h, id.vars = c("hospital", "icu", "follow_up_days", "female", "male"))
    setnames(draws_h, "variable", "draw")
    setnames(draws_h, "value", "proportion")
    draws_h$proportion <- exp(draws_h$proportion) / (1 + exp(draws_h$proportion))
    draws_h <- draws_h[proportion>1 | is.na(proportion), proportion := 1]
        
    draws_h <- reshape(draws_h, idvar = c("hospital", "icu", "follow_up_days", "female", "male"), timevar = "draw", direction = "wide")
    setnames(draws_h, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
    draws_save <- draws_h[hospital==1 | icu==1]
    
    write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws_hospital_icu.csv"))
    
    
    
    draws_c <- model_c$create_draws(
      data = predict_data_c,
      beta_samples = samples_c[[1]],
      gamma_samples = samples_c[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
    # write draws for pipeline
    draws_c <- data.table(draws_c)
    draws_c <- cbind(draws_c, predict_matrix_c)
    draws_c$intercept <- NULL
    setnames(draws_c, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
    draws_c$other_list <- NULL
    draws_c$shortness_of_breath <- NULL
    draws_c$memory_problems <- NULL
    draws_c$fatigue <- NULL
    draws_c$administrative <- NULL
    draws_c$cough <- NULL
    draws_c <- melt(data = draws_c, id.vars = c("hospital", "icu", "follow_up_days", "female", "male"))
    setnames(draws_c, "variable", "draw")
    setnames(draws_c, "value", "proportion")
    draws_c$proportion <- exp(draws_c$proportion) / (1 + exp(draws_c$proportion))
    draws_c <- draws_c[proportion>1 | is.na(proportion), proportion := 1]
    
    draws_c <- reshape(draws_c, idvar = c("hospital", "icu", "follow_up_days", "female", "male"), timevar = "draw", direction = "wide")
    setnames(draws_c, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
    draws_save <- draws_c[hospital==0 & icu==0]
    
    write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws_community.csv"))
    
    
    
    
    predict_matrix_h$pred_raw <- model_h$predict(predict_data_h, sort_by_data_id = TRUE)
    predict_matrix_h$pred <- exp(predict_matrix_h$pred_raw) / (1 + exp(predict_matrix_h$pred_raw))
    predict_matrix_h$pred_lo <- apply(draws_h[,2:ncol(draws_h)], 1, function(x) quantile(x, 0.025))
    predict_matrix_h$pred_hi <- apply(draws_h[,2:ncol(draws_h)], 1, function(x) quantile(x, 0.975))
    used_data_h <- cbind(model_h$data$to_df(), data.frame(w = model_h$w_soln))
    used_data_h <- as.data.table(used_data_h)
    rr_summaries_h <- copy(predict_matrix_h)
    
    rr_summaries_h
    rr_summaries_h$pred_hi[rr_summaries_h$pred_hi>1] <- 1
    rr_summaries_h$gamma <- mean(samples_h[[2]])
    write.csv(rr_summaries_h, file =paste0(outputfolder, model_dir, "predictions_summary_hospital_icu.csv"))
    
    
    
    
    
    
    predict_matrix_c$pred_raw <- model_c$predict(predict_data_c, sort_by_data_id = TRUE)
    predict_matrix_c$pred <- exp(predict_matrix_c$pred_raw) / (1 + exp(predict_matrix_c$pred_raw))
    predict_matrix_c$pred_lo <- apply(draws_c[,2:ncol(draws_c)], 1, function(x) quantile(x, 0.025))
    predict_matrix_c$pred_hi <- apply(draws_c[,2:ncol(draws_c)], 1, function(x) quantile(x, 0.975))
    used_data_c <- cbind(model_c$data$to_df(), data.frame(w = model_c$w_soln))
    used_data_c <- as.data.table(used_data_c)
    rr_summaries_c <- copy(predict_matrix_c)
    
    rr_summaries_c
    rr_summaries_c$pred_hi[rr_summaries_c$pred_hi>1] <- 1
    rr_summaries_c$gamma <- mean(samples_c[[2]])
    write.csv(rr_summaries_c, file =paste0(outputfolder, model_dir, "predictions_summary_community.csv"))
    
    
    
    
    
    
    
    
    used_data_h$pop[used_data_h$icu==0] <- "hospital"
    used_data_h$pop[used_data_h$icu==1] <- "ICU"
    used_data_c$pop <- "community"

    df_se <- df[,c("study_id", "re", "hospital", "icu", "female", "male", "follow_up_days", "mean", "standard_error", "cough", "shortness_of_breath", "fatigue", "memory_problems")]
    setnames(df_se, "standard_error", "se")
    setnames(used_data_h, "study_id", "re")
    setnames(used_data_c, "study_id", "re")
    used_data_h$hospital <- 0
    used_data_h <- used_data_h[icu==0, hospital := 1]
    used_data_c <- used_data_c[, hospital := 0]
    used_data_c <- used_data_c[, icu := 0]
    if (out=="rsp") {
      used_data_h <- merge(used_data_h, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days", "cough", "shortness_of_breath"))
      used_data_c <- merge(used_data_c, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days", "cough", "shortness_of_breath"))
      used_data_h$other_list <- 0
      used_data_c$other_list <- 0
    } else if (out=="cog") {
      used_data_h <- merge(used_data_h, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days", "memory_problems"))
      used_data_c <- merge(used_data_c, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days", "memory_problems"))
      used_data_h$other_list <- 0
      used_data_c$other_list <- 0
    } else if (out=="fat") {
      used_data_h <- merge(used_data_h, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days", "fatigue"))
      used_data_c <- merge(used_data_c, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days", "fatigue"))
      used_data_h$other_list <- 0
      used_data_c$other_list <- 0
    } else if (out=="any") {
      used_data_h <- merge(used_data_h, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days"))
      used_data_c <- merge(used_data_c, df_se, by=c("re", "icu", "hospital", "female", "male", "follow_up_days"))
    }
    
#    used_data_h$study_id <- paste(used_data_h$study_id, used_data_h$pop)
#    used_data_c$study_id <- paste(used_data_c$study_id, used_data_c$pop)
    
    
    ### ADJUST DATA
    coeffs_h
    coeffs_c
    used_data_i_f <- used_data_h[female==1 | (female==0 & male==0)]
    used_data_i_m <- used_data_h[male==1 | (female==0 & male==0)]
    used_data_h_f <- used_data_h[female==1 | (female==0 & male==0)]
    used_data_h_m <- used_data_h[male==1 | (female==0 & male==0)]
    used_data_c_f <- used_data_c[female==1 | (female==0 & male==0)]
    used_data_c_m <- used_data_c[male==1 | (female==0 & male==0)]
    used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3])
    used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4])
    used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3])
    used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4])
    used_data_c_f$obs_adj <- used_data_c_f$obs - used_data_c_f$other_list * as.numeric(coeffs_c[2,5]) + (1-used_data_c_f$female) * as.numeric(coeffs_c[2,2])
    used_data_c_m$obs_adj <- used_data_c_m$obs - used_data_c_m$other_list * as.numeric(coeffs_c[2,5]) + (1-used_data_c_m$male) * as.numeric(coeffs_c[2,3])
    
    

    used_data_i_f$obs_exp <- exp(used_data_i_f$obs) / (1 + exp(used_data_i_f$obs))
    used_data_i_f$obs_exp_adj <- exp(used_data_i_f$obs_adj) / (1 + exp(used_data_i_f$obs_adj))
    used_data_i_m$obs_exp <- exp(used_data_i_m$obs) / (1 + exp(used_data_i_m$obs))
    used_data_i_m$obs_exp_adj <- exp(used_data_i_m$obs_adj) / (1 + exp(used_data_i_m$obs_adj))
    used_data_h_f$obs_exp <- exp(used_data_h_f$obs) / (1 + exp(used_data_h_f$obs))
    used_data_h_f$obs_exp_adj <- exp(used_data_h_f$obs_adj) / (1 + exp(used_data_h_f$obs_adj))
    used_data_h_m$obs_exp <- exp(used_data_h_m$obs) / (1 + exp(used_data_h_m$obs))
    used_data_h_m$obs_exp_adj <- exp(used_data_h_m$obs_adj) / (1 + exp(used_data_h_m$obs_adj))
    used_data_c_f$obs_exp <- exp(used_data_c_f$obs) / (1 + exp(used_data_c_f$obs))
    used_data_c_f$obs_exp_adj <- exp(used_data_c_f$obs_adj) / (1 + exp(used_data_c_f$obs_adj))
    used_data_c_m$obs_exp <- exp(used_data_c_m$obs) / (1 + exp(used_data_c_m$obs))
    used_data_c_m$obs_exp_adj <- exp(used_data_c_m$obs_adj) / (1 + exp(used_data_c_m$obs_adj))
    rr_summaries <- rbind(rr_summaries_h, rr_summaries_c)
    rr_summaries <- rr_summaries[follow_up_days<=365]
    rr_summariesF <- rr_summaries[female==1]
    rr_summariesM <- rr_summaries[male==1]
    used_data <- rbind(used_data_h, used_data_c, fill = TRUE)
    used_data$weight <- 1/(30*used_data$se)
    #  used_data$weight <- 1/(used_data$obs_se)
    used_data_i_f$weight <- 1/(20*used_data_i_f$se)
    used_data_i_f$weight[used_data_i_f$weight>10] <- 10
    used_data_i_f$weight[used_data_i_f$weight<2] <- 2
    used_data_i_m$weight <- 1/(20*used_data_i_m$se)
    used_data_i_m$weight[used_data_i_m$weight>10] <- 10
    used_data_i_m$weight[used_data_i_m$weight<2] <- 2
    used_data_h_f$weight <- 1/(20*used_data_h_f$se)
    used_data_h_f$weight[used_data_h_f$weight>10] <- 10
    used_data_h_f$weight[used_data_h_f$weight<2] <- 2
    used_data_h_m$weight <- 1/(20*used_data_h_m$se)
    used_data_h_m$weight[used_data_h_m$weight>10] <- 10
    used_data_h_m$weight[used_data_h_m$weight<2] <- 2
    used_data_c_f$weight <- 1/(20*used_data_c_f$se)
    used_data_c_f$weight[used_data_c_f$weight>10] <- 10
    used_data_c_f$weight[used_data_c_f$weight<2] <- 2
    used_data_c_m$weight <- 1/(20*used_data_c_m$se)
    used_data_c_m$weight[used_data_c_m$weight>10] <- 10
    used_data_c_m$weight[used_data_c_m$weight<2] <- 2
    
    used_data <- rbind(used_data_h_f, used_data_h_m, used_data_c_f, used_data_c_m, fill=TRUE)
    used_data <- unique(used_data)
    table(used_data$study_id)
    
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred, fill = "blue"))+
      geom_ribbon(data= rr_summariesF[icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
      geom_line(data=rr_summariesF[icu==1], aes(x=follow_up_days, y=pred, color = "red"), size=1) +
      geom_ribbon(data= rr_summariesF[hospital==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgreen", alpha=.3) +
      geom_line(data=rr_summariesF[hospital==1], aes(x=follow_up_days, y=pred, color = "green"), size=1) +
      geom_ribbon(data= rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, y=pred, color = "blue"), size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, ", females")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      theme(legend.text=element_text(size=rel(.7))) +
      guides(colour = guide_legend(reverse = TRUE)) +
      scale_colour_manual(values =c('blue'='blue', 'green'='green', 'red'='red'), labels = c('Community', 'Hospitalized', 'ICU'), order(c('ICU', 'Hospitalized', 'Community')))

      
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "F.pdf"), width = 8, height = 5)
  
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred, fill = "blue"))+
      geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, y=pred, color = "blue"), size=1) +
      geom_ribbon(data= rr_summariesM[hospital==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgreen", alpha=.3) +
      geom_line(data=rr_summariesM[hospital==1], aes(x=follow_up_days, y=pred, color = "green"), size=1) +
      geom_ribbon(data= rr_summariesM[icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
      geom_line(data=rr_summariesM[icu==1], aes(x=follow_up_days, y=pred, color = "red"), size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, ", males")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      theme(legend.text=element_text(size=rel(.7))) +
      guides(colour = guide_legend(reverse = TRUE)) +
      scale_colour_manual(values =c('blue'='blue', 'green'='green', 'red'='red'), labels = c('Community', 'Hospitalized', 'ICU'), order(c('ICU', 'Hospitalized', 'Community')))
    plot
    #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "M.pdf"), width = 8, height = 5)
    
  
    
    
    ##############################
    # Community
    rr_summariesF$pred_hi[rr_summariesF$pred_hi>0.8 & rr_summariesF$hospital==0 & rr_summariesF$icu==0] <- 0.8
    rr_summariesM$pred_hi[rr_summariesM$pred_hi>0.8 & rr_summariesF$hospital==0 & rr_summariesF$icu==0] <- 0.8
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among community, females")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.8,0.1), limits=c(0,0.8)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & male!=1 & pop=="community"], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & male!=1 & pop=="community", study_id]), 
                 size=(used_data[w==1 & male!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & pop=="community", study_id]), 
                 size=(used_data[w==0 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "F_comm.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among community, males")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.8,0.1), limits=c(0,0.8)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & female!=1 & pop=="community"], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & female!=1 & pop=="community", study_id]), 
                 size=(used_data[w==1 & female!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & pop=="community", study_id]), 
                 size=(used_data[w==0 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "M_comm.pdf"), width = 8, height = 5)
    
    # ADJUSTED
    
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among community, females, adjusted values")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.8,0.1), limits=c(0,0.8)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_c_f[w==1 & male!=1,], aes(x=follow_up_days, y=obs_exp_adj, color=used_data_c_f[w ==1 & male!=1, study_id]), 
                 size=(used_data_c_f[w==1 & male!=1, weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & pop=="community", study_id]), 
                 size=(used_data[w==0 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "F_comm_adj.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among community, males, adjusted values")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.8,0.1), limits=c(0,0.8)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_c_m[w==1 & female!=1,], aes(x=follow_up_days, y=obs_exp_adj, color=used_data_c_m[w ==1 & female!=1, study_id]), 
                 size=(used_data_c_m[w==1 & female!=1, weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & pop=="community", study_id]), 
                 size=(used_data[w==0 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "M_comm_adj.pdf"), width = 8, height = 5)
    
    
    #####################################
    # Hospital
    
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among hospitalized, females")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & male!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, 
                                                                         color=used_data[w ==1 & male!=1 & pop!="community", study_id], 
                                                                         shape=used_data[w ==1 & male!=1 & pop!="community", pop]), 
                 size=(used_data[w==1 & male!=1 & pop!="community", weight]), alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                color=used_data[w ==0 & pop=="hospital", study_id],
                                                                shape=used_data[w ==0 & pop=="hospital", pop]), 
                 size=(used_data[w==0 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                color=used_data[w ==0 & pop=="ICU", study_id],
                                                                shape=used_data[w ==0 & pop=="ICU", pop]), 
                 size=(used_data[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "F_hosp.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among hospitalized, males")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & female!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, 
                                                                           color=used_data[w ==1 & female!=1 & pop!="community", study_id], 
                                                                           shape=used_data[w ==1 & female!=1 & pop!="community", pop]), 
                 size=(used_data[w==1 & female!=1 & pop!="community", weight]), alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                               color=used_data[w ==0 & pop=="hospital", study_id],
                                                               shape=used_data[w ==0 & pop=="hospital", pop]), 
                 size=(used_data[w==0 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                          color=used_data[w ==0 & pop=="ICU", study_id],
                                                          shape=used_data[w ==0 & pop=="ICU", pop]), 
                 size=(used_data[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "M_hosp.pdf"), width = 8, height = 5)
    
    # ADJUSTED
    
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among hospitalized, females, adjusted values")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_h_f[w==1 & male!=1,], aes(x=follow_up_days, y=obs_exp_adj,
                                                          color=used_data_h_f[w ==1 & male!=1, study_id], 
                                                          shape=used_data_h_f[w ==1 & male!=1, pop]), 
                 size=(used_data_h_f[w==1 & male!=1, weight]), alpha=0.5) +
      geom_point(data=used_data_h_f[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                               color=used_data_h_f[w ==0 & pop=="hospital", study_id],
                                                               shape=used_data_h_f[w ==0 & pop=="hospital", pop]), 
                 size=(used_data_h_f[w==0 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data_h_f[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                          color=used_data_h_f[w ==0 & pop=="ICU", study_id],
                                                          shape=used_data_h_f[w ==0 & pop=="ICU", pop]), 
                 size=(used_data_h_f[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "F_hosp_adj.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among hospitalized, males, adjusted values")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_h_m[w==1 & female!=1,], aes(x=follow_up_days, y=obs_exp_adj, 
                                                            color=used_data_h_m[w ==1 & female!=1, study_id], 
                                                            shape=used_data_h_m[w ==1 & female!=1, pop]), 
                 size=(used_data_h_m[w==1 & female!=1, weight]), alpha=0.5) +
      geom_point(data=used_data_h_m[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                   color=used_data_h_m[w ==0 & pop=="hospital", study_id],
                                                                   shape=used_data_h_m[w ==0 & pop=="hospital", pop]), 
                 size=(used_data_h_m[w==0 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data_h_m[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                              color=used_data_h_m[w ==0 & pop=="ICU", study_id],
                                                              shape=used_data_h_m[w ==0 & pop=="ICU", pop]), 
                 size=(used_data_h_m[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "M_hosp_adj.pdf"), width = 8, height = 5)
    
    
    
    ################################
    # ICU
    
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among ICU, females")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & male!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & male!=1 & pop!="community", study_id]), 
                 size=(used_data[w==1 & male!=1 & pop!="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                   color=used_data[w ==0 & pop=="hospital", study_id],
                                                                   shape=used_data[w ==0 & pop=="hospital", pop]), 
                 size=(used_data[w==0 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                              color=used_data[w ==0 & pop=="ICU", study_id],
                                                              shape=used_data[w ==0 & pop=="ICU", pop]), 
                 size=(used_data[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "F_icu.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among ICU, males")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & female!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & female!=1 & pop!="community", study_id]), 
                 size=(used_data[w==1 & female!=1 & pop!="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                               color=used_data[w ==0 & pop=="hospital", study_id],
                                                               shape=used_data[w ==0 & pop=="hospital", pop]), 
                 size=(used_data[w==0 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                          color=used_data[w ==0 & pop=="ICU", study_id],
                                                          shape=used_data[w ==0 & pop=="ICU", pop]), 
                 size=(used_data[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "M_icu.pdf"), width = 8, height = 5)
    
    # ADJUSTED
    
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among ICU, females, adjusted values")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_i_f[w==1 & male!=1,], aes(x=follow_up_days, y=obs_exp_adj, 
                                                          color=used_data_i_f[w ==1 & male!=1, study_id], 
                                                          shape=used_data_i_f[w ==1 & male!=1, pop]),
                   size=(used_data_i_f[w==1 & male!=1, weight]), alpha=0.5) +
      geom_point(data=used_data_i_f[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                               color=used_data_i_f[w ==0 & pop=="hospital", study_id],
                                                               shape=used_data_i_f[w ==0 & pop=="hospital", pop]), 
                 size=(used_data_i_f[w==0 & pop=="hospital", weight]), shape = 1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data_i_f[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                          color=used_data_i_f[w ==0 & pop=="ICU", study_id],
                                                          shape=used_data_i_f[w ==0 & pop=="ICU", pop]), 
                 size=(used_data_i_f[w==0 & pop=="ICU", weight]), shape = 2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "F_icu_adj.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title, "among ICU, males, adjusted values")) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_i_m[w==1 & female!=1,], aes(x=follow_up_days, y=obs_exp_adj, 
                                                            color=used_data_i_m[w ==1 & female!=1, study_id],
                                                            shape=used_data_i_m[w ==1 & female!=1, pop]),
                 size=(used_data_i_m[w==1 & female!=1, weight]), alpha=0.5) +
      geom_point(data=used_data_i_m[w ==0 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                   color=used_data_i_m[w ==0 & pop=="hospital", study_id],
                                                                   shape=used_data_i_m[w ==0 & pop=="hospital", pop]), 
                 size=(used_data_i_m[w==0 & pop=="hospital", weight]), shape = 1, alpha=1, show.legend = FALSE) +
      geom_point(data=used_data_i_m[w ==0 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                              color=used_data_i_m[w ==0 & pop=="ICU", study_id],
                                                              shape=used_data_i_m[w ==0 & pop=="ICU", pop]), 
                 size=(used_data_i_m[w==0 & pop=="ICU", weight]), shape = 2, alpha=1, show.legend = FALSE) +
      guides(fill=FALSE, shape=FALSE) + scale_colour_discrete("Study") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "M_icu_adj.pdf"), width = 8, height = 5)
    
    
  
}
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  ######################################################################################
  #   run model with all data of overlaps among any long COVID
  ######################################################################################
  
  for(out in c("mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp")) {
    #out <- "cog_rsp"
    title <- out
    if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
      xmax <- 1
      envelope <- "among all long-COVID patients"
      filename <- "amongLongCOVID"
      cvs <- list('hospital_or_icu', 'administrative')
    } else if (out=="mild_rsp" | out=="mod_rsp" | out=="sev_rsp") {
      xmax <- 1
      envelope <- "among all respiratory long-COVID patients"
      filename <- "amongRsp"
      cvs <- list('hospital_or_icu')
    } else if (out=="mild_cog" | out=="mod_cog") {
      xmax <- 1
      envelope <- "among all cognitive long-COVID patients"
      filename <- "amongCog"
      cvs <- list('hospital_or_icu')
    }
    model_dir <- paste0(out, "_v", version, "/")
    dir.create(paste0(outputfolder, model_dir))
    message(paste0("working on ", out))
    df <- dataset[outcome==out]
  
    #    df <- df[sample_size_envelope>10 | study_id=="PRA"]
    df <- df[sample_size_envelope>10 & study_id!="PRA"]
	
    #    df <- df[study_id!="HAARVI"]
    df <- df[study_id=="PRA", mean := mean_adjusted]
    
    # calculate standard errors using the cases of any long covid as the denominator
    df$log_mean <- log(df$mean)
    ### basically a loop that goes through each row and calcs the se in log space
    df$delta_log_se <- sapply(1:nrow(df), function(i) {
      ratio_i <- df[i, "mean"] # relative_risk column
      ratio_se_i <- df[i, "standard_error"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })
    df$log_se <- df$delta_log_se
    df <- df[!is.na(standard_error) & !is.na(log_se)]
    
    
    df <- df[age_specific==0]
    table(df$study_id, df$female)
    df <- df[sex=="Both"]
    
    
    # set up data
    mr_df <- MRData()
    
    mr_df$load_df(
      data = df, col_obs = "logitmean", col_obs_se = "logitse",
      col_covs = cvs, col_study_id = "study_id")
    
    if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
      model <- MRBRT(
        data = mr_df,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("hospital_or_icu", use_re = FALSE)
          #        LinearCovModel("administrative", use_re = FALSE)
        ),
        inlier_pct = 1)
    } else {
      model <- MRBRT(
        data = mr_df,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("hospital_or_icu", use_re = FALSE)
        ),
        inlier_pct = 1)
    }
    
    # fit model
    model$fit_model(inner_print_level = 5L, inner_max_iter = n_samples)
    
    (coeffs <- rbind(model$cov_names, model$beta_soln))
    write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs.csv"))
    
    # save model object
    py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1.pkl"), pickle = "dill")
    #py_save_object(object = model_no, filename = paste0(folder, "mrbrt_model_outputs/", model_dir_no, "mod1.pkl"), pickle = "dill")
    
    # make predictions for full year
    predict_matrix <- data.table(intercept = model$beta_soln[1], hospital_or_icu=c(0,1), administrative=0)
    
    predict_data <- MRData()
    predict_data$load_df(
      data = predict_matrix,
      col_covs=cvs)
    
    #n_samples <- 1000L
    samples <- model$sample_soln(sample_size = n_samples)
    
    draws <- model$create_draws(
      data = predict_data,
      beta_samples = samples[[1]],
      gamma_samples = samples[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
    # write draws for pipeline
    draws <- data.table(draws)
    draws <- cbind(draws, predict_matrix)
    draws$intercept <- NULL
    setnames(draws, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
    
    draws$administrative <- NULL
    draws <- melt(data = draws, id.vars = c("hospital_or_icu"))
    setnames(draws, "variable", "draw")
    setnames(draws, "value", "proportion")
    draws$proportion <- exp(draws$proportion) / (1 + exp(draws$proportion))
    
    draws <- reshape(draws, idvar = c("hospital_or_icu"), timevar = "draw", direction = "wide")
    setnames(draws, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
    draws_save <- draws
    setnames(draws_save, 'hospital_or_icu', 'hospital_icu')
    
    
    write.csv(draws, file =paste0(outputfolder, model_dir, "predictions_draws.csv"))
    
    
    
    
    
    predict_matrix$pred_raw <- model$predict(predict_data, sort_by_data_id = TRUE)
    predict_matrix$pred <- exp(predict_matrix$pred_raw) / (1 + exp(predict_matrix$pred_raw))
    predict_matrix$pred_lo <- apply(draws[,2:ncol(draws)], 1, function(x) quantile(x, 0.025))
    predict_matrix$pred_hi <- apply(draws[,2:ncol(draws)], 1, function(x) quantile(x, 0.975))
    used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
    used_data <- as.data.table(used_data)
    rr_summaries <- copy(predict_matrix)
    
    rr_summaries
    rr_summaries$pred_hi[rr_summaries$pred_hi>1] <- 1
    rr_summaries$gamma <- mean(samples[[2]])
    write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary.csv"))
    
    
    
    
    #    rr_summaries$xval <- 0
    #    used_data$xval[used_data$study_id=="CO-FLOW hosp/ICU"] <- 1
    #    used_data$xval[used_data$study_id=="Iran hosp/ICU"] <- 2
    #    used_data$xval[used_data$study_id=="Sechenov StopCOVID hosp/ICU"] <- 3
    #    used_data$xval[used_data$study_id=="Sweden PronMed ICU hosp/ICU"] <- 4
    
    used_data$pop[used_data$hospital_or_icu==1] <- "hosp/ICU"
    used_data$pop[used_data$hospital_or_icu==0] <- "community"
    #used_data$study_id <- paste(used_data$study_id, used_data$pop)
    used_data$weight <- 0.3*(1/used_data$se)
    
    used_data$obs_exp <- exp(used_data$obs) / (1 + exp(used_data$obs))
    used_data$se <- df$standard_error
    
    
    if (length(unique(used_data$hospital_or_icu))==1) {
      rr_summaries <- rr_summaries[hospital_or_icu==1]
    }
    
    rr_summaries$study_id[rr_summaries$hospital_or_icu==1] <- " ESTIMATE hosp/ICU"
    rr_summaries$pop[rr_summaries$hospital_or_icu==1] <- "hosp/ICU"
    rr_summaries$study_id[rr_summaries$hospital_or_icu==0] <- " ESTIMATE community"
    rr_summaries$pop[rr_summaries$hospital_or_icu==0] <- "community"
    used_data$obs_lo <- used_data$obs-2*used_data$obs_se
    used_data$obs_lo <- exp(used_data$obs_lo) / (1 + exp(used_data$obs_lo))
    used_data$obs_hi <- used_data$obs+2*used_data$obs_se
    used_data$obs_hi <- exp(used_data$obs_hi) / (1 + exp(used_data$obs_hi))
    used_data$obs_hi[used_data$obs_hi>xmax] <- xmax
    
    plot <- ggplot(data=rr_summaries, aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi)) +
      geom_pointrange(data=rr_summaries[pop=="community"], aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi), shape = 16, size = .8, color="blue") + 
      geom_pointrange(data=rr_summaries[pop=="hosp/ICU"], aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi), shape = 17, size = .8, color="blue") + 
      geom_pointrange(data=used_data[w==1 & pop=="community"], aes(x=study_id, y=obs_exp, ymin=obs_lo, ymax=obs_hi), shape = 16, size = .8) + 
      geom_pointrange(data=used_data[w==1 & pop=="hosp/ICU"], aes(x=study_id, y=obs_exp, ymin=obs_lo, ymax=obs_hi), shape = 17, size = .8) + 
      geom_pointrange(data=used_data[w==0 & pop=="community"], aes(x=study_id, y=obs_exp, ymin=obs_lo, ymax=obs_hi), shape=1, size = .8) + 
      geom_pointrange(data=used_data[w==0 & pop=="hosp/ICU"], aes(x=study_id, y=obs_exp, ymin=obs_lo, ymax=obs_hi), shape=2, size = .8) + 
      coord_flip() +  # flip coordinates (puts labels on y axis)
      ylab("Proportion") +
      xlab("Study") +
      ggtitle(paste(title, envelope)) +
      theme_minimal() +
      theme(axis.line=element_line(colour="black")) +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,xmax,0.1), limits=c(0,xmax)) + 
      guides(fill=FALSE)
    
    plot
    
    #geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "_", filename, ".pdf"), width = 6, height = 5)
    
    
    
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  ######################################################################################
  #   run model with all data of overlaps among any long COVID by follow-up time
  ######################################################################################
  
  for(out in c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp")) {
    #out <- "cog_rsp"
    title <- out
    if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
      xmax <- 1
      envelope <- "among all long-COVID patients"
      filename <- "amongLongCOVID"
      cvs <- list('hospital_or_icu', 'follow_up_days')
    } else if (out=="mild_rsp" | out=="mod_rsp" | out=="sev_rsp") {
      xmax <- 1
      envelope <- "among all respiratory long-COVID patients"
      filename <- "amongRsp"
      cvs <- list('hospital_or_icu')
    } else if (out=="mild_cog" | out=="mod_cog") {
      xmax <- 1
      envelope <- "among all cognitive long-COVID patients"
      filename <- "amongCog"
      cvs <- list('hospital_or_icu')
    }
    model_dir <- paste0(out, "_v", version, "/")
    dir.create(paste0(outputfolder, model_dir))
    message(paste0("working on ", out))
    df <- dataset[outcome==out]
  
    #    df <- df[sample_size_envelope>10 | study_id=="PRA"]
    df <- df[sample_size_envelope>10 & study_id!="PRA"]

    #    df <- df[study_id!="HAARVI"]
   
    df <- df[study_id=="PRA", mean := mean_adjusted]
    
    # calculate standard errors using the cases of any long covid as the denominator
    df$log_mean <- log(df$mean)
    ### basically a loop that goes through each row and calcs the se in log space
    df$delta_log_se <- sapply(1:nrow(df), function(i) {
      ratio_i <- df[i, "mean"] # relative_risk column
      ratio_se_i <- df[i, "standard_error"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })
    df$log_se <- df$delta_log_se
    df <- df[!is.na(standard_error) & !is.na(log_se)]
    
    
    df <- df[age_specific==0]
    table(df$study_id, df$female)
    df <- df[sex=="Both"]
    
    
    # set up data
    mr_df <- MRData()
    
    mr_df$load_df(
      data = df, col_obs = "logitmean", col_obs_se = "logitse",
      col_covs = cvs, col_study_id = "study_id")
    
    if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
      model <- MRBRT(
        data = mr_df,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("hospital_or_icu", use_re = FALSE),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd)))
        ),
        inlier_pct = 1)
      model <- MRBRT(
        data = mr_df,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("hospital_or_icu", use_re = FALSE),
          LinearCovModel("follow_up_days", use_re = FALSE)
        ),
        inlier_pct = 1)
      
    } else {
      model <- MRBRT(
        data = mr_df,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("hospital_or_icu", use_re = FALSE)
        ),
        inlier_pct = 1)
    }
    
    # fit model
    model$fit_model(inner_print_level = 5L, inner_max_iter = n_samples)
    
    (coeffs <- rbind(model$cov_names, model$beta_soln))
    write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs.csv"))
    
    # save model object
    py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1.pkl"), pickle = "dill")
    #py_save_object(object = model_no, filename = paste0(folder, "mrbrt_model_outputs/", model_dir_no, "mod1.pkl"), pickle = "dill")
    
    # make predictions for full year
    predict_matrix_c <- data.table(intercept = model$beta_soln[1], follow_up_days=c(0:1460), hospital_or_icu=0)
    predict_matrix_h <- data.table(intercept = model$beta_soln[1], follow_up_days=c(0:1460), hospital_or_icu=1)
    predict_matrix <- rbind(predict_matrix_c, predict_matrix_h)
    
    
    predict_data <- MRData()
    predict_data$load_df(
      data = predict_matrix,
      col_covs=cvs)

    
    ## CREATE DRAWS  
    
    samples <- model$sample_soln(sample_size = n_samples)
    
    draws <- model$create_draws(
      data = predict_data,
      beta_samples = samples[[1]],
      gamma_samples = samples[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
    
    # write draws for pipeline
    draws <- data.table(draws)
    draws <- cbind(draws, predict_matrix)
    draws$intercept <- NULL
    setnames(draws, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
    
    draws <- melt(data = draws, id.vars = c("hospital_or_icu", "follow_up_days"))
    setnames(draws, "variable", "draw")
    setnames(draws, "value", "proportion")
    draws$proportion <- exp(draws$proportion) / (1 + exp(draws$proportion))
    
    draws <- reshape(draws, idvar = c("hospital_or_icu", "follow_up_days"), timevar = "draw", direction = "wide")
    setnames(draws, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
    
    write.csv(draws, file =paste0(outputfolder, model_dir, "predictions_draws_full.csv"))
    
    draws_save <- draws
    setnames(draws_save, 'hospital_or_icu', 'hospital_icu')
    draws_save <- draws_save[follow_up_days==0]
    draws_save$follow_up_days <- NULL
    write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws.csv"))

    
    predict_matrix$pred_raw <- model$predict(predict_data, sort_by_data_id = TRUE)
    predict_matrix$pred <- exp(predict_matrix$pred_raw) / (1 + exp(predict_matrix$pred_raw))
    predict_matrix$pred_lo <- apply(draws[,2:ncol(draws)], 1, function(x) quantile(x, 0.025))
    predict_matrix$pred_hi <- apply(draws[,2:ncol(draws)], 1, function(x) quantile(x, 0.975))
    used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
    used_data <- as.data.table(used_data)
    rr_summaries <- copy(predict_matrix)
    
    rr_summaries
    rr_summaries$pred_hi[rr_summaries$pred_hi>1] <- 1
    rr_summaries$gamma <- mean(samples[[2]])
    write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary.csv"))
    
    
    
    
    used_data$pop[used_data$hospital_or_icu==1] <- "hosp/ICU"
    used_data$pop[used_data$hospital_or_icu==0] <- "community"
    used_data$re <- used_data$study_id
    used_data$study_id <- paste(used_data$study_id, used_data$pop)
    
    used_data$obs_exp <- exp(used_data$obs) / (1 + exp(used_data$obs))

    if (length(unique(used_data$hospital_or_icu))==1) {
      rr_summaries <- rr_summaries[hospital_or_icu==1]
    }
    
    rr_summaries$study_id[rr_summaries$hospital_or_icu==1] <- " ESTIMATE hosp/ICU"
    rr_summaries$pop[rr_summaries$hospital_or_icu==1] <- "hosp/ICU"
    rr_summaries$study_id[rr_summaries$hospital_or_icu==0] <- " ESTIMATE community"
    rr_summaries$pop[rr_summaries$hospital_or_icu==0] <- "community"
    rr_summaries <- rr_summaries[follow_up_days<=365]
    used_data$obs_lo <- used_data$obs-2*used_data$obs_se
    used_data$obs_lo <- exp(used_data$obs_lo) / (1 + exp(used_data$obs_lo))
    used_data$obs_hi <- used_data$obs+2*used_data$obs_se
    used_data$obs_hi <- exp(used_data$obs_hi) / (1 + exp(used_data$obs_hi))
    used_data$obs_hi[used_data$obs_hi>xmax] <- xmax

    
    
    df_se <- df[,c("study_id", "re", "hospital_or_icu", "follow_up_days", "mean", "standard_error")]
    setnames(df_se, "standard_error", "se")
    df_se$study_id <- NULL
#    setnames(used_data, "study_id", "re")
    used_data <- merge(used_data, df_se, by=c("re", "hospital_or_icu", "follow_up_days"))
    used_data$weight <- 0.3*(1/used_data$se)
    used_data$weight[used_data$weight>10] <- 10
    
    used_data$shape <- 16
    used_data$shape[used_data$pop=="hosp/ICU"] <- 17
    
    plot <- ggplot(data=rr_summaries, aes(x=follow_up_days, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summaries[hospital_or_icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
      geom_line(data=rr_summaries[hospital_or_icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
      geom_ribbon(data= rr_summaries[hospital_or_icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.5) +
      geom_line(data=rr_summaries[hospital_or_icu==1], aes(x=follow_up_days, y=pred), color = "red", size=1) +
      ylab("Proportion") +
      xlab("Follow up (days)") +
      ggtitle(paste(title)) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
      scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data, aes(x=follow_up_days, y=obs_exp, color=used_data[, study_id], shape=(used_data[, pop])), 
                 size=(used_data[, weight]), alpha=0.5) +
      guides(fill=FALSE) + scale_colour_discrete("Study") + scale_shape_discrete("Population") +
      theme(legend.text=element_text(size=rel(0.7)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_", out, "_", filename, ".pdf"), width = 6, height = 5)
    
    
    
  }
  
