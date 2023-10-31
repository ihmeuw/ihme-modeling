#--------------------------------------------------------------
# Name:
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
setwd("FILEPATH")



# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
library(reticulate) 
#library(mrbrt001, lib.loc = 'FILEPATH')
#source("FILEPATH")
#remove_python_objects()
library(mrbrt002, lib.loc = "FILEPATH")
library(plyr)
library(msm)
#  library(msm, lib.loc = "FILEPATH")


folder <- "FILEPATH"
outputfolder <- "FILEPATH"


cvs <- list("hospital", "icu", "female", "male", "follow_up_days", "other_list")
#  datadate <- '051321'
#  datadate <- '081221'
#  datadate <- '091221'
datadate <- '012822'
n_samples <- 1000L
max_draw <- n_samples - 1
year_start <- 2020
year_end <- 2023
inlier <- 0.9
drop_zeroes <- 0
drop_cough <- 1
min_cv <- 0.1
# make kid estimates sex-specific?
kid_estimate_sex_specific_c <- 0
kid_estimate_sex_specific_h <- 0
# make separate estimates among kids for hosp/ICU models?  likely not given data sparsity
hosp_kids <- 0
# log-transform follow_up_days?  (if this option is 1, then outcome stays in linear space)
ln_follow_up_days <- 0
# add offset to follow-up days?  useful if log-transforming follow_up_days, set it to c(10,20,30,40,50).  
#     else, set it to 0 if just doing a normal log-linear or logit-linear model without transforming follow_up_days
offset_days <- 0
follow_up_sd_multiplier <- 1
version <- 79




################################################################
# READ DATA
################################################################

dataset <- read.csv(paste0(outputfolder, "prepped_data_", datadate, ".csv"))

dim(dataset)
length(unique(dataset$study_id))

dataset <- data.table(dataset)
dataset <- dataset[!(study_id=="PRA" & outcome=="any")]
dataset <- dataset[!is.na(standard_error)]
table(dataset$study_id, dataset$outcome)
table(dataset$symptom_cluster, dataset$zero)
if (drop_zeroes == 1) {
  dataset <- dataset[mean>0]
}
if (hosp_kids == 0) {
  # drop age-specific hosp data for Iran ICC and PRA
  dataset[(study_id %in% c("Iran ICC", "PRA") & (age_end < 20 | age_start >= 20) & hospital_or_icu == 1), is_outlier := 1]
  # drop all-age comm data for Iran ICC and PRA
  dataset[(study_id %in% c("Iran ICC", "PRA") & (age_start < 10 & age_end > 90) & hospital_or_icu == 0), is_outlier := 1]
} else {
  # drop all-age comm data for Iran ICC and PRA
  dataset[(study_id %in% c("Iran ICC", "PRA") & (age_start < 10 & age_end > 90) & hospital_or_icu == 0 & 
             outcome %in% c("any", "cog", "fat", "rsp")), is_outlier := 1]
  # drop all-age hosp data for Iran ICC and PRA
  dataset[(study_id %in% c("Iran ICC", "PRA") & (age_start < 10 & age_end > 90) & hospital_or_icu == 1 & 
             outcome %in% c("any", "cog", "fat", "rsp")), is_outlier := 1]

  # drop age-specific hosp data for Iran ICC and PRA overlap and severities
  dataset[(study_id %in% c("Iran ICC", "PRA") & (age_end < 20 | age_start >= 20) & hospital_or_icu == 1 & 
             outcome %in% c("cog_rsp", "fat_cog", "fat_cog_rsp", "fat_rsp", "mild_cog", "mild_rsp", "mod_cog", "mod_rsp", "sev_rsp")), is_outlier := 1]
  # drop all-age hosp data for Iran ICC and PRA overlap and severities
  dataset[(study_id %in% c("Iran ICC", "PRA") & (age_start < 10 & age_end > 90) & hospital_or_icu == 1 & 
             outcome %in% c("cog_rsp", "fat_cog", "fat_cog_rsp", "fat_rsp", "mild_cog", "mild_rsp", "mod_cog", "mod_rsp", "sev_rsp")), is_outlier := 1]
}
dataset <- dataset[is_outlier!=1]
dataset <- dataset[outcome!="gbs"]

dataset[study_id=="Zurich CC" & sample_characteristics=="prospective sample", study_id := "Zurich CC prosp"]
dataset[study_id=="Zurich CC" & sample_characteristics=="retrospective sample", study_id := "Zurich CC retro"]


######################################################################################
#   run model with all data with multiple follow up times, to get duration
#      and beta on follow_up_days to use as prior for cluster-specific models
######################################################################################

# add offset to symptom cluster data
(offset <- 0.01 * median(dataset$mean))
dataset[outcome %in% c('any', 'fat', 'rsp', 'cog'), mean := (mean + offset)]
min(dataset$mean)

if (min_cv > 0) {
  dataset$cv <- dataset$standard_error / dataset$mean
  dataset[cv < min_cv, standard_error := mean * min_cv]
  dataset$cv <- NULL
}

dataset$log_mean <- log(dataset$mean)
logitvals <- linear_to_logit(mean = array(dataset$mean), sd = array(dataset$standard_error))
logitmean <- logitvals[1]
logitse <- logitvals[2]
dataset$logitmean <- logitmean
dataset$logitse <- logitse

if (ln_follow_up_days == 1) {
  dataset[, logitmean := mean]
  dataset[, logitse := standard_error]
}

### basically a loop that goes through each row and calcs the se in log space
#  dataset$delta_log_se <- sapply(1:nrow(dataset), function(i) {
#    ratio_i <- dataset[i, "mean"] # relative_risk column
#    ratio_se_i <- dataset[i, "standard_error"]
#    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
#  })
#  dataset$log_se <- dataset$delta_log_se

df_ped <- copy(dataset[children==1])
df_adult <- copy(dataset[children!=1])



# function to change size of legend
addSmallLegend <- function(myPlot, pointSize = 0.5, titleSize = 0, textSize = 3, spaceLegend = 0.1) {
  myPlot +
    guides(fill=FALSE) + guides(shape = guide_legend(override.aes = list(size = pointSize)),
           color = guide_legend(override.aes = list(size = pointSize))) +
    theme(legend.title = element_text(size = titleSize), 
          legend.text  = element_text(size = textSize),
          legend.key.size = unit(spaceLegend, "lines"))
}





##########################################################
##  HOSPITAL CASES: DURATION
##########################################################
out <- "duration"
message(paste0("working on duration"))

# duration for hosp/ICU is same for all ages because we don't have data for this among children
df <- copy(df_adult)
df <- df[age_specific==0]

# if dropping zeroes, drop rsp and cog from zurich study because then left with only one time point for each
if (drop_zeroes == 1) {
  df <- df[!((study_id=="Zurich CC prosp" | study_id=="Zurich CC retro") & (outcome=="rsp" | outcome=="cog"))]
}
if (version >= 67) {
  df <- df[study_id=="Cirulli et al" | study_id=="CO-FLOW" | study_id=="CSS" | study_id=="Faroe" | (study_id=="UK CIS" & outcome=='any') | 
             study_id=="Zurich CC prosp" | study_id=="Zurich CC retro" | (study_id=="CSS peds" & outcome=='any') | (study_id=="García‑Abellán J et al" & (outcome=='any')) | 
             (study_id=="Sechenov StopCOVID" & outcome=='any') | (study_id=="Sweden PronMed") | study_id=="Becker et al"]
} else {
  df <- df[study_id=="Cirulli et al" | study_id=="CO-FLOW" | study_id=="CSS" | study_id=="Faroe" | (study_id=="UK CIS" & outcome=='any') | 
             study_id=="Zurich CC prosp" | study_id=="Zurich CC retro" | (study_id=="CSS peds" & outcome=='any') | (study_id=="García‑Abellán J et al" & (outcome=='any'))]
}

df <- df[female==0 & male==0 & !is.na(standard_error) & !is.na(logitse)]

df <- df[outcome=="any" | outcome=="fat" | outcome=="cog" | outcome=="rsp"]
table(df$study_id, df$outcome)
df <- df[outcome=="any"]

df$study_id <- paste(df$study_id, df$outcome)
table(df$study_id, df$outcome)
#df <- df[outcome=="any"]
# set up folder
model_dir <- paste0(out, "FILEPATH")
dir.create(paste0(outputfolder, model_dir))
dir.create(paste0(outputfolder, "plots"))
df <- df[hospital_or_icu==1]
write.csv(df, paste0(outputfolder, "prepped_data_", datadate, "_hosp_duration_for_gather.csv"))

df[, follow_up_days_orig := follow_up_days]

df$other_list_dur <- 0
df[study_id=="Becker et al any", other_list_dur := 1]

#for (i in offset_days) {
i <- 0
  if (ln_follow_up_days == 1) {
    df[, follow_up_days := log(follow_up_days_orig+i)]
  }
  
  # set up data
  mr_df <- MRData()
  
  mr_df$load_df(
    data = df, col_obs = "logitmean", col_obs_se = "logitse",
    col_covs = list("follow_up_days", "other_list_dur"), col_study_id = "study_id")
  
  model <- MRBRT(
    data = mr_df,
    cov_models =list(
      LinearCovModel("intercept", use_re = TRUE),
      LinearCovModel("follow_up_days", use_re = FALSE),
      LinearCovModel("other_list_dur", use_re = FALSE)
    ),
    inlier_pct = 1)
  #  LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_uniform = array(c(min, max)))
  

  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  (coeffs <- rbind(model$cov_names, model$beta_soln))
  write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_hosp_offset", i, ".csv"))
  
  # save model object
  py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1_offset", i, ".pkl"), pickle = "dill")
  #py_save_object(object = model_no, filename = paste0(folder, "FILEPATH", model_dir_no, "mod1.pkl"), pickle = "dill")
  
  # make predictions for full day-series
  # subtract one from the day count because we start follow up at day 0
  days <- (year_end - year_start + 1) * 365
  predict_matrix_hospicu <- data.table(intercept = model$beta_soln[1], follow_up_days = c(0:days), other_list_dur = 0)
  predict_matrix <- predict_matrix_hospicu
  if (ln_follow_up_days == 1) {
    predict_matrix <- data.table(intercept = model$beta_soln[1], follow_up_days = log(c(0:days)+i))
  }

  predict_data <- MRData()
  predict_data$load_df(
    data = predict_matrix,
    col_covs=list("follow_up_days", "other_list_dur"))
  
  #n_samples <- 1000L
  samples <- model$sample_soln(sample_size = n_samples)
  (beta_hosp <- mean(samples[[1]][,2]))
  (beta_hosp_sd <- sd(samples[[1]][,2]))
  (beta_hosp_sd <- abs(0.1 * beta_hosp))
  
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
  draws_raw$other_list_dur <- NULL
  
  draws_raw <- melt(data = draws_raw, id.vars = c("intercept", "follow_up_days"))
  setnames(draws_raw, "variable", "draw")
  setnames(draws_raw, "value", "proportion")
  draws_raw$intercept <- NULL
  if (ln_follow_up_days != 1) {
    draws_raw$proportion <- exp(draws_raw$proportion) / (1 + exp(draws_raw$proportion))
  }
  draws_raw <- reshape(draws_raw, idvar = c("follow_up_days"), timevar = "draw", direction = "wide")
  
  setnames(draws_raw, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  draws_save <- copy(draws_raw)
  if (ln_follow_up_days == 1) {
    draws_raw[, follow_up_days := round(exp(follow_up_days)-i,0)]
    draws_save[, follow_up_days := round(exp(follow_up_days)-i,0)]
  }
  
  draws_save$hospital_icu <- 1
  write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws_offset", i, ".csv"))
  
  
  predict_matrix$pred_raw <- model$predict(predict_data, sort_by_data_id = TRUE)
  if (ln_follow_up_days != 1) {
    predict_matrix$pred <- exp(predict_matrix$pred_raw) / (1 + exp(predict_matrix$pred_raw))
  } else {
    predict_matrix$pred <- predict_matrix$pred_raw
  }
  predict_matrix$pred_lo <- apply(draws_raw[,2:ncol(draws_raw)], 1, function(x) quantile(x, 0.025))
  predict_matrix$pred_hi <- apply(draws_raw[,2:ncol(draws_raw)], 1, function(x) quantile(x, 0.975))
  used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
  used_data <- as.data.table(used_data)
  rr_summaries <- copy(predict_matrix)
  
  if (ln_follow_up_days == 1) {
    rr_summaries[, follow_up_days := round(exp(follow_up_days)-i,0)]
  }
  rr_summaries
  rr_summaries$pred_hi[rr_summaries$pred_hi>1] <- 1
  rr_summaries$pred_lo[rr_summaries$pred_lo<0] <- 0
  rr_summaries$gamma <- mean(samples[[2]])
  write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary_offset", i, ".csv"))
  
  
  dur <- data.table(draws_raw)
  dur <- melt(data = dur, id.vars = c("follow_up_days"))
  if (ln_follow_up_days == 1) {
    dur <- dur[follow_up_days >= 0]
  }
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
  write.csv(dur, file = paste0(outputfolder, "duration_parameters_hospicu_offset", i, ".csv"))
  
  # day end for threshold of proportion going below 0.001, for calculation of overall median duration 
  # (not used directly in analysis, but rather for presentations, etc)
  
  if (ln_follow_up_days != 1) {
    dur$day_end_whole <- (log(0.001/(1-0.001))-dur$beta0)/dur$beta1
    dur <- dur[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 0)))]
    dur <- dur[, integral_start_who := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 60)))]
    dur <- dur[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * day_end_whole)))]
    dur <- dur[, prop_start := exp(beta0) / (1+exp(beta0))]
    # 95 days from infection is WHO's def of 3 months from symptom onset (which is 5 days after infection)
    dur <- dur[, prop_start_who := exp(beta0 + beta1 * 60) / (1 + exp((beta0 + beta1 * 60)))]
  } else if (ln_follow_up_days == 1) {
    dur$day_end_whole <- 18000
    dur[, integral_start := beta1 * (i * log(i) - i) + beta0 * i]
    dur[, integral_very_end := beta1 * (day_end_whole * log(day_end_whole) - day_end_whole) + beta0 * day_end_whole]
    dur <- dur[, prop_start := beta0]
  }
  
  dur <- dur[, integral_whole_who := integral_very_end - integral_start_who]
  dur$duration_whole_who <- dur$integral_whole_who / dur$prop_start_who
  dur <- dur[, integral_whole := integral_very_end - integral_start]
  dur$duration_whole <- dur$integral_whole / dur$prop_start
  (hospdur_all <- median(dur$duration_whole))
  (hospdur_yr_all <- hospdur_all / 365)
  (hospdur <- median(dur$duration_whole_who))
  (hospdur_yr <- hospdur / 365)
  
  
  if (ln_follow_up_days != 1) {
    used_data$obs_exp <- exp(used_data$obs) / (1 + exp(used_data$obs))
  } else {
    used_data$obs_exp <- used_data$obs
    used_data[, follow_up_days := exp(follow_up_days)-i]
  }
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
    ggtitle(paste0(out, ", hospital/ICU COVID cases"), subtitle = paste0("median duration = ", round(hospdur, 1), " days from 3 months post-symptom onset \n", round(hospdur_all, 1), " days from end of acute phase")) +
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
  
  plot <- ggplot(data=rr_summaries, aes(x=follow_up_days, y=pred), fill = "blue")+
    geom_ribbon(data= rr_summaries[coflow==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries[coflow==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries[coflow==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries[coflow==1], aes(x=follow_up_days, y=pred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste0(out, ", hospital/ICU COVID cases"), subtitle = paste0("median duration = ", round(hospdur, 1), " days from 3 months post-symptom onset \n", round(hospdur_all, 1), " days from end of acute phase")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=1) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs_exp) , color="dark gray", shape=1, alpha=1) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 8, spaceLegend = 1)

  plot
  #pdf(paste0(folder,"FILEPATH", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_hosp_duration_offset", i, ".pdf"), width = 8, height = 5)
  
  
  
  # adjusted for other list in Becker et al
  if (!("other_list_dur" %in% names(used_data))) {
    used_data$other_list_dur <- 0
  }
  (beta_other_list_dur <- mean(samples[[1]][,3]))
  used_data$obs_adj <- used_data$obs - used_data$other_list_dur * beta_other_list_dur
  used_data$obs_exp <- exp(used_data$obs_adj) / (1 + exp(used_data$obs_adj))
  
  
  
  # plot results of model using data that does control for blood pressure
  plot <- ggplot(data=rr_summaries, aes(x=follow_up_days, y=pred), fill = "blue")+
    geom_ribbon(data= rr_summaries[coflow==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries[coflow==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries[coflow==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries[coflow==1], aes(x=follow_up_days, y=pred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste0(out, ", hospital/ICU COVID cases"), subtitle = paste0("median duration = ", round(hospdur, 1), " days from 3 months post-symptom onset \n", round(hospdur_all, 1), " days from end of acute phase")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=1) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs_exp) , color="dark gray", shape=1, alpha=1) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 8, spaceLegend = 1)
  plot
  #pdf(paste0(folder,"FILEPATH", exp, "_", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", "_hosp_duration_adjusted_offset", i, ".pdf"), width = 8, height = 5)
  
  #}

coeffs
##########################################################
##  COMMUNITY CASES: DURATION
##########################################################

out <- "duration"
message(paste0("working on duration"))


#for (pop in c('children', 'adults', 'all')) {
#for (pop in c('all')) {
pop <- 'all'
  print(pop)
  
  if (pop == 'children') {
    df <- df_ped[study_id %in% c('CSS peds', 'UK CIS')]
    df[study_id == 'UK CIS', study_id := 'UK CIS peds']
  } else if (pop == 'adults') {
    df <- df_adult[!is.na(outcome) | study_id=="CSS"]
  } else {
    df <- rbind(df_ped[study_id %in% c('CSS peds', 'UK CIS')], 
                df_adult[!is.na(outcome) | study_id=="CSS"])
    df[study_id=='UK CIS' & age_end<25, study_id := 'UK CIS peds']
  }
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
  df <- df[female==0 & male==0 & !is.na(standard_error) & !is.na(logitse)]
  if (pop == 'children') {
    df <- df[(study_id=="UK CIS peds" & outcome=='any') | (study_id=="CSS peds" & outcome=='any')]
  } else if (pop == 'adults') {
    df <- df[study_id=="Cirulli et al" | study_id=="CO-FLOW" | study_id=="CSS" | study_id=="Faroe" | (study_id=="UK CIS" & outcome=='any') | 
               study_id=="Zurich CC prosp" | study_id=="Zurich CC retro"]
  } else {
    df <- df[study_id=="Cirulli et al" | study_id=="CO-FLOW" | study_id=="CSS" | study_id=="Faroe" | (study_id=="UK CIS" & outcome=='any') | 
               study_id=="Zurich CC prosp" | study_id=="Zurich CC retro" | (study_id=="CSS peds" & outcome=='any') | (study_id=="UK CIS peds" & outcome=='any')]
  }
  table(df$study_id, df$female)
  df$study_id <- paste(df$study_id, df$outcome)
  df <- df[outcome=="any" | outcome=="fat" | outcome=="cog" | outcome=="rsp"]
  table(df$study_id, df$outcome)
  df <- df[outcome=="any"]
  

  # set up folder
  model_dir <- paste0(out, "FILEPATH")
  dir.create(paste0(outputfolder, model_dir))
  dir.create(paste0(outputfolder, "plots"))
  df <- df[hospital_or_icu==0]
  write.csv(df, paste0(outputfolder, "prepped_data_", datadate, "_comm_duration_for_gather_", pop, ".csv"))
  
  # set up data
  mr_df <- MRData()
  
  if (pop == 'children') {
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
  } else if (pop == 'adults' | pop == 'all') {
    mr_df$load_df(
      data = df, col_obs = "logitmean", col_obs_se = "logitse",
      col_covs = list("follow_up_days", "other_list", "children"), col_study_id = "study_id")
    
    model <- MRBRT(
      data = mr_df,
      cov_models =list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("follow_up_days", use_re = FALSE),
        LinearCovModel("other_list", use_re = FALSE),
        LinearCovModel("children", use_re = FALSE)
      ),
      inlier_pct = 1)
  }
  #  LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_uniform = array(c(min, max)))
  
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  (coeffs <- rbind(model$cov_names, model$beta_soln))
  write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_comm_", pop, ".csv"))
  
  # save model object
  py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1_", pop, ".pkl"), pickle = "dill")
  
  # make predictions for full year
  predict_matrix_midmod <- data.table(intercept = model$beta_soln[1], follow_up_days = c(0:days), other_list = 0, children = 0)
  predict_matrix_hospicu <- data.table(intercept = model$beta_soln[1], follow_up_days = c(0:days), other_list = 0, children = 0)
  predict_matrix <- predict_matrix_hospicu
  
  if (version<18 | pop == 'children') {
    predict_data <- MRData()
    predict_data$load_df(
      data = predict_matrix,
      col_covs=list("follow_up_days"))
  } else {
    predict_data <- MRData()
    predict_data$load_df(
      data = predict_matrix,
      col_covs=list("follow_up_days", "other_list", "children"))
  }
  
  samples <- model$sample_soln(sample_size = n_samples)
  if (pop == 'children') {
    (beta_comm_children <- mean(samples[[1]][,2]))
    (beta_comm_sd_children <- sd(samples[[1]][,2]))
    beta_other_list <- 1
    beta_other_list_sd <- 0.0001
  } else if (pop == 'adults') {
    (beta_comm_adults <- mean(samples[[1]][,2]))
    (beta_comm_sd_adults <- sd(samples[[1]][,2]))
  } else if (pop == 'all') {
    (beta_comm_all <- mean(samples[[1]][,2]))
    (beta_comm_sd_all <- sd(samples[[1]][,2]))
  }
  if (version>=18 & (pop == 'adults' | pop == 'all')) {
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
  draws_raw$intercept <- NULL
  draws_raw$children <- NULL
  draws_raw$pred <- NULL
  draws_raw$pred_raw <- NULL
  draws_raw <- melt(data = draws_raw, id.vars = c("follow_up_days"))
  setnames(draws_raw, "variable", "draw")
  setnames(draws_raw, "value", "proportion")
  draws_raw$proportion <- exp(draws_raw$proportion) / (1 + exp(draws_raw$proportion))
  
  draws_raw <- reshape(draws_raw, idvar = c("follow_up_days"), timevar = "draw", direction = "wide")
  setnames(draws_raw, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  draws_save <- draws_raw
  
  draws_save$hospital_icu <- 0
  write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws_", pop, ".csv"))
  
  
  
  
  
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
  setnames(dur0, "value", "prop_start")
  setnames(dur0, "V1", "beta0")
  setnames(dur0, "V2", "beta1")
  setnames(dur0, "variable", "draw")
  write.csv(dur0, file = paste0(outputfolder, "duration_parameters_midmod_", pop, ".csv"))
  
  
  
  # day end for threshold of proportion going below 0.001, for calculation of overall median duration 
  # (not used directly in analysis, but rather for presentations, etc)
  dur0$day_end_whole <- (log(0.001/(1-0.001))-dur0$beta0)/dur0$beta1
  
  dur0 <- dur0[, C := -(1 / beta1) * log(abs(1 + exp(beta0)))]
  dur0 <- dur0[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 0))) + C]
  dur0 <- dur0[, integral_start_who := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 81))) + C]
  dur0 <- dur0[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * day_end_whole))) + C]
  dur0 <- dur0[, integral_whole := integral_very_end - integral_start]
  dur0 <- dur0[, integral_whole_who := integral_very_end - integral_start_who]
  dur0 <- dur0[, prop_start := exp(beta0) / (1+exp(beta0))]
  dur0 <- dur0[, prop_start_who := exp(beta0 + beta1 * 81) / (1+exp(beta0 + beta1 * 81))]
  # calculate duration by year (for the pipeline) and overall (for reporting)
  dur0$duration_whole_who <- dur0$integral_whole_who / dur0$prop_start_who
  dur0$duration <- dur0$integral / dur0$prop_start
  dur0$duration_whole <- dur0$integral_whole / dur0$prop_start
  (commdur_all <- median(dur0$duration_whole))
  (commdur <- median(dur0$duration_whole_who))
  
  
  
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
  write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary_", pop, ".csv"))
  
  
  
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
    ggtitle(paste(out, ", mild/moderate COVID cases, ", pop), subtitle = paste0("median duration = ", round(commdur, 1), " days from 3 months post-symptom onset \n", round(commdur_all, 1), " days from end of acute phase")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.5,0.1), limits=c(0,0.5)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs_exp) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 8, spaceLegend = 1)
  plot
  
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", "_community_duration_zoom_", pop, ".pdf"), width = 8, height = 5)
  
  
  
  
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
    ggtitle(paste(out, ", mild/moderate COVID cases, ", pop), subtitle = paste0("median duration = ", round(commdur, 1), " days from 3 months post-symptom onset \n", round(commdur_all, 1), " days from end of acute phase")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(-12,0,2), limits=c(-12,0)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 8, spaceLegend = 1)
  
  #  guides(shape = guide_legend(override.aes = list(size = 1))) +
  plot
  #pdf(paste0(folder,"FILEPATH", "_", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", "_community_duration_logit_", pop, ".pdf"), width = 8, height = 5)
  
  ############################################
  # GRAPHS WITH ADJUSTED DATA
  
  if (!("other_list" %in% names(used_data))) {
    used_data$other_list <- 0
  }
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
    ggtitle(paste(out, ", mild/moderate COVID cases, ", pop), subtitle = paste0("median duration = ", round(commdur, 1), " days from 3 months post-symptom onset \n", round(commdur_all, 1), " days from end of acute phase")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,0.5,0.1), limits=c(0,0.5)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs_exp) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 8, spaceLegend = 1)
  plot
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_community_duration_adjusted_", pop, ".pdf"), width = 8, height = 5)
  
  
  
  plot <- ggplot(data=rr_summaries2, aes(x=follow_up_days, y=logitpred), fill = "blue")+
    geom_ribbon(data= rr_summaries2[coflow==0], aes(x=follow_up_days, ymin=logitpred_lo, ymax=logitpred_hi),  fill="lightgrey", alpha=.5) +
    geom_line(data=rr_summaries2[coflow==0], aes(x=follow_up_days, y=logitpred), color = "blue", size=1) +
    geom_ribbon(data= rr_summaries2[coflow==1], aes(x=follow_up_days, ymin=logitpred_lo, ymax=logitpred_hi),  fill="pink", alpha=.3) +
    geom_line(data=rr_summaries2[coflow==1], aes(x=follow_up_days, y=logitpred), color = "red", size=1) +
    ylab("Proportion") +
    xlab("Follow up (days)") +
    ggtitle(paste(out, ", mild/moderate COVID cases, ", pop), subtitle = paste0("median duration = ", round(commdur, 1), " days from 3 months post-symptom onset \n", round(commdur_all, 1), " days from end of acute phase")) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(-12,0,2), limits=c(-12,0)) + 
    scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data[w ==1,], aes(x=follow_up_days, y=obs, color=used_data[w ==1, study_id]), 
               size=(used_data$weight[used_data$w==1]), shape=16, alpha=0.5) +
    geom_point(data=used_data[w ==0,], aes(x=follow_up_days, y=obs) , color="dark gray", shape=1, alpha=0.4) +
    geom_hline(yintercept=0, linetype="dashed", color="dark grey", size=1) +
    theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 8, spaceLegend = 1)
  
  #  guides(shape = guide_legend(override.aes = list(size = 1))) +
  plot
  #pdf(paste0(folder,"FILEPATH", exp, "_", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_community_duration_log_adjusted_", pop, ".pdf"), width = 8, height = 5)
  
  
  
  coeffs
#}




unique(dataset$outcome)
#beta_hosp <- -0.00327
#minh <- 1.9*beta_hosp
#maxh <- 0.1*beta_hosp
#beta_comm <- -0.0182
#minc <- 1.9*beta_comm
#maxc <- 0.1*beta_comm

hospdur_all
hospdur
commdur_all
commdur
beta_hosp
#beta_comm_children
#beta_comm_adults
beta_comm_all
beta_comm_sd_all

########################################################################################################
# GET PRIOR FOR ICU COVARIATE
# using VA and PRA data

#for (pop in c('children', 'adults', 'all')) {
for (pop in c('all')) {
  if (pop == 'children' ) {
    df_va <- df_ped[(study_id=="PRA" & age_start==0 & age_end==19 & sex=="Both" & hospital_or_icu==1)]
  } else if (pop == 'adults') {
    df_va <- df_adult[study_id=="Veterans Affairs" | (study_id=="PRA" & age_start==20 & age_end==99 & sex=="Both" & hospital_or_icu==1)]
  } else if (pop == 'all') {
    df_va <- dataset[study_id=="Veterans Affairs" | (study_id=="PRA" & age_start==0 & age_end==99 & sex=="Both" & hospital_or_icu==1)]
  }
  df_va <- df_va[outcome=='fat' | outcome=='rsp' | outcome=='cog']
  df_va <- df_va[!is.na(standard_error) & !is.na(logitse)]
  df_va$re <- paste0(df_va$study_id, "_", df_va$outcome)
  
  write.csv(df_va, paste0(outputfolder, "prepped_data_", datadate, "_icu_prior_for_gather_", pop, ".csv"))
  
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
  if (pop == 'children') {
    beta_icu_children <- mean(samples[[1]][,2])
    beta_icu_sd_children <- sd(samples[[1]][,2])
  } else if (pop == 'adults') {
    beta_icu_adults <- mean(samples[[1]][,2])
    beta_icu_sd_adults <- sd(samples[[1]][,2])
  } else if (pop == 'all') {
    beta_icu_all <- mean(samples[[1]][,2])
    beta_icu_sd_all <- sd(samples[[1]][,2])
  }
  write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_icu_beta_", pop, ".csv"))
}  





########################################################
# make sure pa-COVID and PRA data with different follow-up times are treated the same (not as a pattern of recovery)
# random effect = study (and follow-up time for these two sources)

df_ped$re <- df_ped$study_id
df_ped <- df_ped[study_id=="pa-COVID" | study_id=="PRA", re := paste(re, follow_up_value)]
df_adult$re <- df_adult$study_id
df_adult <- df_adult[study_id=="pa-COVID" | study_id=="PRA", re := paste(re, follow_up_value)]


############################################################################################################################################
df_ped <- df_ped[!is.na(outcome)]
table(df_ped$symptom_cluster[is.na(df_ped$outcome)])
table(df_ped$symptom_cluster, df_ped$study_id)
df_ped$weight <- 1/(30*df_ped$standard_error)
df_ped$weight[df_ped$weight>10] <- 10
#  plot(df_ped$weight, df_ped$standard_error)
df_adult <- df_adult[!is.na(outcome)]
table(df_adult$symptom_cluster[is.na(df_adult$outcome)])
table(df_adult$symptom_cluster, df_adult$study_id)
df_adult$weight <- 1/(30*df_adult$standard_error)
df_adult$weight[df_adult$weight>10] <- 10
#  plot(df_adult$weight, df_adult$standard_error)

# do a run of symptom clusters with only cohort and administrative data
if (version == 61) {
  df_ped <- df_ped[memory_problems==0 & fatigue==0 & cough==0 & shortness_of_breath==0]
  df_adult <- df_adult[memory_problems==0 & fatigue==0 & cough==0 & shortness_of_breath==0]
}


#  for (pop in c('all', 'children', 'adults')) {
for (pop in c('all')) {
  if (pop == 'children') {
    df <- df_ped
  } else if (pop == 'adults') {
    df <- df_adult
  } else if (pop == 'all') {
    df <- rbind(df_ped, df_adult)
  }
  df <- df[age_specific==0]
  table(df$study_id, df$female)
#  df$sex_specific <- 0
#  df$sex_specific[df$female==1 | df$male==0] <- 1
  df$sex_keep <- 0
  df$sex_keep[(df$study_id=="Iran ICC")] <- 1
  df$sex_keep[(df$study_id=="Italy ISARIC")] <- 1
  df$sex_keep[(df$study_id=="Sechenov StopCOVID")] <- 1
  df$sex_keep[(df$study_id=="UK CIS" & df$follow_up_value==5)] <- 1
  #  df <- df[(sex_keep==1 & sex!="Both") | (sex_keep==0 & sex=="Both")]
  if (drop_cough == 1) {
    df <- df[cough!=1]
  }
  
  # v59+ remove Garcia-Abellan from symptom cluster models
  df <- df[study_id!="García‑Abellán J et al"]
  length(unique(df$study_id))
  
  write.csv(df, paste0(outputfolder, "prepped_data_", datadate, "_symptoms_for_gather_", version, pop, ".csv"))
  
  for(out in c('any', 'rsp', 'fat', 'cog')) {
#  for(out in c('any', 'rsp')) {
    model_dir <- paste0(out, "FILEPATH")
    dir.create(paste0(outputfolder, model_dir))
    message(paste0("working on ", out))
    if (pop == 'children') {
      df <- df_ped[outcome==out]
      beta_icu <- beta_icu_children
      beta_icu_sd <- beta_icu_sd_children
      beta_comm <- beta_comm_children
      beta_comm_sd <- beta_comm_sd_children
    } else if (pop == 'adults') {
      df <- df_adult[outcome==out]
      beta_icu <- beta_icu_adults
      beta_icu_sd <- beta_icu_sd_adults
      beta_comm <- beta_comm_adults
      beta_comm_sd <- beta_comm_sd_adults
    } else if (pop == 'all') {
      df <- rbind(df_ped, df_adult)
      df <- df[outcome==out]
      beta_icu <- beta_icu_all
      beta_icu_sd <- beta_icu_sd_all
      beta_comm <- beta_comm_all
      beta_comm_sd <- beta_comm_sd_all
      beta_comm_sd <- abs(beta_comm_all * 0.1)
#      if (version >= 65) {
#        beta_comm_sd <- beta_comm_sd_all * 3
#        beta_hosp_sd <- beta_hosp_sd * 2
#      }
    }
    df_any <- rbind(df_ped[outcome %in% c('any', 'rsp', 'fat', 'cog')], df_adult[outcome %in% c('any', 'rsp', 'fat', 'cog')])
    df_any <- df_any[age_specific==0]
    df <- df[!is.na(standard_error) & !is.na(logitse)]
    if (drop_cough == 1) {
      df <- df[cough!=1]
      df_any <- df_any[cough!=1]
    }
    
    # v59+ remove Garcia-Abellan from symptom cluster models
    df <- df[study_id!="García‑Abellán J et al"]
    #    if (version > 63 & out == 'any') {
    #      df <- df[other_list != 1]
    #    }
    
    df <- df[age_specific==0]
    table(df$study_id, df$female)
    df$sex_specific <- 0
    df$sex_specific[df$female==1 | df$male==0] <- 1
    df$sex_keep <- 0
    df$sex_keep[(df$study_id=="Iran ICC")] <- 1
    df$sex_keep[(df$study_id=="Italy ISARIC")] <- 1
    df$sex_keep[(df$study_id=="Sechenov StopCOVID")] <- 1
    if (out == "any") {
      #      df$sex_keep[df$study_id=="Zurich CC" & df$age_start==18 & df$age_end==99 & df$follow_up_value!=6] <- 1
      df$sex_keep[(df$study_id=="Zurich CC retro" | df$study_id=="Zurich CC prosp") & df$age_start==18 & df$age_end==99] <- 1
    }
    if (pop == 'children') {
      df[study_id == 'PRA', sex_keep := 1]
      df[study_id == 'Iran ICC', sex_keep := 0]
    }
    table(df$study_id, df$sex_keep)
    
    table(df$study_id, df$other_list)
    df$follow_up_days_hosp <- df$follow_up_days * df$hospital_icu
    df$follow_up_days_comm <- df$follow_up_days * (1-df$hospital_icu)
    
    
    
    
    
    #############################
    # GET PRIOR ON FEMALE BETA
    df_sex <- df[sex_keep==1]
    df_sex$re <- paste(df_sex$re, df_sex$follow_up_days)
    table(df_sex$study_id, df_sex$female)
    mr_df <- MRData()
    mr_df$load_df(
      data = df_sex, col_obs = "logitmean", col_obs_se = "logitse",
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
    write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_female_beta", out, "_", pop, ".csv"))
    
    
    #############################
    # GET PRIOR ON CHILDREN BETA
    
    mr_df <- MRData()
    if (out == 'any') {
      df_any$children_keep <- 0
      df_any[study_id=="CSS peds", study_id := "CSS"]
      df_any[(study_id=="CSS" & follow_up_days %in% c(19, 47) & outcome=='any') | 
               (study_id=="UK CIS" & follow_up_days %in% c(26, 75) & sex=="Both" & !(age_start==2 & age_end==99)) | 
               (study_id=="PRA" & sex=="Both") | (study_id=="Iran ICC" & sex=="Both"), children_keep := 1]
      df_any$re2 <- paste(df_any$study_id, as.character(df_any$outcome))
      df_any[study_id!="PRA", re2 := paste(re2, follow_up_days)]
      mr_df$load_df(
        data = df_any[children_keep==1 & hospital_or_icu==0], col_obs = "logitmean", col_obs_se = "logitse",
        col_covs = list("children"), col_study_id = "re2")
    } else {
      df$children_keep <- 0
      df[(study_id %in% c("CSS", "CSS peds") & follow_up_days %in% c(19, 47) & outcome=='any') | 
           (study_id=="UK CIS" & follow_up_days %in% c(26, 75) & sex=="Both" & !(age_start==2 & age_end==99)) | 
           (study_id=="PRA" & sex=="Both")  , children_keep := 1]
      df[study_id!="PRA", re2 := paste(re, follow_up_days)]
      df[study_id=="PRA", re2 := study_id]
      mr_df$load_df(
        data = df[children_keep==1 & hospital_or_icu==0], col_obs = "logitmean", col_obs_se = "logitse",
        col_covs = list("children"), col_study_id = "re2")
    }
    
    model <- MRBRT(
      data = mr_df,
      cov_models =list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("children", use_re = FALSE)
      ),
      inlier_pct = 1)
    
    # fit model
    model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    samples <- model$sample_soln(sample_size = n_samples)
    (beta_children_c <- mean(samples[[1]][,2]))
    (beta_children_c_sd <- sd(samples[[1]][,2]))
    (coeffs <- rbind(model$cov_names, model$beta_soln))
    write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_children_comm_beta", out, "_", pop, ".csv"))
    
    
    if (hosp_kids == 1) {
      # hospital/ICU data
      mr_df <- MRData()
      if (out == 'any') {
        mr_df$load_df(
          data = df_any[children_keep==1 & hospital_or_icu==1], col_obs = "logitmean", col_obs_se = "logitse",
          col_covs = list("children"), col_study_id = "re")
      } else {
        mr_df$load_df(
          data = df[children_keep==1 & hospital_or_icu==1], col_obs = "logitmean", col_obs_se = "logitse",
          col_covs = list("children"), col_study_id = "re")
      }
      
      model <- MRBRT(
        data = mr_df,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("children", use_re = FALSE)
        ),
        inlier_pct = 1)
      df$children_keep <- NULL
      
      # fit model
      model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
      
      samples <- model$sample_soln(sample_size = n_samples)
      (beta_children_h <- mean(samples[[1]][,2]))
      (beta_children_h_sd <- sd(samples[[1]][,2]))
      (coeffs <- rbind(model$cov_names, model$beta_soln))
      write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_children_hosp_beta", out, "_", pop, ".csv"))
    } else {
      beta_children_h <- 0
      beta_children_h_sd <- 0.01
    }
    
    
    #####################################
    # set up data
    df <- df[(sex_keep==1 & sex!="Both") | (sex_keep==0 & sex=="Both")]
    table(df$study_id, df$sex)
    mr_df <- MRData()
    
    if(out=="any") {
      if (pop == "all" & hosp_kids == 1) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "other_list")
        cvs_c <- list("female", "male", "follow_up_days", "children", "other_list")
      } else if (pop == "all" & hosp_kids == 0) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "other_list")
        cvs_c <- list("female", "male", "follow_up_days", "children", "other_list")
      } else {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "other_list")
        cvs_c <- list("female", "male", "follow_up_days", "other_list")
      }
      title <- "At least 1 symptom cluster"
    } else if (out=="cog") {
      #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
      #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
      if (pop == "all" & hosp_kids == 1) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "memory_problems")
        cvs_c <- list("female", "male", "follow_up_days", "children", "memory_problems")
      } else if (pop == "all" & hosp_kids == 0) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "memory_problems")
        cvs_c <- list("female", "male", "follow_up_days", "children", "memory_problems")
      } else {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "memory_problems")
        cvs_c <- list("female", "male", "follow_up_days", "memory_problems")
      }
      title <- "Cognitive symptoms"
    } else if (out=="fat") {
      #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
      #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
      if (pop == "all" & hosp_kids == 1) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "fatigue", "administrative")
        cvs_c <- list("female", "male", "follow_up_days", "children", "fatigue", "administrative")
      } else if (pop == "all" & hosp_kids == 0) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "fatigue", "administrative")
        cvs_c <- list("female", "male", "follow_up_days", "children", "fatigue", "administrative")
      } else {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "fatigue", "administrative")
        cvs_c <- list("female", "male", "follow_up_days", "fatigue", "administrative")
      }
      title <- "Post-acute fatigue syndrome"
    } else if (out=="rsp") {
      #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
      #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
      if (pop == "all" & hosp_kids == 1) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "cough", 
                      "shortness_of_breath", "administrative")
        cvs_c <- list("female", "male", "follow_up_days", "children", "cough", 
                      "shortness_of_breath", "administrative")
      } else if (pop == "all" & hosp_kids == 0) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "cough", "shortness_of_breath", "administrative")
        cvs_c <- list("female", "male", "follow_up_days", "children", "cough", "shortness_of_breath", "administrative")
        if (drop_cough) {
          cvs_h <- list("icu", "female", "male", "follow_up_days", "shortness_of_breath", "administrative")
          cvs_c <- list("female", "male", "follow_up_days", "children", "shortness_of_breath", "administrative")
          df <- df[cough!=1]
        }
      } else {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "cough", "shortness_of_breath", "administrative")
        cvs_c <- list("female", "male", "follow_up_days", "cough", "shortness_of_breath", "administrative")
      }
      title <- "Respiratory symptoms"
    } else {
      #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
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
      if (pop == "all") {
        if (hosp_kids == 1) {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_h, beta_children_h_sd))),
              LinearCovModel("other_list", use_re = FALSE, prior_beta_gaussian = array(c(beta_other_list, beta_other_list_sd*3)))
            ),
            inlier_pct = inlier)
        } else {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("other_list", use_re = FALSE, prior_beta_gaussian = array(c(beta_other_list, beta_other_list_sd*3)))
            ),
            inlier_pct = inlier)
        }
        if (version < 64) {
          model_c <- MRBRT(
            data = mr_df_c,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_c, beta_children_c_sd))),
              LinearCovModel("other_list", use_re = FALSE, prior_beta_gaussian = array(c(beta_other_list, beta_other_list_sd)))
            ),
            inlier_pct = inlier)
        } else {
          model_c <- MRBRT(
            data = mr_df_c,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_c, beta_children_c_sd))),
              LinearCovModel("other_list", use_re = FALSE, prior_beta_gaussian = array(c(beta_other_list, beta_other_list_sd*3)))
            ),
            inlier_pct = inlier)
        }
      } else if (pop != 'all') {
        model_h <- MRBRT(
          data = mr_df_h,
          cov_models =list(
            LinearCovModel("intercept", use_re = TRUE),
            LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
            LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
            LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier)))
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
      }
      
    } else if (out=="cog") {
      if (pop == "all") {
        if (hosp_kids == 1) {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_h, beta_children_h_sd))),
              LinearCovModel("memory_problems", use_re = FALSE)
            ),
            inlier_pct = inlier)
        } else {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("memory_problems", use_re = FALSE)
            ),
            inlier_pct = inlier)
        }
        model_c <- MRBRT(
          data = mr_df_c,
          cov_models =list(
            LinearCovModel("intercept", use_re = TRUE),
            LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
            LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
            LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_c, beta_children_c_sd))),
            LinearCovModel("memory_problems", use_re = FALSE)
          ),
          inlier_pct = inlier)
      } else {
        model_h <- MRBRT(
          data = mr_df_h,
          cov_models =list(
            LinearCovModel("intercept", use_re = TRUE),
            LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
            LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
            LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
            LinearCovModel("memory_problems", use_re = FALSE)
          ),
          inlier_pct = inlier)
        
        model_c <- MRBRT(
          data = mr_df_c,
          cov_models =list(
            LinearCovModel("intercept", use_re = TRUE),
            LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
            LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
            LinearCovModel("memory_problems", use_re = FALSE)
          ),
          inlier_pct = inlier)
      }
    } else if (out=="fat") {
      if (pop == "all") {
        if (hosp_kids == 1) {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_h, beta_children_h_sd))),
              LinearCovModel("fatigue", use_re = FALSE),
              LinearCovModel("administrative", use_re = FALSE)
            ),
            inlier_pct = inlier)
        } else {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("fatigue", use_re = FALSE),
              LinearCovModel("administrative", use_re = FALSE)
            ),
            inlier_pct = inlier)
        }
        
        model_c <- MRBRT(
          data = mr_df_c,
          cov_models =list(
            LinearCovModel("intercept", use_re = TRUE),
            LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
            LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
            LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_c, beta_children_c_sd))),
            LinearCovModel("fatigue", use_re = FALSE),
            LinearCovModel("administrative", use_re = FALSE)
          ),
          inlier_pct = inlier)
      } else {
        model_h <- MRBRT(
          data = mr_df_h,
          cov_models =list(
            LinearCovModel("intercept", use_re = TRUE),
            LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
            LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
            LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
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
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
            LinearCovModel("fatigue", use_re = FALSE),
            LinearCovModel("administrative", use_re = FALSE)
          ),
          inlier_pct = inlier)
      }
    } else if (out=="rsp") {
      if (pop == "all") {
        if (hosp_kids == 1) {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_h, beta_children_h_sd))),
              LinearCovModel("cough", use_re = FALSE),
              LinearCovModel("shortness_of_breath", use_re = FALSE),
              LinearCovModel("administrative", use_re = FALSE, prior_beta_uniform = array(0,2))
            ),
            inlier_pct = inlier)
        } else if (drop_cough==1) {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("shortness_of_breath", use_re = FALSE),
              LinearCovModel("administrative", use_re = FALSE, prior_beta_uniform = array(0,2))
            ),
            inlier_pct = inlier)
        } else {
          model_h <- MRBRT(
            data = mr_df_h,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
              LinearCovModel("cough", use_re = FALSE),
              LinearCovModel("shortness_of_breath", use_re = FALSE),
              LinearCovModel("administrative", use_re = FALSE, prior_beta_uniform = array(0,2))
            ),
            inlier_pct = inlier)
        }
        #        inlier <- 0.8
        if (drop_cough==1) {
          model_c <- MRBRT(
            data = mr_df_c,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_c, beta_children_c_sd))),
              LinearCovModel("shortness_of_breath", use_re = FALSE),
              LinearCovModel("administrative", use_re = FALSE)
            ),
            inlier_pct = inlier)
        } else {
          model_c <- MRBRT(
            data = mr_df_c,
            cov_models =list(
              LinearCovModel("intercept", use_re = TRUE),
              LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
              LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
              LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
              LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children_c, beta_children_c_sd))),
              LinearCovModel("cough", use_re = FALSE),
              LinearCovModel("shortness_of_breath", use_re = FALSE),
              LinearCovModel("administrative", use_re = FALSE)
            ),
            inlier_pct = inlier)
        }
      } else {
        model_h <- MRBRT(
          data = mr_df_h,
          cov_models =list(
            LinearCovModel("intercept", use_re = TRUE),
            LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
            LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
            LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
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
            LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
            LinearCovModel("cough", use_re = FALSE),
            LinearCovModel("shortness_of_breath", use_re = FALSE),
            LinearCovModel("administrative", use_re = FALSE)
          ),
          inlier_pct = inlier)
      }
    } else if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
      model_h <- MRBRT(
        data = mr_df_h,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier)))
        ),
        inlier_pct = inlier)
      
      model_c <- MRBRT(
        data = mr_df_c,
        cov_models =list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier)))
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
    if (hosp_kids == 1) {
      coeffs_h$prior_children <- beta_children_h
      coeffs_h$prior_children_sd <- beta_children_h_sd
    }
    coeffs_h$prior_follow_up <- beta_hosp
    if (out=='any') {
      coeffs_h$prior_follow_up_sd <- beta_hosp_sd*follow_up_sd_multiplier
    } else {
      coeffs_h$prior_follow_up_sd <- beta_hosp_sd*follow_up_sd_multiplier
    }
    coeffs_h$prior_other_list <- beta_other_list
    coeffs_h$prior_other_list_sd <- beta_other_list_sd
    (coeffs_c <- rbind(model_c$cov_names, model_c$beta_soln))
    coeffs_c <- data.table(coeffs_c)
    coeffs_c$hospital_icu <- 0
    coeffs_c$hospital_icu <- 1
    coeffs_c$prior_female <- beta_female
    coeffs_c$prior_female_sd <- beta_female_sd
    coeffs_c$prior_male <- beta_male
    coeffs_c$prior_male_sd <- beta_male_sd
    coeffs_h$prior_children <- beta_children_c
    coeffs_h$prior_children_sd <- beta_children_c_sd
    coeffs_c$prior_follow_up <- beta_comm
#    if (out=='any') {
#      coeffs_c$prior_follow_up_sd <- beta_comm_sd*follow_up_sd_multiplier
#    } else {
      coeffs_c$prior_follow_up_sd <- beta_comm_sd*follow_up_sd_multiplier
#    }
    coeffs_c$prior_other_list <- beta_other_list
    coeffs_c$prior_other_list_sd <- beta_other_list_sd
    write.csv(coeffs_h, paste0(outputfolder, model_dir, "coeffs_hospital_icu_", pop, ".csv"))
    write.csv(coeffs_c, paste0(outputfolder, model_dir, "coeffs_community_", pop, ".csv"))
    
    
    
    # save model object
    py_save_object(object = model_h, filename = paste0(outputfolder, model_dir, "mod1_h_", pop, ".pkl"), pickle = "dill")
    py_save_object(object = model_c, filename = paste0(outputfolder, model_dir, "mod1_c_", pop, ".pkl"), pickle = "dill")
    
    # make predictions for full year
    predict_matrix_midmod_M <- data.table(intercept = model_c$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=0, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_midmod_F <- data.table(intercept = model_c$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=0, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_hosp_M <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=1, icu=0, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_hosp_F <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=1, icu=0, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_icu_M <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=1, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_icu_F <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=1, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
    predict_matrix_c <- rbind(predict_matrix_midmod_M,predict_matrix_midmod_F)
    predict_matrix_h <- rbind(predict_matrix_hosp_M,predict_matrix_hosp_F,predict_matrix_icu_M,predict_matrix_icu_F)
    predict_matrix_c$children <- 0
    predict_matrix_c_ch <- copy(predict_matrix_c)
    predict_matrix_c_ch$children <- 1
    if (kid_estimate_sex_specific_c == 0) {
      predict_matrix_c_ch$male <- 0
      predict_matrix_c_ch$female <- 0
    }
    predict_matrix_c <- rbind(predict_matrix_c, predict_matrix_c_ch)
    predict_matrix_c <- unique(predict_matrix_c)
    
    predict_matrix_h$children <- 0
    predict_matrix_h_ch <- copy(predict_matrix_h)
    predict_matrix_h_ch$children <- 1
    if (kid_estimate_sex_specific_h == 0) {
      predict_matrix_h_ch$male <- 0
      predict_matrix_h_ch$female <- 0
    }
    predict_matrix_h <- rbind(predict_matrix_h, predict_matrix_h_ch)
    predict_matrix_h <- unique(predict_matrix_h)
    
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
    draws_h[1:10,1:10]
    draws_h$intercept <- NULL
    setnames(draws_h, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
    draws_h$other_list <- NULL
    draws_h$shortness_of_breath <- NULL
    draws_h$memory_problems <- NULL
    draws_h$fatigue <- NULL
    draws_h$administrative <- NULL
    draws_h$cough <- NULL
    draws_h <- melt(data = draws_h, id.vars = c("hospital", "icu", "follow_up_days", "female", "male", "children"))
    setnames(draws_h, "variable", "draw")
    setnames(draws_h, "value", "proportion")
    draws_h$proportion <- exp(draws_h$proportion) / (1 + exp(draws_h$proportion))
    draws_h$proportion <- draws_h$proportion - offset
    draws_h[proportion>1 | is.na(proportion), proportion := 1]
    draws_h[proportion<0, proportion := 0]
    
    if (hosp_kids == 0) {
      draws_h <- unique(draws_h[,!c('children')])
      draws_h <- reshape(draws_h, idvar = c("hospital", "icu", "follow_up_days", "female", "male"), timevar = "draw", direction = "wide")
    } else {
      draws_h <- reshape(draws_h, idvar = c("hospital", "icu", "follow_up_days", "female", "male", "children"), timevar = "draw", direction = "wide")
    }
    setnames(draws_h, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
    draws_save <- draws_h[hospital==1 | icu==1]
    
    write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws_hospital_icu_", pop, ".csv"))
    
    
    
    dur <- data.table(draws_h)
    dur <- melt(data = dur, id.vars = c("hospital", "icu", "follow_up_days", "female", "male"))
    head(dur)
    dur <- dur[follow_up_days==0]
    dur$follow_up_days <- NULL
    dur <- cbind(dur, samples[[1]])
    setnames(dur, "value", "prop_start")
    setnames(dur, "V1", "beta0")
    setnames(dur, "V2", "beta1")
    setnames(dur, "variable", "draw")
    write.csv(dur, file = paste0(outputfolder, "duration_parameters_hospicu_", pop, ".csv"))
    
    
    
    
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
    draws_c <- melt(data = draws_c, id.vars = c("hospital", "icu", "follow_up_days", "female", "male", "children"))
    setnames(draws_c, "variable", "draw")
    setnames(draws_c, "value", "proportion")
    draws_c$proportion <- exp(draws_c$proportion) / (1 + exp(draws_c$proportion))
    draws_c$proportion <- draws_c$proportion - offset
    draws_c[proportion>1 | is.na(proportion), proportion := 1]
    draws_c[proportion<0, proportion := 0]
    
    draws_c <- reshape(draws_c, idvar = c("hospital", "icu", "follow_up_days", "female", "male", "children"), timevar = "draw", direction = "wide")
    setnames(draws_c, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
    draws_save <- draws_c[hospital==0 & icu==0]
    
    write.csv(draws_save, file =paste0(outputfolder, model_dir, "predictions_draws_community_", pop, ".csv"))
    
    
    
    predict_matrix_h$pred_raw <- model_h$predict(predict_data_h, sort_by_data_id = TRUE)
    if (hosp_kids == 0) {
      predict_matrix_h <- data.table(unique(predict_matrix_h[, !c("children")]))
    }
    predict_matrix_h$pred <- exp(predict_matrix_h$pred_raw) / (1 + exp(predict_matrix_h$pred_raw))
    predict_matrix_h$pred_lo <- apply(draws_h[,2:ncol(draws_h)], 1, function(x) quantile(x, 0.025))
    predict_matrix_h$pred_hi <- apply(draws_h[,2:ncol(draws_h)], 1, function(x) quantile(x, 0.975))
    predict_matrix_h[, pred := pred - offset]
    predict_matrix_h[, pred_lo := pred_lo - offset]
    predict_matrix_h[, pred_hi := pred_hi - offset]
    predict_matrix_h[pred_lo < 0, pred_lo := 0]
    used_data_h <- cbind(model_h$data$to_df(), data.frame(w = model_h$w_soln))
    used_data_h <- as.data.table(used_data_h)
    rr_summaries_h <- copy(predict_matrix_h)
    
    rr_summaries_h
    rr_summaries_h$pred_hi[rr_summaries_h$pred_hi>1] <- 1
    rr_summaries_h$gamma <- mean(samples_h[[2]])
    write.csv(rr_summaries_h, file =paste0(outputfolder, model_dir, "predictions_summary_hospital_icu_", pop, ".csv"))
    
    
    
    
    
    
    predict_matrix_c$pred_raw <- model_c$predict(predict_data_c, sort_by_data_id = TRUE)
    predict_matrix_c$pred <- exp(predict_matrix_c$pred_raw) / (1 + exp(predict_matrix_c$pred_raw))
    predict_matrix_c$pred_lo <- apply(draws_c[,2:ncol(draws_c)], 1, function(x) quantile(x, 0.025))
    predict_matrix_c$pred_hi <- apply(draws_c[,2:ncol(draws_c)], 1, function(x) quantile(x, 0.975))
    predict_matrix_c[, pred := pred - offset]
    predict_matrix_c[, pred_lo := pred_lo - offset]
    predict_matrix_c[, pred_hi := pred_hi - offset]
    predict_matrix_c[pred_lo < 0, pred_lo := 0]
    used_data_c <- cbind(model_c$data$to_df(), data.frame(w = model_c$w_soln))
    used_data_c <- as.data.table(used_data_c)
    rr_summaries_c <- copy(predict_matrix_c)
    
    rr_summaries_c
    rr_summaries_c$pred_hi[rr_summaries_c$pred_hi>1] <- 1
    rr_summaries_c$gamma <- mean(samples_c[[2]])
    write.csv(rr_summaries_c, file =paste0(outputfolder, model_dir, "predictions_summary_community_", pop, ".csv"))
    
    
    
    
    
    
    
    
    used_data_h$pop[used_data_h$icu==0] <- "hospital"
    used_data_h$pop[used_data_h$icu==1] <- "ICU"
    used_data_c$pop <- "community"
    
    df_se <- df[,c("study_id", "re", "hospital", "icu", "female", "male", 
                   "follow_up_days", "mean", "standard_error", "cough", 
                   "shortness_of_breath", "fatigue", "memory_problems", "children")]
    setnames(df_se, "standard_error", "se")
    setnames(used_data_h, "study_id", "re")
    setnames(used_data_c, "study_id", "re")
    used_data_h$hospital <- 0
    used_data_h <- used_data_h[icu==0, hospital := 1]
    used_data_c <- used_data_c[, hospital := 0]
    used_data_c <- used_data_c[, icu := 0]
    if (hosp_kids == 1) {
      hmerge_vars <- c("re", "icu", "hospital", "female", "male", "children", "follow_up_days")
      cmerge_vars <- c("re", "icu", "hospital", "female", "male", "children", "follow_up_days")
    } else {
      hmerge_vars <- c("re", "icu", "hospital", "female", "male", "follow_up_days")
      cmerge_vars <- c("re", "icu", "hospital", "female", "male", "children", "follow_up_days")
    }
    if (out=="rsp") {
      if (drop_cough == 1) {
        used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "shortness_of_breath"))
        used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "shortness_of_breath"))
      } else {
        used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "cough", "shortness_of_breath"))
        used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "cough", "shortness_of_breath"))
      }
      used_data_h$other_list <- 0
      used_data_c$other_list <- 0
    } else if (out=="cog") {
      used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "memory_problems"))
      used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "memory_problems"))
      used_data_h$other_list <- 0
      used_data_c$other_list <- 0
    } else if (out=="fat") {
      used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "fatigue"))
      used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "fatigue"))
      used_data_h$other_list <- 0
      used_data_c$other_list <- 0
    } else if (out=="any") {
      used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars))
      used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars))
    }
    
    #    used_data_h$study_id <- paste(used_data_h$study_id, used_data_h$pop)
    #    used_data_c$study_id <- paste(used_data_c$study_id, used_data_c$pop)
    
    
    if (pop == "all") {
      for (poppredict in c("children", "adults")) {
        ### ADJUST DATA
        if (poppredict == "children") {
          kids <- 1
        } else {
          kids <- 0
        }
        coeffs_h
        coeffs_c
        used_data_i_f <- used_data_h[female==1 | (female==0 & male==0)]
        used_data_i_m <- used_data_h[male==1 | (female==0 & male==0)]
        used_data_h_f <- used_data_h[female==1 | (female==0 & male==0)]
        used_data_h_m <- used_data_h[male==1 | (female==0 & male==0)]
        used_data_c_f <- used_data_c[female==1 | (female==0 & male==0)]
        used_data_c_m <- used_data_c[male==1 | (female==0 & male==0)]
        
        #          used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3]) + (used_data_i_f$children - kids) * as.numeric(coeffs_h[2,6])
        #          used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4]) + (used_data_i_m$children - kids) * as.numeric(coeffs_h[2,6])
        #          used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3]) + (used_data_h_f$children - kids) * as.numeric(coeffs_h[2,6])
        #          used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4]) + (used_data_h_m$children - kids) * as.numeric(coeffs_h[2,6])
        #          used_data_c_f$obs_adj <- used_data_c_f$obs - used_data_c_f$other_list * as.numeric(coeffs_c[2,5]) + (1-used_data_c_f$female) * as.numeric(coeffs_c[2,2]) + (used_data_c_f$children - kids) * as.numeric(coeffs_c[2,6])
        #          used_data_c_m$obs_adj <- used_data_c_m$obs - used_data_c_m$other_list * as.numeric(coeffs_c[2,5]) + (1-used_data_c_m$male) * as.numeric(coeffs_c[2,3]) + (used_data_c_m$children - kids) * as.numeric(coeffs_c[2,6])
        
        used_data_i_f$obs_adj <- used_data_i_f$obs
        used_data_i_m$obs_adj <- used_data_i_m$obs
        used_data_h_f$obs_adj <- used_data_h_f$obs
        used_data_h_m$obs_adj <- used_data_h_m$obs
        used_data_c_f$obs_adj <- used_data_c_f$obs
        used_data_c_m$obs_adj <- used_data_c_m$obs
        #        if (kids == 0) {
        #          used_data_i_f[, obs_adj := obs + (1 - icu) * as.numeric(coeffs_h[2,2]) + (1-female) * as.numeric(coeffs_h[2,3]) + (kids - children) * as.numeric(coeffs_h[2,6])]
        #          used_data_i_m[, obs_adj := obs + (1 - icu) * as.numeric(coeffs_h[2,2]) + (1-male) * as.numeric(coeffs_h[2,4]) + (kids - children) * as.numeric(coeffs_h[2,6])]
        #          used_data_h_f[, obs_adj := obs - icu * as.numeric(coeffs_h[2,2]) + (1-female) * as.numeric(coeffs_h[2,3]) + (kids - children) * as.numeric(coeffs_h[2,6])]
        #          used_data_h_m[, obs_adj := obs - icu * as.numeric(coeffs_h[2,2]) + (1-male) * as.numeric(coeffs_h[2,4]) + (kids - children) * as.numeric(coeffs_h[2,6])]
        #          used_data_c_f[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) + (1-female) * as.numeric(coeffs_c[2,2]) + (kids - children) * as.numeric(coeffs_c[2,5])]
        #          used_data_c_m[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) + (1-male) * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5])]
        #      } else if (kids == 1 & kid_estimate_sex_specific_h == 0 & hosp_kids == 1) {
        if (kid_estimate_sex_specific_h == 0 & hosp_kids == 1) {
          # adjust all kid data to Both sexes because we are not doing sex-specific child estimates anymore
          used_data_i_f[, obs_adj := obs + (1 - icu) * as.numeric(coeffs_h[2,2]) - female * as.numeric(coeffs_h[2,3]) - male * as.numeric(coeffs_h[2,4]) + (kids - children) * as.numeric(coeffs_h[2,6]) - other_list * as.numeric(coeffs_h[2,7])]
          used_data_i_m[, obs_adj := obs + (1 - icu) * as.numeric(coeffs_h[2,2]) - female * as.numeric(coeffs_h[2,3]) - male * as.numeric(coeffs_h[2,4]) + (kids - children) * as.numeric(coeffs_h[2,6]) - other_list * as.numeric(coeffs_h[2,7])]
          used_data_h_f[, obs_adj := obs - icu * as.numeric(coeffs_h[2,2]) - female * as.numeric(coeffs_h[2,3]) - male * as.numeric(coeffs_h[2,4]) + (kids - children) * as.numeric(coeffs_h[2,6]) - other_list * as.numeric(coeffs_h[2,7])]
          used_data_h_m[, obs_adj := obs - icu * as.numeric(coeffs_h[2,2]) - female * as.numeric(coeffs_h[2,3]) - male * as.numeric(coeffs_h[2,4]) + (kids - children) * as.numeric(coeffs_h[2,6]) - other_list * as.numeric(coeffs_h[2,7])]
          used_data_c_f[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5])]
          used_data_c_m[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5])]
        } else if (kid_estimate_sex_specific_h == 1 & hosp_kids == 1) {
          used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3]) + (kids - used_data_i_f$children) * as.numeric(coeffs_h[2,6]) - used_data_i_f$other_list * as.numeric(coeffs_h[2,7])
          used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4]) + (kids - used_data_i_m$children) * as.numeric(coeffs_h[2,6]) - used_data_i_m$other_list * as.numeric(coeffs_h[2,7])
          used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3]) + (kids - used_data_h_f$children) * as.numeric(coeffs_h[2,6]) - used_data_h_f$other_list * as.numeric(coeffs_h[2,7])
          used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4]) + (kids - used_data_h_m$children) * as.numeric(coeffs_h[2,6]) - used_data_h_m$other_list * as.numeric(coeffs_h[2,7])
          used_data_c_f$obs_adj <- used_data_c_f$obs - used_data_c_f$other_list * as.numeric(coeffs_c[2,6]) + (1-used_data_c_f$female) * as.numeric(coeffs_c[2,2]) + (kids - used_data_c_f$children) * as.numeric(coeffs_c[2,5])
          used_data_c_m$obs_adj <- used_data_c_m$obs - used_data_c_m$other_list * as.numeric(coeffs_c[2,6]) + (1-used_data_c_m$male) * as.numeric(coeffs_c[2,3]) + (kids - used_data_c_m$children) * as.numeric(coeffs_c[2,5])
        } else if (kid_estimate_sex_specific_h == 0 & hosp_kids == 0) {
          if (out == 'fat') {
            # adjust all kid data to Both sexes because we are not doing sex-specific child estimates anymore
            used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3]) - used_data_i_f$fatigue * as.numeric(coeffs_h[2,6]) - used_data_i_f$administrative * as.numeric(coeffs_h[2,7])
            used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4]) - used_data_i_m$fatigue * as.numeric(coeffs_h[2,6]) - used_data_i_m$administrative * as.numeric(coeffs_h[2,7])
            used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3]) - used_data_h_f$fatigue * as.numeric(coeffs_h[2,6]) - used_data_h_f$administrative * as.numeric(coeffs_h[2,7])
            used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4]) - used_data_h_m$fatigue * as.numeric(coeffs_h[2,6]) - used_data_h_m$administrative * as.numeric(coeffs_h[2,7])
            if (kids == 1) {
              used_data_c_f[, obs_adj := obs - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - fatigue * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
              used_data_c_m[, obs_adj := obs - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - fatigue * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
            } else {
              used_data_c_f[, obs_adj := obs + (1 - female) * as.numeric(coeffs_c[2,2]) + (kids - children) * as.numeric(coeffs_c[2,5]) - fatigue * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
              used_data_c_m[, obs_adj := obs + (1 - male) * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - fatigue * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
            }
          } else if (out == 'rsp') {
            if (drop_cough == 1) {
              used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3]) - used_data_i_f$shortness_of_breath * as.numeric(coeffs_h[2,6]) - used_data_i_f$administrative * as.numeric(coeffs_h[2,7])
              used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4]) - used_data_i_m$shortness_of_breath * as.numeric(coeffs_h[2,6]) - used_data_i_m$administrative * as.numeric(coeffs_h[2,7])
              used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3]) - used_data_h_f$shortness_of_breath * as.numeric(coeffs_h[2,6]) - used_data_h_f$administrative * as.numeric(coeffs_h[2,7])
              used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4]) - used_data_h_m$shortness_of_breath * as.numeric(coeffs_h[2,6]) - used_data_h_m$administrative * as.numeric(coeffs_h[2,7])
              if (kids == 1) {
                used_data_c_f[, obs_adj := obs - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - shortness_of_breath * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
                used_data_c_m[, obs_adj := obs - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - shortness_of_breath * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
              } else {
                used_data_c_f[, obs_adj := obs + (1 - female) * as.numeric(coeffs_c[2,2]) + (kids - children) * as.numeric(coeffs_c[2,5]) - shortness_of_breath * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
                used_data_c_m[, obs_adj := obs + (1 - male) * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - shortness_of_breath * as.numeric(coeffs_c[2,6]) - administrative * as.numeric(coeffs_c[2,7])]
              }
            } else {
              used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3]) - used_data_i_f$cough * as.numeric(coeffs_h[2,6]) - used_data_i_f$shortness_of_breath * as.numeric(coeffs_h[2,7]) - used_data_i_f$administrative * as.numeric(coeffs_h[2,8])
              used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4]) - used_data_i_m$cough * as.numeric(coeffs_h[2,6]) - used_data_i_m$shortness_of_breath * as.numeric(coeffs_h[2,7]) - used_data_i_m$administrative * as.numeric(coeffs_h[2,8])
              used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3]) - used_data_h_f$cough * as.numeric(coeffs_h[2,6]) - used_data_h_f$shortness_of_breath * as.numeric(coeffs_h[2,7]) - used_data_h_f$administrative * as.numeric(coeffs_h[2,8])
              used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4]) - used_data_h_m$cough * as.numeric(coeffs_h[2,6]) - used_data_h_m$shortness_of_breath * as.numeric(coeffs_h[2,7]) - used_data_h_m$administrative * as.numeric(coeffs_h[2,8])
              used_data_c_f[, obs_adj := obs  - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - cough * as.numeric(coeffs_c[2,6]) - shortness_of_breath * as.numeric(coeffs_c[2,7]) - administrative * as.numeric(coeffs_c[2,8])]
              used_data_c_m[, obs_adj := obs  - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - cough * as.numeric(coeffs_c[2,6]) - shortness_of_breath * as.numeric(coeffs_c[2,7]) - administrative * as.numeric(coeffs_c[2,8])]
            }
          } else if (out == 'cog') {
            used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3]) - used_data_i_f$memory_problems * as.numeric(coeffs_h[2,6])
            used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4]) - used_data_i_m$memory_problems * as.numeric(coeffs_h[2,6])
            used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3]) - used_data_h_f$memory_problems * as.numeric(coeffs_h[2,6])
            used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4]) - used_data_h_m$memory_problems * as.numeric(coeffs_h[2,6])
            if (kids == 1) {
              used_data_c_f[, obs_adj := obs - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - memory_problems * as.numeric(coeffs_c[2,6])]
              used_data_c_m[, obs_adj := obs - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - memory_problems * as.numeric(coeffs_c[2,6])]
            } else {
              used_data_c_f[, obs_adj := obs + (1 - female) * as.numeric(coeffs_c[2,2]) + (kids - children) * as.numeric(coeffs_c[2,5]) - memory_problems * as.numeric(coeffs_c[2,6])]
              used_data_c_m[, obs_adj := obs + (1 - male) * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5]) - memory_problems * as.numeric(coeffs_c[2,6])]
            }
          } else if (out == 'any') {
            used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_f$female) * as.numeric(coeffs_h[2,3]) - used_data_i_f$other_list * as.numeric(coeffs_h[2,6])
            used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h[2,2]) + (1-used_data_i_m$male) * as.numeric(coeffs_h[2,4]) - used_data_i_m$other_list * as.numeric(coeffs_h[2,6])
            used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_f$female) * as.numeric(coeffs_h[2,3]) - used_data_h_f$other_list * as.numeric(coeffs_h[2,6])
            used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h[2,2]) + (1-used_data_h_m$male) * as.numeric(coeffs_h[2,4]) - used_data_h_m$other_list * as.numeric(coeffs_h[2,6])
            if (kids == 1) {
              used_data_c_f[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5])]
              used_data_c_m[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) - female * as.numeric(coeffs_c[2,2]) - male * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5])]
            } else {
              used_data_c_f[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) + (1 - female) * as.numeric(coeffs_c[2,2]) + (kids - children) * as.numeric(coeffs_c[2,5])]
              used_data_c_m[, obs_adj := obs - other_list * as.numeric(coeffs_c[2,6]) + (1 - male) * as.numeric(coeffs_c[2,3]) + (kids - children) * as.numeric(coeffs_c[2,5])]
            }
          }
        }
        
        #used_data_c_m[study_id=='pa-COVID', c('mean', 'obs', 'obs_adj')]
        #used_data_c_m[study_id=='UK CIS', c('mean', 'obs', 'obs_adj')]
        
        
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
        if (hosp_kids == 0) {
          rr_summaries_h$children <- 0
          temp <- copy(rr_summaries_h)
          temp$children <- 1
          rr_summaries_h <- rbind(rr_summaries_h, temp)
        }
        rr_summaries <- rbind(rr_summaries_h, rr_summaries_c, fill = TRUE)
        rr_summaries <- rr_summaries[follow_up_days<=365]
        if (poppredict == 'children' & hosp_kids == 0) {
          rr_summariesF <- rr_summaries[((female==1 & (hospital==1 | icu==1)) | 
                                           (female==0 & male==0 & hospital==0 & icu==0)) & 
                                          children==kids]
          rr_summariesM <- rr_summaries[((male==1 & (hospital==1 | icu==1)) | 
                                           (female==0 & male==0 & hospital==0 & icu==0)) & 
                                          children==kids]
        } else if (poppredict == 'children' & hosp_kids == 1) {
          rr_summariesF <- rr_summaries[((female==0 & male==0 & (hospital==1 | icu==1)) | 
                                           (female==0 & male==0 & hospital==0 & icu==0)) & 
                                          children==kids]
          rr_summariesM <- rr_summaries[((female==0 & male==0 & (hospital==1 | icu==1)) | 
                                           (female==0 & male==0 & hospital==0 & icu==0)) & 
                                          children==kids]
        } else {
          rr_summariesF <- rr_summaries[female == 1 & children==kids]
          rr_summariesM <- rr_summaries[male == 1 & children==kids]
        }
        # used_data <- rbind(used_data_h, used_data_c, fill = TRUE)
        
        
        
        #  used_data$weight <- 1/(used_data$obs_se)
        used_data_i_f$weight <- 1/(10*used_data_i_f$se)
        used_data_i_f$weight[used_data_i_f$weight>8] <- 8
        used_data_i_f$weight[used_data_i_f$weight<2] <- 2
        used_data_i_m$weight <- 1/(10*used_data_i_m$se)
        used_data_i_m$weight[used_data_i_m$weight>8] <- 8
        used_data_i_m$weight[used_data_i_m$weight<2] <- 2
        used_data_h_f$weight <- 1/(10*used_data_h_f$se)
        used_data_h_f$weight[used_data_h_f$weight>8] <- 8
        used_data_h_f$weight[used_data_h_f$weight<2] <- 2
        used_data_h_m$weight <- 1/(10*used_data_h_m$se)
        used_data_h_m$weight[used_data_h_m$weight>8] <- 8
        used_data_h_m$weight[used_data_h_m$weight<2] <- 2
        used_data_c_f$weight <- 1/(20*used_data_c_f$se)
        used_data_c_f$weight[used_data_c_f$weight>8] <- 8
        used_data_c_f$weight[used_data_c_f$weight<2] <- 2
        used_data_c_m$weight <- 1/(20*used_data_c_m$se)
        used_data_c_m$weight[used_data_c_m$weight>8] <- 8
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
          ggtitle(paste(title, ", females", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          theme(legend.text=element_text(size=rel(0.7)), legend.title=element_text(size=0)) +
          guides(colour = guide_legend(reverse = TRUE)) +
          scale_colour_manual(values =c('blue'='blue', 'green'='green', 'red'='red'), labels = c('Community', 'Hospitalized', 'ICU'), order(c('ICU', 'Hospitalized', 'Community')))
        
        
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_",  poppredict, "F.pdf"), width = 8, height = 5)
        
        plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred, fill = "blue"))+
          geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, y=pred, color = "blue"), size=1) +
          geom_ribbon(data= rr_summariesM[hospital==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightgreen", alpha=.3) +
          geom_line(data=rr_summariesM[hospital==1], aes(x=follow_up_days, y=pred, color = "green"), size=1) +
          geom_ribbon(data= rr_summariesM[icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="pink", alpha=.3) +
          geom_line(data=rr_summariesM[icu==1], aes(x=follow_up_days, y=pred, color = "red"), size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, ", males", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          theme(legend.text=element_text(size=rel(0.7)), legend.title=element_text(size=0)) +
          guides(colour = guide_legend(reverse = TRUE)) +
          scale_colour_manual(values =c('blue'='blue', 'green'='green', 'red'='red'), labels = c('Community', 'Hospitalized', 'ICU'), order(c('ICU', 'Hospitalized', 'Community')))
        plot
        #pdf(paste0(folder,"FILEPATH", exp, "_", out, "_log.pdf"),width=10, height=7.5)
        ggsave(plot, filename=paste0(outputfolder,"FILEPOATH", version, "_", out, "_all_", poppredict, "M.pdf"), width = 8, height = 5)
        
        
        
        
        ##############################
        # Community
        rr_summariesF$pred_hi[rr_summariesF$pred_hi>1 & rr_summariesF$hospital==0 & rr_summariesF$icu==0] <- 1
        rr_summariesM$pred_hi[rr_summariesM$pred_hi>1 & rr_summariesF$hospital==0 & rr_summariesF$icu==0] <- 1
        plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among community, females", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data[w==1 & male!=1 & pop=="community"], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & male!=1 & pop=="community", study_id]), 
                     size=(used_data[w==1 & male!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
          geom_point(data=used_data[w ==0 & male!=1 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & male!=1 & pop=="community", study_id]), 
                     size=(used_data[w==0 & male!=1 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        
        
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_comm.pdf"), width = 8, height = 5)
        
        plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among community, males", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data[w==1 & female!=1 & pop=="community"], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & female!=1 & pop=="community", study_id]), 
                     size=(used_data[w==1 & female!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
          geom_point(data=used_data[w ==0 & female!=1 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & female!=1 & pop=="community", study_id]), 
                     size=(used_data[w==0 & female!=1 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        
        plot
        #pdf(paste0(folder,"FILEPATH", exp, "_", out, "_log.pdf"),width=10, height=7.5)
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_comm.pdf"), width = 8, height = 5)
        
        # ADJUSTED
        
        plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among community, females, adjusted values", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data_c_f[w==1 & male!=1,], aes(x=follow_up_days, y=obs_exp_adj, color=used_data_c_f[w ==1 & male!=1, study_id]), 
                     size=(used_data_c_f[w==1 & male!=1, weight]), shape=16, alpha=0.5) +
          geom_point(data=used_data[w ==0 & male!=1 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & male!=1 & pop=="community", study_id]), 
                     size=(used_data[w==0 & male!=1 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)

        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_comm_adj.pdf"), width = 8, height = 5)
        
        plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among community, males, adjusted values", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data_c_m[w==1 & female!=1,], aes(x=follow_up_days, y=obs_exp_adj, color=used_data_c_m[w ==1 & female!=1, study_id]), 
                     size=(used_data_c_m[w==1 & female!=1, weight]), shape=16, alpha=0.5) +
          geom_point(data=used_data[w ==0 & female!=1 & pop=="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==0 & female!=1 & pop=="community", study_id]), 
                     size=(used_data[w==0 & female!=1 & pop=="community", weight]), shape=1, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_comm_adj.pdf"), width = 8, height = 5)
        
        
        if ((hosp_kids == 1) | (hosp_kids == 0 & poppredict == 'adults')) {
        #####################################
        # Hospital
        
        plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among hospitalized, females", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data[w==1 & male!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, 
                                                                             color=used_data[w ==1 & male!=1 & pop!="community", study_id], 
                                                                             shape=used_data[w ==1 & male!=1 & pop!="community", pop]), 
                     size=(used_data[w==1 & male!=1 & pop!="community", weight]), alpha=0.5) +
          geom_point(data=used_data[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                             color=used_data[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                             shape=used_data[w ==0 & male!=1 & pop=="hospital", pop]), 
                     size=(used_data[w==0 & male!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                        color=used_data[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                        shape=used_data[w ==0 & male!=1 & pop=="ICU", pop]), 
                     size=(used_data[w==0 & male!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))

        #guides(fill=FALSE, shape=FALSE)
        #  theme(legend.text=element_text(size=rel(hosp_legendsize)), legend.title=element_text(size=rel(hosp_legendsize)), legend.key.size = unit(0.1, "lines"))
        
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_hosp.pdf"), width = 8, height = 5)
        
        plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among hospitalized, males", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data[w==1 & female!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, 
                                                                               color=used_data[w ==1 & female!=1 & pop!="community", study_id], 
                                                                               shape=used_data[w ==1 & female!=1 & pop!="community", pop]), 
                     size=(used_data[w==1 & female!=1 & pop!="community", weight]), alpha=0.5) +
          geom_point(data=used_data[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                               color=used_data[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                               shape=used_data[w ==0 & female!=1 & pop=="hospital", pop]), 
                     size=(used_data[w==0 & female!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                          color=used_data[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                          shape=used_data[w ==0 & female!=1 & pop=="ICU", pop]), 
                     size=(used_data[w==0 & female!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        
        plot
        #pdf(paste0(folder,"FILEPATH", exp, "_", out, "_log.pdf"),width=10, height=7.5)
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_hosp.pdf"), width = 8, height = 5)
        
        # ADJUSTED
        
        plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among hospitalized, females, adjusted values", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data_h_f[w==1 & male!=1,], aes(x=follow_up_days, y=obs_exp_adj,
                                                              color=used_data_h_f[w ==1 & male!=1, study_id], 
                                                              shape=used_data_h_f[w ==1 & male!=1, pop]), 
                     size=(used_data_h_f[w==1 & male!=1, weight]), alpha=0.5) +
          geom_point(data=used_data_h_f[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                                 color=used_data_h_f[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                                 shape=used_data_h_f[w ==0 & male!=1 & pop=="hospital", pop]), 
                     size=(used_data_h_f[w==0 & male!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data_h_f[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                            color=used_data_h_f[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                            shape=used_data_h_f[w ==0 & male!=1 & pop=="ICU", pop]), 
                     size=(used_data_h_f[w==0 & male!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_hosp_adj.pdf"), width = 8, height = 5)
        
        plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among hospitalized, males, adjusted values", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data_h_m[w==1 & female!=1,], aes(x=follow_up_days, y=obs_exp_adj, 
                                                                color=used_data_h_m[w ==1 & female!=1, study_id], 
                                                                shape=used_data_h_m[w ==1 & female!=1, pop]), 
                     size=(used_data_h_m[w==1 & female!=1, weight]), alpha=0.5) +
          geom_point(data=used_data_h_m[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                                   color=used_data_h_m[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                                   shape=used_data_h_m[w ==0 & female!=1 & pop=="hospital", pop]), 
                     size=(used_data_h_m[w==0 & female!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data_h_m[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                              color=used_data_h_m[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                              shape=used_data_h_m[w ==0 & female!=1 & pop=="ICU", pop]), 
                     size=(used_data_h_m[w==0 & female!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_hosp_adj.pdf"), width = 8, height = 5)
        
        
        
        ################################
        # ICU
        
        plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among ICU, females", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data[w==1 & male!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & male!=1 & pop!="community", study_id]), 
                     size=(used_data[w==1 & male!=1 & pop!="community", weight]), shape=16, alpha=0.5) +
          geom_point(data=used_data[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                             color=used_data[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                             shape=used_data[w ==0 & male!=1 & pop=="hospital", pop]), 
                     size=(used_data[w==0 & male!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                        color=used_data[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                        shape=used_data[w ==0 & male!=1 & pop=="ICU", pop]), 
                     size=(used_data[w==0 & male!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_icu.pdf"), width = 8, height = 5)
        
        plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among ICU, males", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data[w==1 & female!=1 & pop!="community",], aes(x=follow_up_days, y=obs_exp, color=used_data[w ==1 & female!=1 & pop!="community", study_id]), 
                     size=(used_data[w==1 & female!=1 & pop!="community", weight]), shape=16, alpha=0.5) +
          geom_point(data=used_data[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                               color=used_data[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                               shape=used_data[w ==0 & female!=1 & pop=="hospital", pop]), 
                     size=(used_data[w==0 & female!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                          color=used_data[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                          shape=used_data[w ==0 & female!=1 & pop=="ICU", pop]), 
                     size=(used_data[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        plot
        #pdf(paste0(folder,"FILEPATH", exp, "_", out, "_log.pdf"),width=10, height=7.5)
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_icu.pdf"), width = 8, height = 5)
        
        # ADJUSTED
        
        plot <- ggplot(data=rr_summariesF, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among ICU, females, adjusted values", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data_i_f[w==1 & male!=1,], aes(x=follow_up_days, y=obs_exp_adj, 
                                                              color=used_data_i_f[w ==1 & male!=1, study_id], 
                                                              shape=used_data_i_f[w ==1 & male!=1, pop]),
                     size=(used_data_i_f[w==1 & male!=1, weight]), alpha=0.5) +
          geom_point(data=used_data_i_f[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                                 color=used_data_i_f[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                                 shape=used_data_i_f[w ==0 & male!=1 & pop=="hospital", pop]), 
                     size=(used_data_i_f[w==0 & male!=1 & pop=="hospital", weight]), shape = 1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data_i_f[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                            color=used_data_i_f[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                            shape=used_data_i_f[w ==0 & male!=1 & pop=="ICU", pop]), 
                     size=(used_data_i_f[w==0 & male!=1 & pop=="ICU", weight]), shape = 2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        plot
        ggsave(plot, filename=paste0(outputfolder,"FIELPATH", version, "_", out, "_all_", poppredict, "F_icu_adj.pdf"), width = 8, height = 5)
        
        plot <- ggplot(data=rr_summariesM, aes(x=follow_up_days, y=pred), fill = "blue")+
          geom_ribbon(data= rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
          geom_line(data=rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_days, y=pred), color = "blue", size=1) +
          ylab("Proportion") +
          xlab("Follow up (days)") +
          ggtitle(paste(title, "among ICU, males, adjusted values", poppredict)) +
          theme_minimal() +
          scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
          scale_x_continuous(expand=c(0,10), breaks = seq(0,365,30)) + 
          theme(axis.line=element_line(colour="black")) +
          geom_point(data=used_data_i_m[w==1 & female!=1,], aes(x=follow_up_days, y=obs_exp_adj, 
                                                                color=used_data_i_m[w ==1 & female!=1, study_id],
                                                                shape=used_data_i_m[w ==1 & female!=1, pop]),
                     size=(used_data_i_m[w==1 & female!=1, weight]), alpha=0.5) +
          geom_point(data=used_data_i_m[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_days, y=obs_exp, 
                                                                                   color=used_data_i_m[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                                   shape=used_data_i_m[w ==0 & female!=1 & pop=="hospital", pop]), 
                     size=(used_data_i_m[w==0 & female!=1 & pop=="hospital", weight]), shape = 1, alpha=1, show.legend = FALSE) +
          geom_point(data=used_data_i_m[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_days, y=obs_exp, 
                                                                              color=used_data_i_m[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                              shape=used_data_i_m[w ==0 & female!=1 & pop=="ICU", pop]), 
                     size=(used_data_i_m[w==0 & female!=1 & pop=="ICU", weight]), shape = 2, alpha=1, show.legend = FALSE) +
          scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
        plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
        plot <- plot + guides(color = guide_legend(ncol = 2))
        plot
        ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_icu_adj.pdf"), width = 8, height = 5)
        
        }
      }
    }
  }
  
}



























######################################################################################
#   run model with all data of overlaps among any long COVID
######################################################################################

df <- dataset[outcome %in% c("mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp")]
write.csv(df, paste0(outputfolder, "prepped_data_", datadate, "_severities_for_gather.csv"))

if (version >= 53) {
  df <- dataset[outcome %in% c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp")]
  df <- df[sample_size_envelope>10 & study_id!="PRA"]
  df <- df[study_id=="PRA", mean := mean_adjusted]
  df$log_mean <- log(df$mean)
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
  write.csv(df, paste0(outputfolder, "prepped_data_", datadate, "_overlap_for_gather.csv"))
  
  outcomes <- c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp", "mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp")
} else {
  outcomes <- c("mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp")
}


for(out in outcomes) {
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
  model_dir <- paste0(out, "FILEPATH")
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
  #py_save_object(object = model_no, filename = paste0(folder, "FILEPATH", model_dir_no, "mod1.pkl"), pickle = "dill")
  
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
  #    used_data$xval[used_data$study_id=="Iran ICC hosp/ICU"] <- 2
  #    used_data$xval[used_data$study_id=="Sechenov StopCOVID hosp/ICU"] <- 3
  #    used_data$xval[used_data$study_id=="Sweden PronMed ICU hosp/ICU"] <- 4
  
  used_data$pop[used_data$hospital_or_icu==1] <- "hosp/ICU"
  used_data$pop[used_data$hospital_or_icu==0] <- "community"
  #used_data$study_id <- paste(used_data$study_id, used_data$pop)
  used_data$weight <- 0.3*(1/used_data$se)
  
  used_data$obs_exp <- exp(used_data$obs) / (1 + exp(used_data$obs))
  used_data <- used_data[order(obs_exp)]
  df <- df[order(mean)]
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
  #pdf(paste0(folder,"FILEPATH", exp, "_", out, "_log.pdf"),width=10, height=7.5)
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_", filename, ".pdf"), width = 6, height = 5)
  
  
  
}


















if (version < 53) {
  
  ######################################################################################
  #   run model with all data of overlaps among any long COVID by follow-up time
  ######################################################################################
  
  df <- dataset[outcome %in% c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp")]
  df <- df[sample_size_envelope>10 & study_id!="PRA"]
  df <- df[study_id=="PRA", mean := mean_adjusted]
  df$log_mean <- log(df$mean)
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
  write.csv(df, paste0(outputfolder, "prepped_data_", datadate, "_overlap_for_gather.csv"))
  
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
    model_dir <- paste0(out, "FILEPATH")
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
          LinearCovModel("follow_up_days", use_re = FALSE)
        ),
        inlier_pct = 1)
    }
    
    # fit model
    model$fit_model(inner_print_level = 5L, inner_max_iter = n_samples)
    
    (coeffs <- rbind(model$cov_names, model$beta_soln))
    write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs.csv"))
    
    # save model object
    py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1.pkl"), pickle = "dill")
    #py_save_object(object = model_no, filename = paste0(folder, "FILEPATH", model_dir_no, "mod1.pkl"), pickle = "dill")
    
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
    #    draws_save <- draws_save[follow_up_days==0]
    #    draws_save$follow_up_days <- NULL
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
    
    
    
    setnames(df, "study_id", "re")
    df_se <- df[,c("re", "hospital_or_icu", "follow_up_days", "mean", "standard_error")]
    setnames(df_se, "standard_error", "se")
    #df_se$study_id <- NULL
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
      theme(legend.text=element_text(size=rel(0.6)))
    plot
    ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_", filename, ".pdf"), width = 6, height = 5)
    
    
    
  }
  
}



