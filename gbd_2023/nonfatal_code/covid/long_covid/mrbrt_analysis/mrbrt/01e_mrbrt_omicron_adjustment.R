#--------------------------------------------------------------
# Name: NAME (USERNAME)
# Date: 1 October 2024
# Project: GBD nonfatal COVID
# Purpose: estimate adjustment of long COVID risk for Omicron period vs earlier variants
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
# setwd("FILEPATH")

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- '~/'
} else {
  j_root <- 'FILEPATH/'
  h_root <- 'FILEPATH/'
}


# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)

library(reticulate)
library(msm)
library(tidyr)
library(dplyr)

Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")


folder <- "FILEPATH"
outputfolder <- "FILEPATH"
pipelinefolder <- 'FILEPATH'

cvs <- list("hospital", "icu", "female", "male", "follow_up_days", "other_list")

datadate <- '100724'

# "log" for log-linear models, "logit" for logit-linear models
shape <- 'logit'
n_samples <- 1000L
max_draw <- n_samples - 1
min_cv <- 0.1
inlier <- 1


# function to change size of legend
addSmallLegend <- function(myPlot, pointSize = 0.5, titleSize = 0, textSize = 3, spaceLegend = 0.1) {
  myPlot +
    guides(fill=FALSE) + guides(shape = guide_legend(override.aes = list(size = pointSize)),
                                color = guide_legend(override.aes = list(size = pointSize))) +
    theme(legend.title = element_text(size = titleSize),
          legend.text  = element_text(size = textSize),
          legend.key.size = unit(spaceLegend, "lines"))
}


################################################################
# READ DATA
################################################################

dataset_orig <- fread(paste0(outputfolder, "prepped_data_", datadate, ".csv"))
dataset_omicron <- fread(paste0(outputfolder, "prepped_data_omicron_", datadate, ".csv"))
dataset_omicron[study_id == 'Zurich ZSAC prosp - Omicron', study_id := 'Zurich ZSAC prosp']
dataset_omicron[study_id == 'Zurich ZSAC retro - Omicron', study_id := 'Zurich ZSAC retro']
dataset_orig[study_id == 'INSPIRE study', study_id := 'INSPIRE']
dataset_omicron[study_id == 'INSPIRE study', study_id := 'INSPIRE']
dataset_omicron[study_id == 'Arjun et al (2)', study_id := 'Arjun et al']
dataset_orig[study_id == 'Arjun et al (1)', study_id := 'Arjun et al']
dataset_omicron[study_id == 'UK Virus Watch' & follow_up_days == 99, follow_up_days := 110]
dataset_orig[study_id == 'UK Virus Watch' & follow_up_days == 107, follow_up_days := 110]
dataset_omicron <- dataset_omicron[study_id != "UK Virus Watch" | (year_start %in% c(2021, 2022) & 
                                                                     comparison_group == "without eq5d" & follow_up_days %in% c(110)
                                                                   & sample_population == "community")]
dataset_orig <- dataset_orig[study_id != "UK Virus Watch" | (year_start %in% c(2021, 2022) & 
                                                                     comparison_group == "without eq5d" & follow_up_days %in% c(110)
                                                                   & sample_population == "community")]
dataset_omicron[study_id == 'Arjun et al (2)', study_id := 'Arjun et al']
dataset_omicron <- dataset_omicron[study_id != "INSPIRE"]
sort(unique(dataset_omicron$study_id))
sort(unique(dataset_orig$study_id))


########################################################
# random effect = study + follow-up time
dataset_orig[, re := paste(study_id, follow_up_days)]
dataset_omicron[, re := paste(study_id, follow_up_days)]

(omicron_studies <- sort(unique(dataset_omicron$re)))
dataset_orig <- dataset_orig[re %in% omicron_studies]
(preomicron_studies <- sort(unique(dataset_orig$re)))
dataset_omicron <- dataset_omicron[re %in% preomicron_studies]
table(dataset_orig$re, dataset_orig$variant)
table(dataset_omicron$symptom_cluster, dataset_omicron$variant)

dataset <- rbind(dataset_orig, dataset_omicron, fill=TRUE)
dataset[study_id == "Arslan et al", follow_up_days := 100]
dataset <- dataset[outcome %in% c('any', 'any_main', 'any_new', 'cog', 'fat', 'rsp')]
dataset <- dataset[sex == "Both"]
table(dataset$outcome, dataset$variant)
table(dataset$outcome, dataset$study_id, dataset$variant)
table(dataset_omicron$re, dataset_omicron$outcome)
table(dataset_orig$re, dataset_orig$outcome)
table(dataset$re, dataset$outcome)
dataset[variant != "Omicron", variant := "Pre-Omicron"]

dim(dataset)
dataset <- dataset[!is.na(standard_error)]
dataset <- dataset[is_outlier!=1]
dataset <- dataset[follow_up_days < 350 | study_id %in% c("Veterans Affairs", "Arslan et al")]
dataset[, re := paste0(study_id, follow_up_days)]
dim(dataset)



######################################################################################
#   run model with all data with multiple follow up times, to get duration
#      and beta on follow_up_days to use as prior for cluster-specific models
######################################################################################

# add offset and/or meas_noise to symptom cluster data
for (i in c('any', 'any_main', 'any_new', 'fat', 'rsp', 'cog')) {
  print(paste(i, "offset: ", add_offset <- 0.01 * median(dataset$mean[dataset$outcome == i & dataset$mean!=0])))
  dataset[outcome == i, mean := (mean + add_offset)]
  dataset[outcome == i, offset := add_offset]
}

min(dataset$mean)
max(dataset$mean)

if (min_cv > 0) {
  dataset$cv <- dataset$standard_error / dataset$mean
  dataset[cv < min_cv, standard_error := mean * min_cv]
  dataset$cv <- NULL
}

if (shape == "log") {
  logvals <- cw$utils$linear_to_log(mean = array(dataset$mean), sd = array(dataset$standard_error))
  dataset$model_mean <- log(dataset$mean)
  logse <- logvals[2]
  dataset$model_se <- logse
} else if (shape == "logit") {
  dataset[mean >= 1, mean := 0.999]
  logitvals <- cw$utils$linear_to_logit(mean = array(dataset$mean), sd = array(dataset$standard_error))
  logitmean <- logitvals[1]
  logitse <- logitvals[2]
  dataset$model_mean <- logitmean
  dataset$model_se <- logitse
}

dataset <- dataset[!is.na(model_se)]
fwrite(dataset, paste0(outputfolder, "prepped_data_cluster/prepped_data_", datadate, "_symptoms_omicron_for_gather.csv"))
model_dir <- paste0("omicron_adjustment/")
dir.create(paste0(outputfolder, "prepped_data_cluster/", model_dir))

out <- 'any'

  message(paste0("working on ", out))
  
  df <- dataset[outcome == out]
  table(df$re, df$variant)
  table(df$study_id, df$variant)
  offset <- median(df$offset[df$outcome == out])
  
  df_orig <- copy(df)
  df <- copy(df_orig)
  
  df[variant == "Omicron", omicron_cv := 1]
  df[variant != "Omicron", omicron_cv := 0]
  
  df <- df[, .(title, study_id, sample_size, sex, outcome, mean, standard_error,
               follow_up_days, sample_population, sample_age_group,
               age_start, age_end, omicron_cv, lit_nonlit,
               variant, model_mean, model_se, re)]
  
  #############################
  mr_df <- mr$MRData()
  mr_df$load_df(
    data = df, col_obs = "model_mean", col_obs_se = "model_se",
    col_covs = list("omicron_cv"), col_study_id = "re")
  
  model <- mr$MRBRT(
    data = mr_df,
    cov_models =list(
      mr$LinearCovModel("intercept", use_re = TRUE),
      mr$LinearCovModel("omicron_cv", use_re = FALSE)
    ),
    inlier_pct = 1)
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  samples <- model$sample_soln(sample_size = n_samples)
  (beta <- mean(samples[[1]][,2]))
  (beta_sd <- sd(samples[[1]][,2]))
  
  coeffs <- data.frame(t(model$beta_soln))
  colnames(coeffs) <- model$cov_names
  coeffs$omicron_cv_sd <- beta_sd
  (coeffs)
  (exp(coeffs$omicron_cv))
  
  fwrite(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_omicron_beta_", out, ".csv"))
  
  draws <- data.table(samples[[1]][,2])
  draws <- exp(draws)
  setnames(draws, c('V1'), c('omicron_adjustment'))
  draws[, draw := c(0:max_draw)]
  fwrite(draws, paste0(outputfolder, "prepped_data_cluster/", model_dir, "draws_omicron_adjustment_", out, ".csv"))
  
  if (out == 'any') {
    fwrite(draws, paste0(pipelinefolder, "draws_omicron_adjustment_", out, ".csv"))
  }
  ############################
  df[, follow_up_months := (follow_up_days / (365/12))]
  (ymax <- round(max(df$mean) + 0.1, 1))
  plot <- ggplot(data=df, aes(x=follow_up_months, y=mean), fill = "blue")+
    ylab("Proportion") +
    xlab("Follow up (months)") +
    ggtitle(paste("Pre-Omicron vs Omicron proportion", out)) +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,ymax,0.1), limits=c(0,ymax)) +
    scale_x_continuous(expand=c(0,0.05), breaks = seq(0,12,2), limits = c(0, 12)) +
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=df[omicron_cv == 0], aes(x=follow_up_months, y=mean,
                                             shape=df[omicron_cv == 0, variant],
                                             color=df[omicron_cv == 0, study_id])) +
    geom_point(data=df[omicron_cv == 1], aes(x=follow_up_months, y=mean,
                                             shape=df[omicron_cv == 1, variant],
                                             color=df[omicron_cv == 1, study_id])) +
    scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
  plot <- plot + guides(color = guide_legend(ncol = 1))
  plot
  ggsave(plot, filename=paste0(outputfolder,"prepped_data_cluster/", model_dir, "Omicron_adjustment_data_", out, ".pdf"), width = 8, height = 5)
  
  


  rr_summaries <- copy(data.table(coeffs))
  rr_summaries[, pred := exp(omicron_cv)]
  rr_summaries[, pred_lo := quantile(draws$omicron_adjustment, 0.025)]
  rr_summaries[, pred_hi := quantile(draws$omicron_adjustment, 0.975)]
  rr_summaries <- rr_summaries[pred_lo<0, pred_lo := 0]
  rr_summaries[, study_id_ref := " ESTIMATE"]
  
  
  
  
  
  delta_transform <- function(mean, sd, transformation) {
    
    if (transformation == "linear_to_log") f <- cw$utils$linear_to_log
    if (transformation == "log_to_linear") f <- cw$utils$log_to_linear
    if (transformation == "linear_to_logit") f <- cw$utils$linear_to_logit
    if (transformation == "logit_to_linear") f <- cw$utils$logit_to_linear
    
    out <- do.call("cbind", f(mean = array(mean), sd = array(sd)))
    colnames(out) <- paste0(c("mean", "sd"), "_", strsplit(transformation, "_")[[1]][3])
    return(out)
  }
  
  # Calculate differences between random variables
  # calculates means and SDs for differences between random variables,
  # ensuring that the alternative defintion/method is in the numerator: log(alt/ref) = log(alt) - log(ref)
  calculate_diff <- function(df, alt_mean, alt_sd, ref_mean, ref_sd) {
    df <- as.data.frame(df)
    out <- data.frame(
      diff_mean =  df[, alt_mean] - df[, ref_mean],
      diff_sd = sqrt(df[, alt_sd]^2 + df[, ref_sd]^2)
    )
    return(out)
  }

  
  df_orig <- copy(df)
  df <- copy(df_orig)
  
  # calculate SE pre-adjustments
  df[, sample_pop_short := substr(sample_population, 1, 5)]

  method_var <- "variant"
  gold_def <- "Pre-Omicron"
  keep_vars <- c(
    "variant", "mean", "standard_error", "study_id",
    "follow_up_days", "sample_pop_short", "outcome"
  )
  df <- df[follow_up_days < 350 | study_id %in% c("Veterans Affairs", "Arslan et al")]
  
  # Original
  df_matched <- do.call("rbind", lapply(unique(df$outcome), function(i) {
    
    dat_i <- filter(df, outcome == i) %>%
      mutate(dorm = get(method_var))
    
    keep_vars <- c("dorm", keep_vars)
    row_ids <- expand.grid(idx1 = 1:nrow(dat_i), idx2 = 1:nrow(dat_i))
    
    do.call("rbind", lapply(1:nrow(row_ids), function(j) {
      dat_j <- row_ids[j, ]
      dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, ..keep_vars]
      dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, ..keep_vars]
      filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
    })) %>%
      mutate(id = i) %>%
      select(-idx1, -idx2)
  }))
  
  setDT(df_matched)
  
  df_matched <- df_matched[study_id_alt == study_id_ref &
                             (follow_up_days_alt == follow_up_days_ref) &
                             (sample_pop_short_alt == sample_pop_short_ref)]
  
  # delta_transform() deprecated, switch to using cw$utils$linear_to_logit instead
  # values remain the same but formatting now different, have to construct data.frame
  # ourselves
  dat_diff <- data.frame(
    mean_alt = cw$utils$linear_to_log(
      mean = array(df_matched$mean_alt),
      sd = array(df_matched$standard_error_alt)
    )[[1]],
    mean_se_alt = cw$utils$linear_to_log(
      mean = array(df_matched$mean_alt),
      sd = array(df_matched$standard_error_alt)
    )[[2]],
    mean_ref = cw$utils$linear_to_log(
      mean = array(df_matched$mean_ref),
      sd = array(df_matched$standard_error_ref)
    )[[1]],
    mean_se_ref = cw$utils$linear_to_log(
      mean = array(df_matched$mean_ref),
      sd = array(df_matched$standard_error_ref)
    )[[2]]
  )
  
  df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
    df = dat_diff,
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref"
  )
  
  used_data <- data.frame(
    obs = cw$utils$log_to_linear(
      mean = array(df_matched$logit_diff),
      sd = array(df_matched$logit_diff_se)
    )[[1]],
    obs_se = cw$utils$log_to_linear(
      mean = array(df_matched$logit_diff),
      sd = array(df_matched$logit_diff_se)
    )[[2]],
    df_matched
  )
  used_data <- data.table(used_data)
  
  used_data[, obs_lo := obs - 1.96 * obs_se]
  used_data[, obs_hi := obs + 1.96 * obs_se]
  
  used_data$weight <- 1/(15*used_data$obs_se)

  
  plot <- ggplot(data=rr_summaries, aes(x=study_id_ref, y=pred, ymin=pred_lo, ymax=pred_hi)) +
    geom_pointrange(color="blue") + 
    geom_pointrange(data=used_data, aes(x=study_id_ref, y=obs, ymin=obs_lo, ymax=obs_hi)) + 
    coord_flip() +
    ylab("Ratio Omicron risk/Pre-Omicron risk") +
    xlab("Study") +
    ggtitle("Risk of long COVID among Omicron vs pre-Omicron") +
    theme_minimal() +
    theme(axis.line=element_line(colour="black")) +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(-0.5,2.5,0.25), limits=c(-0.5,2.5)) + 
    guides(fill=FALSE)
  
  plot
  
  ggsave(plot, filename=paste0(outputfolder,"prepped_data_cluster/", model_dir, "Omicron_adjustment_forest_", out, ".pdf"), width = 8, height = 5)
  
















## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 00a_crosswalk_proportions.R
## Description: Crosswalking long covid symptom cluster proportion data.
##    Reference definition: Proportion due to covid-19, accounting for pre-covid
##                          health status
##    Alternative definition: Proportion w/ each symptom cluster regardless of
##                          pre-covid health status
## Inputs: Long covid symptom cluster data, w/ and w/o pre-covid health status
## Outputs: Long covid symptom cluster data crosswalked to reference def,
##          betas
## Contributors: NAME, NAME, NAME
## Date 3/8/2021
## --------------------------------------------------------------------- ----
## stamp: adding germany data and UW data (3/22/2021)
## stamp: adding the full long term cohort data (4/8/2021)

## Environment Prep ---------------------------------------------------- ----
#rm(list = ls(all.names = T))

library(openxlsx)
library(dplyr)
library(data.table)
library(reticulate)

Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
use_python("FILEPATH")
cw <- import("crosswalk")

output_path <- "FILEPATH"

# set.seed(123)
# data for the meta-regression
# -- in a real analysis, you'd get this dataset by
#    creating matched pairs of alternative/reference observations,
#    then using delta_transform() and calculate_diff() to get
#    log(alt)-log(ref) or logit(alt)-logit(ref) as your dependent variable

# Crosswalk functions
# Delta method approximation
# uses the delta method approximation to transform random variables to/from log/logit space
delta_transform <- function(mean, sd, transformation) {
  
  if (transformation == "linear_to_log") f <- cw$utils$linear_to_log
  if (transformation == "log_to_linear") f <- cw$utils$log_to_linear
  if (transformation == "linear_to_logit") f <- cw$utils$linear_to_logit
  if (transformation == "logit_to_linear") f <- cw$utils$logit_to_linear
  
  out <- do.call("cbind", f(mean = array(mean), sd = array(sd)))
  colnames(out) <- paste0(c("mean", "sd"), "_", strsplit(transformation, "_")[[1]][3])
  return(out)
}

# Calculate differences between random variables
# calculates means and SDs for differences between random variables,
# ensuring that the alternative definition/method is in the numerator: log(alt/ref) = log(alt) - log(ref)
calculate_diff <- function(df, alt_mean, alt_sd, ref_mean, ref_sd) {
  df <- as.data.frame(df)
  out <- data.frame(
    diff_mean =  df[, alt_mean] - df[, ref_mean],
    diff_sd = sqrt(df[, alt_sd]^2 + df[, ref_sd]^2)
  )
  return(out)
}
calculate_ratio <- function(df, alt_mean, alt_sd, ref_mean, ref_sd) {
  df <- as.data.frame(df)
  out <- data.frame(
    diff_mean =  df[, alt_mean] / df[, ref_mean],
    diff_sd = sqrt(df[, alt_sd]^2 + df[, ref_sd]^2)
  )
  return(out)
}

df_orig <- copy(df)
df <- copy(df_orig)

# calculate SE pre-adjustments
df[, cases := as.numeric(cases)]
df[, prev := mean]
df[, prev_se := standard_error]

df <- df[, .(title, study_id, sample_size, sex, outcome, mean, standard_error,
                 follow_up_days, sample_population, sample_age_group,
                 age_start, age_end, omicron_cv, lit_nonlit, prev, prev_se, 
                 source, Omicron)]
df[, sample_pop_short := substr(sample_population, 1, 5)]

setnames(df, "mean", "measurement")
setnames(df, "sample_size", "N")

  method_var <- "Omicron"
  gold_def <- "Pre-Omicron"
  keep_vars <- c(
    "Omicron", "prev", "prev_se", "study_id",
    "follow_up_days", "sample_pop_short", "outcome"
  )
  df <- df[follow_up_days < 350]
  
  # Original
  df_matched <- do.call("rbind", lapply(unique(df$outcome), function(i) {
    
    dat_i <- filter(df, outcome == i) %>%
      mutate(dorm = get(method_var))
    
    keep_vars <- c("dorm", keep_vars)
    row_ids <- expand.grid(idx1 = 1:nrow(dat_i), idx2 = 1:nrow(dat_i))
    
    do.call("rbind", lapply(1:nrow(row_ids), function(j) {
      dat_j <- row_ids[j, ]
      dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, ..keep_vars]
      dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, ..keep_vars]
      filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
    })) %>%
      mutate(id = i) %>%
      select(-idx1, -idx2)
  }))
  
  setDT(df_matched)
  
  df_matched <- df_matched[study_id_alt == study_id_ref &
                             (follow_up_days_alt == follow_up_days_ref) &
                             (sample_pop_short_alt == sample_pop_short_ref)]
  
  # delta_transform() deprecated, switch to using cw$utils$linear_to_logit instead
  # values remain the same but formatting now different, have to construct data.frame
  # ourselves
  dat_diff <- data.frame(
    mean_alt = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_alt),
      sd = array(df_matched$prev_se_alt)
    )[[1]],
    mean_se_alt = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_alt),
      sd = array(df_matched$prev_se_alt)
    )[[2]],
    mean_ref = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_ref),
      sd = array(df_matched$prev_se_ref)
    )[[1]],
    mean_se_ref = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_ref),
      sd = array(df_matched$prev_se_ref)
    )[[2]]
  )
  
  df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
    df = dat_diff,
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref"
  )
  
  write.csv(df_matched, file.path(output_path, paste0("FILEPATH", datadate, ".csv")))
  
  df_matched[, re := paste0(study_id_alt, follow_up_days_alt)]
  # format data for meta-regression; pass in data.frame and variable names
  dat1 <- cw$CWData(
    df = df_matched,
    obs = "logit_diff", # matched differences in logit space
    obs_se = "logit_diff_se", # SE of matched differences in logit space
    alt_dorms = "dorm_alt", # var for the alternative def/method
    ref_dorms = "dorm_ref", # var for the reference def/method
    covs = list("Omicron_ref"), # list of (potential) covariate columns
    study_id = "re" # var for random intercepts; i.e. (1|study_id)
  )
  
  fit1 <- cw$CWModel(
    cwdata = dat1, # result of CWData() function call
    obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
    cov_models = list( # specify covariate details
      cw$CovModel("intercept")
    ),
    gold_dorm = "Pre-Omicron" # level of 'ref_dorms' that's the gold standard
  )
  
  # Additional step with reticulate method, fit() must be called after
  # setting up CWModel
  fit1$fit()
  
  fit1$beta
  fit1$beta_sd
  
  # Save out fit1 beta and beta_sd by condition
    betas <- data.frame(
        "beta" = fit1$beta,
        "beta_sd" = fit1$beta_sd
    )

#write.csv(final_all, file.path(output_path, paste0("GBD2023/omicron_adjustment_", datadate, ".csv")), row.names = F)







df[Omicron == "Omicron", omicron_cv := 1]
df[Omicron == "Pre-Omicron", omicron_cv := 0]

#############################
mr_df <- mr$MRData()
mr_df$load_df(
  data = df, col_obs = "model_mean", col_obs_se = "model_se",
  col_covs = list("omicron_cv"), col_study_id = "re")

model <- mr$MRBRT(
  data = mr_df,
  cov_models =list(
    mr$LinearCovModel("intercept", use_re = TRUE),
    mr$LinearCovModel("omicron_cv", use_re = FALSE)
  ),
  inlier_pct = 1)

# fit model
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

samples <- fit1$sample_soln(sample_size = n_samples)
(beta <- mean(samples[[1]][,2]))
(beta_sd <- sd(samples[[1]][,2]))

coeffs <- data.frame(t(model$beta_soln))
colnames(coeffs) <- model$cov_names
coeffs$omicron_cv_sd <- beta_sd
(coeffs)
(exp(coeffs$omicron_cv))

fwrite(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_omicron_beta_", out, ".csv"))

draws <- data.table(samples[[1]][,2])
draws <- exp(draws)
setnames(draws, c('V1'), c('omicron_adjustment'))
draws[, draw := c(0:max_draw)]
fwrite(draws, paste0(outputfolder, "prepped_data_cluster/", model_dir, "draws_omicron_adjustment_", out, ".csv"))

if (out == 'any') {
  fwrite(draws, paste0(pipelinefolder, "draws_omicron_adjustment_", out, ".csv"))
}
############################
df[, follow_up_months := (follow_up_days / (365/12))]
(ymax <- round(max(df$mean) + 0.1, 1))
plot <- ggplot(data=df, aes(x=follow_up_months, y=mean), fill = "blue")+
  ylab("Proportion") +
  xlab("Follow up (months)") +
  ggtitle(paste("Pre-Omicron vs Omicron proportion", out)) +
  theme_minimal() +
  scale_y_continuous(expand=c(0,0.02), breaks = seq(0,ymax,0.1), limits=c(0,ymax)) +
  scale_x_continuous(expand=c(0,0.05), breaks = seq(0,12,2), limits = c(0, 12)) +
  theme(axis.line=element_line(colour="black")) +
  geom_point(data=df[omicron_cv == 0], aes(x=follow_up_months, y=mean,
                                           color=df[omicron_cv == 0, Omicron],
                                           shape=df[omicron_cv == 0, study_id])) +
  geom_point(data=df[omicron_cv == 1], aes(x=follow_up_months, y=mean,
                                           color=df[omicron_cv == 1, Omicron],
                                           shape=df[omicron_cv == 1, study_id])) +
  scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
plot <- plot + guides(color = guide_legend(ncol = 1))
plot
ggsave(plot, filename=paste0(outputfolder,"prepped_data_cluster/", model_dir, "Omicron_adjustment_data_", out, ".pdf"), width = 8, height = 5)



