#--------------------------------------------------------------
# Name: QWR, adapted from swulf from GBD 2020
# Date: Feb. 10, 2021
# Project: KD RR Evidence Score + Pub Bias
# Purpose: Run MRBRT models for all gout risk-outcome pairs, dichotomous!
#--------------------------------------------------------------

# Setup -------------------------------------------------------
rm(list = ls())
setwd("/ihme/epi/ckd/ckd_code/kd") # change the dir

# map drives
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "~/"
} else {
  j_root <- "J:/"
  h_root <- "H:/"
}

# libs
library(reticulate, lib.loc = "/ihme/homes/qwr/package_lib")
library(data.table)
library(mrbrt001, lib.loc = '/ihme/code/mscm/R/packages/')
library(ggplot2)
library(openxlsx)
library(dplyr)
library(plyr)
source("/ihme/code/qwr/escore/evidence_score_pipeline/src/utils/dichotomous_functions.R")
# Settings -------------------------------------------------------------
folder <- "/ihme/code/qwr/ckd_qwr/evidence_score_pipeline"
cvs <- ("intercept")
exp <- "ckd3_5" #ckd3_5 and ckd
out <- "gout"
ver <- "fisher_age_group"

# set directories
data_dir <- "/home/j/WORK/12_bundle/ckd/extractions/kd/gout/" #old but used since we have no gout 
mrbrt_output_dir<-paste0(folder,"/mrbrt_model_outputs/v",ver,"/",exp,"_", out,"/")
summaries <- paste0(folder,"/summary/v",ver,"/")
print(mrbrt_output_dir)
print(summaries)
if(!dir.exists(mrbrt_output_dir)) dir.create(mrbrt_output_dir, recursive = TRUE)
if(!dir.exists(summaries)) dir.create(summaries, recursive = TRUE)

# data
data <- read.xlsx(paste0(data_dir, "extraction_kd_gout.xlsx"))
glimpse(data)
data$to_analyze[data$to_analyze=="ckd3-5"] <- "ckd3_5"
glimpse(data)

# restrict data to relevant rows of exposure (ckd or ckd3-5)
df <- data[data$to_analyze==exp,]

# Clean data before processing --------------------------------------

df$cov_representativeness <- NA
df$cov_exposure_quality <- NA
df$cov_outcome_quality <- NA
df$cov_confounder_quality <- NA
df$cov_reverse_causation <- NA
df$cov_selection_bias <- NA
df$ckd3 <- 0
df$ckd3[df$cohort_exposed_def=="eGFR 30-59"] <- 1
df$ckd4 <- 0
df$ckd4[df$cohort_exposed_def=="eGFR 15-29"] <- 1
#df$cov_selection_bias[df$loss_to_follow_up>=0.85 & df$loss_to_follow_up<=0.95] <- 1
#df$cov_selection_bias[df$loss_to_follow_up<0.85] <- 2
df$notes_exposure_def <- "classified according to GFR cutoff"
df$notes_outcome_def <- ""
df$notes_general <- ""

# graphing function
add_ui <- function(dat, x_var, lo_var, hi_var, color = "darkblue", opacity = 0.2) {
  polygon(
    x = c(dat[, x_var], rev(dat[, x_var])),
    y = c(dat[, lo_var], rev(dat[, hi_var])),
    col = adjustcolor(col = color, alpha.f = opacity), border = FALSE
  )
}

# Run MRBRT ----------------------------------------------------------
message(paste0("working on ", exp, " : ", out))

repl_python() # need R version 3.6 to use this! check your "version"
exit
mr_df <- MRData()

if(cvs[[1]]=="intercept") {
  mr_df$load_df(
    data = df, col_obs = "log_effect_size", col_obs_se = "se",
    col_study_id = "nid")
  model <- MRBRT(
    data = mr_df,
    cov_models =list(LinearCovModel("intercept", use_re = TRUE)),
    inlier_pct = 1)
} else {
  mr_df$load_df(
    data = df, col_obs = "log_effect_size", col_obs_se = "se",
    col_covs = cvs, col_study_id = "nid")
  model <- MRBRT(
    data = mr_df,
    cov_models =list(LinearCovModel("intercept", use_re = TRUE),
                     LinearCovModel("ckd3", use_re = FALSE),
                     LinearCovModel("ckd4", use_re = FALSE)),
    inlier_pct = 1)
}
# fit model
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# save model object
py_save_object(object = model, filename = paste0(mrbrt_output_dir, "mod1.pkl"), pickle = "dill")

# Create draws with fisher mat ------------------------------------------------------------------------------------------------------------
model <- py_load_object(filename = model_path, pickle = "dill")
draw_fish <- get_draws(model)

# write draws for evidence score
dir.create( paste0("/ihme/scratch/users/qwr/ylds/ckd/escore/",ver, "/"), recursive = TRUE)
write.csv(draw_fish, file = paste0("/ihme/scratch/users/qwr/ylds/ckd/escore/",ver,"/rr_draws_", exp, "_", out, ".csv"), row.names = FALSE)




# # Evidence Score with Pub Bias ----------------------------------------
# ro_pair <- paste0(exp,"_",out)
# ro_pair
# 
# path <- paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/mrbrt_model_outputs/v",ver,"/")
# model_path <- paste0(path, exp, "_", out, "/mod1.pkl")
# model_path
# 
# model <- py_load_object(filename = model_path, pickle = "dill")
# 
# # extract data
# df <- extract_data_info(model)
# 
# # detect publication bias
# egger_model_all <- egger_regression(df$residual, df$residual_se)
# egger_model <- egger_regression(df[!df$outlier,]$residual, df[!df$outlier,]$residual_se)
# egger_model
# has_pub_bias <- egger_model$pval < 0.05
# print(has_pub_bias)
# 
# # adjust for publication bias
# if (has_pub_bias) {
#   df_fill <- get_df_fill(df)
#   num_fill <- nrow(df_fill)
# } else {
#   num_fill <- 0
# }
# 
# # fill the data if needed and refit the model
# if (num_fill > 0) {
#   df <- rbind(df, df_fill)
#   
#   # refit the model
#   data = MRData()
#   data$load_df(
#     data=df[!df$outlier,],
#     col_obs='obs',
#     col_obs_se='obs_se',
#     col_covs=as.list(model$cov_names),
#     col_study_id='study_id'
#   )
#   model_fill <- MRBRT(data, cov_models=model$cov_models)
#   model_fill$fit_model()
# } else {
#   model_fill <- NULL
# }
# 
# # extract scores
# uncertainty_info <- get_uncertainty_info(model)
# if (is.null(model_fill)) {
#   uncertainty_info_fill <- NULL
# } else {
#   uncertainty_info_fill <- get_uncertainty_info(model_fill)
# }
# 
# # Output Diagnostics ---------------------------------------------------------------------------------
# # pdf(file = paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/plots/v",ver,"/", ro_pair, ".pdf")) # The height of the plot in inches
# # plot_model(df, uncertainty_info, model, uncertainty_info_fill, model_fill, ro_pair)
# # dev.off()
# 
# # summary <- summarize_model(ro_pair, model, model_fill, egger_model, egger_model_all, uncertainty_info)
# # summary
# # write.csv(summary, file = paste0("/ihme/code/qwr/ckd_qwr/evidence_score_pipeline/summary/v",ver,"/", ro_pair, ".csv"), row.names = FALSE)

