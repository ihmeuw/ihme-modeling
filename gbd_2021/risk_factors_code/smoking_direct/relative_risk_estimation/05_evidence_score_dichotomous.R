#
# create draws for binary outcome--fractures.
#
#
rm(list=ls())
library(mrbrt001, lib.loc = "FILEPATH")
library(data.table)
library(dplyr)
args <- commandArgs(trailingOnly = TRUE)

np <- import("numpy")
np$random$seed(as.integer(123))
set.seed(123)


### Running settings
# ro_pair <- args[1]
# out_dir <- args[2]
# WORK_DIR <- args[3]
ro_pair <- c("fractures")

# directories
out_dir <- "FILEPATH"
work_dir <- "FILEPATH"
model_dir <- "FILEPATH"

setwd(work_dir)
source("./config.R")
source(".FILEPATH/dichotomous_functions.R")

model_path <- paste0(model_dir, "fractures_cov_finder_no_sex_0.9.pkl")


### Load model objects
model <- py_load_object(filename = model_path, pickle = "dill")

### Extract data
df <- extract_data_info(model)

### Detect publication bias
egger_model_all <- egger_regression(df$residual, df$residual_se)
egger_model <- egger_regression(df[!df$outlier,]$residual, df[!df$outlier,]$residual_se)
has_pub_bias_real <- egger_model$pval < 0.05

# do not adjust for pb for this round
has_pub_bias <- F

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
    col_obs='obs',
    col_obs_se='obs_se',
    col_covs=as.list(model$cov_names),
    col_study_id='study_id'
  )
  model_fill <- MRBRT(data, cov_models=model$cov_models)
  model_fill$fit_model()
} else {
  model_fill <- NULL
}


### Extract scores
uncertainty_info <- get_uncertainty_info(model)
if (is.null(model_fill)) {
  uncertainty_info_fill <- NULL
} else {
  uncertainty_info_fill <- get_uncertainty_info(model_fill)
}


### Output diagnostics
title <- paste0(ro_pair, ": score=",round(uncertainty_info$score,3),", egger_mean=", round(egger_model$mean, 3),
                ", egger_sd=", round(egger_model$sd,3), ", egger_pval=", 
                round(egger_model$pval, 3))
pdf(paste0(out_dir, "plots/", ro_pair,"_plots.pdf"), width = 8)
plot_model(df, uncertainty_info, model, uncertainty_info_fill, model_fill, title =  title)
dev.off()

summary <- summarize_model(ro_pair, model, model_fill, egger_model, egger_model_all, uncertainty_info)
draws <- get_draws(model) %>% data.table
draws[, cause_id := 923]
setcolorder(draws, neworder = c("cause_id", "draw"))
mean(draws$draw) %>% exp

# save the summary and the draws
write.csv(summary, paste0(out_dir, "summary/", ro_pair,"_summary.csv"), row.names = F)
write.csv(draws, paste0(out_dir, "draws/", ro_pair,"_draws.csv"), row.names = F)


