#
# publication bias and fisher information boost
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

# directories
out_dir <- "FILEPATH"
work_dir <- "FILEPATH"
model_dir <- "FILEPATH"

### Running settings
if(interactive()){
  ro_pair <- "lung_cancer"
} else {
  ro_pair <- args[1]
}


setwd(work_dir)
source("./config.R")
source("./continuous_functions.R")
source("FILEPATH/get_ids.R")

linear_model_path <- paste0("FILEPATH/", ro_pair, ".pkl")
signal_model_path <- paste0("FILEPATH/", ro_pair, ".pkl")
ref_covs <- c("a_0", "a_1")
alt_covs <- c("b_0", "b_1")


### Load model objects
linear_model <- py_load_object(filename = linear_model_path, pickle = "dill")
signal_model <- py_load_object(filename = signal_model_path, pickle = "dill")

data_info <- extract_data_info(signal_model,
                               linear_model,
                               ref_covs = ref_covs,
                               alt_covs = alt_covs,
                               num_points = 1001)
data_info$ro_pair <- ro_pair
df <- data_info$df

### Detect publication bias
df_no_outlier <- df[!df$outlier,]
egger_model_all <- egger_regression(df$residual, df$residual_se)
egger_model <- egger_regression(df_no_outlier$residual, df_no_outlier$residual_se)
has_pub_bias_real <- egger_model$pval < 0.05

has_pub_bias <- F

### Adjust for publication bias
if (has_pub_bias) {
  df_fill <- get_df_fill(df[!df$outlier,])
  num_fill <- nrow(df_fill)
} else {
  num_fill <- 0
}

# fill the data if needed and refit the model
if (num_fill > 0) {
  df <- rbind(df, df_fill)
  data_info$df <- df
  
  # refit the model
  data = MRData()
  data$load_df(
    data=df[!df$outlier,],
    col_obs='obs',
    col_obs_se='obs_se',
    col_covs=as.list(linear_model$cov_names),
    col_study_id='study_id'
  )
  linear_model_fill <- MRBRT(data, cov_models=linear_model$cov_models)
  linear_model_fill$fit_model()
} else {
  linear_model_fill <- NULL
}

### Extract scores
uncertainty_info <- get_uncertainty_info(data_info, linear_model)
if (is.null(linear_model_fill)) {
  uncertainty_info_fill <- NULL
} else {
  uncertainty_info_fill <- get_uncertainty_info(data_info, linear_model_fill)
}


### Output diagnostics
# figures
title <- paste0(ro_pair, ": egger_mean=", round(egger_model$mean, 3),
                ", egger_sd=", round(egger_model$sd,3), ", egger_pval=", 
                round(egger_model$pval, 3))

pdf(paste0(out_dir, "plots/", ro_pair,"_plots.pdf"))
plot_residual(df, title)

plot_model(data_info,
           uncertainty_info,
           linear_model,
           signal_model,
           uncertainty_info_fill,
           linear_model_fill)
dev.off()

# summary
summary <- summarize_model(data_info,
                           uncertainty_info,
                           linear_model,
                           signal_model,
                           egger_model,
                           egger_model_all,
                           uncertainty_info_fill,
                           linear_model_fill)
write.csv(summary, paste0(out_dir, "summary/", ro_pair,"_summary.csv"), row.names = F)

# find the cause_id for the ro_pair
cause_ids <- get_ids("cause")
all_causes <- c(426, 509, 493, 494, 322, 297, 515, 587, 500:502, 543, 544, 546,
                411,414,487,417,423,429,432,441,444,447,450,456,474,471,671,672,
                534,630,527,627,438,878,923)
map <- fread("FILEPATH/map_causes.csv")
setnames(map, "cause_choice_formal", "cause_name")

cause_names <- cause_ids[cause_id %in% all_causes, .(cause_id, cause_name)]
cause_names <- merge(cause_names, map, by=c("cause_name"), all.x = T)
cause_names[cause_id==502, cause_choice := 'peripheral_artery_disease']

# get cause_id for the ro_pair
cause_id <- cause_names[cause_choice==ro_pair, cause_id]

# draws
draws <- get_draws(data_info, linear_model) %>% data.table
draws[, cause_id := cause_id]

setcolorder(draws, neworder = c("cause_id", "exposure", paste0("draw_",1:1000)))
write.csv(draws, paste0(out_dir, "draws/", ro_pair,"_draws.csv"), row.names = F)


