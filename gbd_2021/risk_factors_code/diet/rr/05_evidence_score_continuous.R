#
# 05_evidence_score_continuous.R
#
#
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")

# set seed
np <- import("numpy")
np$random$seed(as.integer(2738))
set.seed(2738)
## 

if(interactive()){
  
  version <- "VERSION"
  ro_pair <- "fruit_ihd"
  out_dir <- paste0("FILEPATH", version, "/")
  WORK_DIR <- "FILEPATH"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  
}
setwd(WORK_DIR)
source(paste0(out_dir, "/config.R"))
source("FILEPATH/helper_functions.R")
source("FILEPATH/continuous_functions.R")

is_j_shaped <- ro_pair %in% J_SHAPE_RISKS
monotonicity <- DIRECTION[strsplit(ro_pair, "_")[[1]][1]][[1]]


linear_model_path <- paste0(out_dir, "/04_mixed_effects_pkl_files/", ro_pair, ".pkl")
signal_model_path <- paste0(out_dir, "/01_template_pkl_files/", ro_pair, ".pkl")


ref_covs <- c("a_0", "a_1")
alt_covs <- c("b_0", "b_1")


### Load model objects
linear_model <- py_load_object(filename = linear_model_path, pickle = "dill")
signal_model <- py_load_object(filename = signal_model_path, pickle = "dill")


# Use config parameter to determine which exposure values to predict curve for
if(!USE_GLOBAL_DIST_PREDICT){
  exposure <- NULL
}else{
  diet_ro_pair_map <- fread("FILEPATH/diet_ro_map.csv")
  diet_ro_pair_map <- diet_ro_pair_map[include==1,]
  rei_id <- unique(diet_ro_pair_map[risk_cause==ro_pair, rei_id])
  exposure <- get_global_exposure_distribution(rei_id=rei_id, decomp_step="iterative", gbd_round_id=7)
  exposure <- exp_df$exposure
}

data_info <- extract_data_info(signal_model,
                               linear_model,
                               ref_covs = ref_covs,
                               alt_covs = alt_covs,
                               exposure_vec = exposure)

data_info$ro_pair <- ro_pair
df <- data_info$df

### Detect publication bias
df_no_outlier <- df[!df$outlier,]
egger_model_all <- egger_regression(df$residual, df$residual_se, one_sided = FALSE)
egger_model <- egger_regression(df_no_outlier$residual, df_no_outlier$residual_se, one_sided = FALSE)
has_pub_bias <- egger_model$pval < 0.05

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

pdf(paste0(out_dir, "/05_all_plots/", ro_pair, "_pub_bias.pdf"), width = 11, height = 8)


### Output diagnostics
# figures
title <- paste0(ro_pair, ": egger_mean=", round(egger_model$mean, 3),
                ", egger_sd=", round(egger_model$sd,3), ", egger_pval=", 
                round(egger_model$pval, 3))
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

write.csv(summary, paste0(out_dir, "/05_pub_bias/", ro_pair, "_summary.csv"), row.names = F)

if(F){
  version <- "VERSION"
  out_dir <- paste0("FILEPATH", version, "/")
  setwd(paste0(out_dir, "05_all_plots"))
  system(paste0("FILEPATH -dBATCH -dSAFER -DNOPAUSE -q -sDEVICE=pdfwrite -sOutputFile=complied_plots.pdf -f $(ls | grep pdf  |sort -n | xargs)"), intern = T)
  system(paste0("mv complied_plots.pdf ", out_dir,version, "complied_plots.pdf"))
  setwd(paste0("FILEPATH"))
}




