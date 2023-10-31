## set-up
library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH")
library(msm)
library(msm, lib.loc = "FILEPATH")

library(readxl)
library(readxl, lib.loc = "FILEPATH")

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

## prep data
df <- read.csv('FILEPATH')
df <- subset(df, df$ratio != 'Inf' & df$ratio_se != 'Inf')
df$num <- as.character(df$num)
df$den <- as.character(df$den)

# replace na in denominator to signal case definition
df$den <- ifelse(df$den == '', 'cv_case_def', df$den)

colnames(df)[colnames(df)=="num"] <- "alt"
colnames(df)[colnames(df)=="den"] <- "ref"

case_defs <- grep('cv_', colnames(df), value=T)
nonGBD_case_defs <- case_defs[!case_defs == 'cv_case_def']

for (i in nonGBD_case_defs) df[, i] <- 0
for (i in nonGBD_case_defs) df[, i] <- df[, i] - sapply(i, grepl, df$ref)
for (i in nonGBD_case_defs) df[, i] <- df[, i] + sapply(i, grepl, df$alt)

tmp_metareg <- df
ratio_var <- "ratio"
ratio_se_var <- "ratio_se"

# log-transform ratio and standard error 
tmp_metareg$ratio_log <- log(tmp_metareg$ratio)
tmp_metareg$ratio_se_log <- sapply(1:nrow(tmp_metareg), function(i) {
  ratio_i <- tmp_metareg[i, "ratio"]
  ratio_se_i <- tmp_metareg[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

cov_list <- lapply(nonGBD_case_defs, function(x) cov_info(x, "X"))

print('data are prepped. starting model.')

tmp_metareg <- sample_n(tmp_metareg, 50000)

# run MR-BRT model
fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "crosswalk_network_75k",
  data = tmp_metareg,
  mean_var = "ratio_log",
  se_var = "ratio_se_log",
  covs = cov_list,
  remove_x_intercept = TRUE,
  overwrite_previous = TRUE
)

saveRDS(
  fit1, 
  'FILEPATH'
)

results <- fit1$model_coefs[,c('x_cov', 'beta_soln')]
results$exponentiated <- exp(results$beta_soln)
results

