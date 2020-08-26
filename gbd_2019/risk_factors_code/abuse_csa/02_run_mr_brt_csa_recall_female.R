# AUTHOR
# 3 June 2019
# GBD 2019
# Purpose: Run MR-BRT on prepped data to obtain CSA crosswalks

rm(list=ls())

library(data.table)

source("FILEPATH/mr_brt_functions.R")

dataset <- fread("FILEPATH/csa_recallu15_csa_female_prepped_mrbrt_aug7.csv")

sex <- "female"
model <- "recall_u15"
outdir <- paste0("FILEPATH/csa_", sex, "_", model)


## Run stage 1 model ##


fit1 <- run_mr_brt(
  output_dir = "FILEPATH",   #Change as needed
  model_label = paste0("csa_", sex, "_", model),    #Change as needed
  data = dataset,
  mean_var = "logit_diff",
  se_var = "logit_diff_se",
  covs = list(
    cov_info("cv_recall_under_age15", "X"),
    cov_info("cv_recall_over_age15", "X")
  ),
  method = "trim_maxL",
  trim_pct = 0.1,
  remove_x_intercept = TRUE,
  study_id = "nid",
  overwrite_previous = TRUE)

# plot_mr_brt(fit1)
saveRDS(fit1, paste0(outdir, "/model_output.RDS"))

check_for_outputs(fit1)
results1 <- load_mr_brt_outputs(fit1)
df_pred1 <- expand.grid(cv_recall_under_age15 = c(0, 1), 
                        cv_recall_over_age15 = c(0,1)
)  #Change as needed
pred1 <- as.data.table(predict_mr_brt(fit1, newdata = df_pred1)["model_summaries"], n_samples = 10000)
names(pred1) <- gsub("model_summaries.", "", names(pred1))
names(pred1) <- gsub("X_", "", names(pred1))

## Prepare stage 2 model ##

pred1[, `:=` (beta = Y_mean, se = (Y_mean_hi - Y_mean_lo) / (qnorm(0.975, 0, 1)))]
pred1[, `:=` (keep = cv_recall_under_age15 + cv_recall_over_age15)] #Change as needed
pred1[, `:=` (x_cov = ifelse(cv_recall_under_age15 == 1, "cv_recall_under_age15",
                             ifelse(cv_recall_over_age15 == 1, "cv_recall_over_age15",
                                                  "reference")))]   #Change as needed

final_table <- pred1[keep==1, .(x_cov, beta = Y_mean, 
                                prev05 = exp((log(.05/(1-.05))-Y_mean))/(1+exp((log(.05/(1-.05))-Y_mean))), 
                                prev20 = exp((log(.2/(1-.2))-Y_mean))/(1+exp((log(.2/(1-.2))-Y_mean))), 
                                prev40 = exp((log(.4/(1-.4))-Y_mean))/(1+exp((log(.4/(1-.4))-Y_mean))), 
                                prev60 = exp((log(.6/(1-.6))-Y_mean))/(1+exp((log(.6/(1-.6))-Y_mean))), 
                                lower = Y_mean_lo, upper = Y_mean_hi, se = se)]

write.csv(final_table, paste0(outdir, "/final_betas_v", model, ".csv"), row.names=F)   #Change as needed

#plot_mr_brt(fit1)

