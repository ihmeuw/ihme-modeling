# SPLINE ON AGE

# Author
# 3 June 2019
# GBD 2019
# Purpose: Run MR-BRT on prepped WHO data to obtain IPV crosswalks for both lifetime and 12-month prevalence

rm(list=ls())

library(data.table)
source("FILEPATH/mr_brt_functions.R")

dataset <- fread("FILEPATH/WHO_IPV_cx_input_prepped_logit_spline_age.csv")

model <- "LT_agespline_noprior"
outdir <- paste0("FILEPATH/who_ipv_", model)


## Run stage 1 model ##


fit1 <- run_mr_brt(
  output_dir = "FILEPATH",   #Change as needed
  model_label = paste0("who_ipv_", model),    #Change as needed
  data = dataset,
  mean_var = "logit_diff",
  se_var = "logit_diff_se",
  covs = list(#Change as needed:
    cov_info("d_curr_age", "X", 
             degree = 3, i_knots = c("20, 25, 35"),
             r_linear = TRUE, l_linear = TRUE,
             bspline_gprior_mean = "0, 0, 0, 0", 
             bspline_gprior_var = "inf, inf, inf, inf")),
  study_id = "id",
  overwrite_previous = TRUE)

# plot_mr_brt(fit1)
saveRDS(fit1, paste0(outdir, "/model_output.RDS"))


check_for_outputs(fit1)
results1 <- load_mr_brt_outputs(fit1)
df_pred1 <- expand.grid(d_curr_age = c(12.5, 17.5, 22.5, 27.5, 32.5, 37.5, 42.5, 47.5, 
                                       52.5, 57.5, 62.5, 67.5, 72.5, 77.5, 82.5, 87.5, 92.5, 97.5))  #Change as needed
pred1 <- as.data.table(predict_mr_brt(fit1, newdata = df_pred1)["model_summaries"], n_samples = 10000)
names(pred1) <- gsub("model_summaries.", "", names(pred1))
names(pred1) <- gsub("X_", "", names(pred1))

## Prepare stage 2 model ##

pred1[, `:=` (beta = Y_mean, se = (Y_mean_hi - Y_mean_lo) / (qnorm(0.975, 0, 1)))]
pred1[, `:=` (x_cov = ifelse(d_curr_age == 12.5, "12mo_age_10_14",
                             ifelse(d_curr_age == 17.5, "12mo_age_15_19",
                                    ifelse(d_curr_age == 22.5, "12mo_age_20_24",
                                           ifelse(d_curr_age == 27.5, "12mo_age_25_29",
                                                  ifelse(d_curr_age == 32.5, "12mo_age_30_34",
                                                         ifelse(d_curr_age == 37.5, "12mo_age_35_39",
                                                                ifelse(d_curr_age == 42.5, "12mo_age_40_44",
                                                                       ifelse(d_curr_age == 47.5, "12mo_age_45_49",
                                                                              ifelse(d_curr_age == 52.5, "12mo_age_50_54",
                                                                                     ifelse(d_curr_age == 57.5, "12mo_age_55_59",
                                                                                            ifelse(d_curr_age == 62.5, "12mo_age_60_64",
                                                                                                   ifelse(d_curr_age == 67.5, "12mo_age_65_69",
                                                                                                          ifelse(d_curr_age == 72.5, "12mo_age_70_74",
                                                                                                                 ifelse(d_curr_age == 77.5, "12mo_age_75_79",
                                                                                                                        ifelse(d_curr_age == 82.5, "12mo_age_80_84",
                                                                                                                               ifelse(d_curr_age == 87.5, "12mo_age_85_89",
                                                                                                                                      ifelse(d_curr_age == 92.5, "12mo_age_90_94",
                                                                                                                                             ifelse(d_curr_age == 97.5, "12mo_age_95_99", "reference")))))))))))))))))))]   #Change as needed
final_table <- pred1[, .(d_curr_age, x_cov, beta = Y_mean, 
                         prev20 = exp((log(.2/(1-.2))-Y_mean))/(1+exp((log(.2/(1-.2))-Y_mean))), 
                         prev40 = exp((log(.4/(1-.4))-Y_mean))/(1+exp((log(.4/(1-.4))-Y_mean))), 
                         prev60 = exp((log(.6/(1-.6))-Y_mean))/(1+exp((log(.6/(1-.6))-Y_mean))), 
                         lower = Y_mean_lo, upper = Y_mean_hi, se = se)]

write.csv(final_table, paste0(outdir, "/final_betas_v", model, ".csv"), row.names=F)   #Change as needed

#plot_mr_brt(fit1)

