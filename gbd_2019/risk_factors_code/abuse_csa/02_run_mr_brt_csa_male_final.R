# Author
# 3 June 2019
# GBD 2019
# Purpose: Run MR-BRT on prepped data to obtain CSA crosswalks

rm(list=ls())

library(data.table)

source("FILEPATH/mr_brt_functions.R")

dataset <- fread("FILEPATH/male_csa_prepped_mrbrt_aug1.csv")
dataset$is_outlier[is.na(dataset$is_outlier)] <- 0
dataset <- dataset[dataset$is_outlier!=1,]
dataset$d_contact_noncontact[dataset$d_intercourse==1] <- 0
dataset <- dataset[dataset$d_contact_noncontact!=-1,]
dataset <- dataset[dataset$d_intercourse!=-1,]
dataset <- dataset[dataset$d_perp3!=-1,]
dataset <- dataset[dataset$d_recall_over_age_15!=-1,]
dataset <- dataset[dataset$d_recall_under_age_15!=-1,]
dataset <- dataset[dataset$sex!="",]

# this source is marked for smaller_site, so not representative of US
dataset <- dataset[is.na(dataset$a_nid) | dataset$a_nid!=124091,]

model <- "int_contact_perp"
outdir <- paste0("FILEPATH/csa_Male_", model)


## Run stage 1 model ##


fit1 <- run_mr_brt(
  output_dir = "FILEPATH",   #Change as needed
  model_label = paste0("csa_Male_", model),    #Change as needed
  data = dataset,
  mean_var = "logit_diff",
  se_var = "logit_diff_se",
  covs = list(
    cov_info("d_intercourse", "X"),
    cov_info("d_contact_noncontact", "X"),
    cov_info("d_perp3", "X")
  ),
  method = "trim_maxL",
  trim_pct = 0.1,
  remove_x_intercept = TRUE,
  study_id = "id",
  overwrite_previous = TRUE)

# plot_mr_brt(fit1)
saveRDS(fit1, paste0(outdir, "/model_output.RDS"))

check_for_outputs(fit1)
results1 <- load_mr_brt_outputs(fit1)
df_pred1 <- expand.grid(d_intercourse = c(0, 1), 
                        d_contact_noncontact = c(0,1),
                        d_perp3 = c(0,1)
)  #Change as needed
pred1 <- as.data.table(predict_mr_brt(fit1, newdata = df_pred1)["model_summaries"], n_samples = 10000)
names(pred1) <- gsub("model_summaries.", "", names(pred1))
names(pred1) <- gsub("X_", "", names(pred1))

## Prepare stage 2 model ##

pred1[, `:=` (beta = Y_mean, se = (Y_mean_hi - Y_mean_lo) / (qnorm(0.975, 0, 1)))]
pred1[, `:=` (keep = d_intercourse + d_contact_noncontact + d_perp3)] #Change as needed
pred1[, `:=` (x_cov = ifelse(d_intercourse == 1, "d_intercourse",
                             ifelse(d_contact_noncontact == 1, "d_contact_noncontact", 
                                    ifelse(d_perp3 == 1, "d_perp3", 
                                                  "reference"))))]   #Change as needed

final_table <- pred1[keep==1, .(x_cov, beta = Y_mean, 
                                prev05 = exp((log(.05/(1-.05))-Y_mean))/(1+exp((log(.05/(1-.05))-Y_mean))), 
                                prev20 = exp((log(.2/(1-.2))-Y_mean))/(1+exp((log(.2/(1-.2))-Y_mean))), 
                                prev40 = exp((log(.4/(1-.4))-Y_mean))/(1+exp((log(.4/(1-.4))-Y_mean))), 
                                prev60 = exp((log(.6/(1-.6))-Y_mean))/(1+exp((log(.6/(1-.6))-Y_mean))), 
                                lower = Y_mean_lo, upper = Y_mean_hi, se = se)]

write.csv(final_table, paste0(outdir, "/final_betas_v", model, ".csv"), row.names=F)   #Change as needed

#plot_mr_brt(fit1)

